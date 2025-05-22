import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}
import scala.io.Source
import java.io.{File, PrintWriter}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MrSplitMyFileIntoCsv {
  // Peut etre utiliser un windows function avec gestion ru rax numb eravec gestin du timestamp
  // Substract sur le df initial du nouveau df bien variabiliser le nb de batch

  // Gestion du fichier de progress a refaire en spark
  private val progressFile = "src/main/scala/OUTPUT/progress.txt"

  private def readProgress(): Int = {
    val f = new File(progressFile)
    if (!f.exists) 0
    else {
      val lines = Source.fromFile(f).getLines().toList
      if (lines.nonEmpty) lines.head.toInt else 0
    }
  }

  private def writeProgress(nextChunkId: Int): Unit = {
    val pw = new PrintWriter(new File(progressFile))
    pw.println(nextChunkId)
    pw.close()
  }

  def main(args: Array[String]): Unit = {
    // Config de SPARK
    val spark = SparkSession.builder()
      .appName("MrSplitMyFileIntoCsv")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    // Empecher la creation des fichier checkpoint CRC
    spark.sparkContext.hadoopConfiguration.set(
      "fs.file.impl",
      classOf[RawLocalFileSystem].getName
    )
    import spark.implicits._

    // Param doivent bouger vers un fichier
    val inputPath     = "src/main/scala/INPUT/marriage_data_india.csv"
    val outputDir     = "src/main/scala/OUTPUT/"
    val chunkSize     = 100        // Lignes par chunk
    val pauseMs       = 2000       // Temps entre les chunk (SUpp 300ms temps de gestion chunk pour le moment)
    val kafkaBrokers  = "localhost:9092"
    val kafkaTopic    = "your-kafka-topic"

    // Mise en cache du CSV
    val rawDF = spark.read
      .option("header", "true")
      .csv(inputPath)
      .cache()

    // println(rawDF.count()) // Astuce anti lazy ajouter un print

    // Index incremental pour gére rle num de chunk sans rename après
    val rddWithIdx = rawDF.rdd.zipWithIndex().map {
      case (row, idx) => Row.fromSeq(row.toSeq :+ idx)
    }
    val schemaWithIdx = StructType(rawDF.schema.fields :+ StructField("idx", LongType, nullable = false))
    val indexedDF = spark.createDataFrame(rddWithIdx, schemaWithIdx)

    // Gestion des deux
    val withChunksDF = indexedDF
      .withColumn("chunkId", floor(col("idx") / chunkSize).cast("int"))
      .cache()
    // println(withChunksDF.count()) // Astuce anti lazy

    // Gestion du nombres total de chunk
    val totalRows = withChunksDF.agg(max("idx")).as[Long].head + 1
    val numChunks = Math.ceil(totalRows.toDouble / chunkSize).toInt

    // Reprend sdepuis le dernier chunk dans le progress
    var startChunk = readProgress()
    if (startChunk >= numChunks) {
      println(s"Les $numChunks chunk ont déjà était fait.")
      spark.stop()
      return
    }

    // Partie KAFKA voir avec Charles
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", kafkaBrokers)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Boucle par chunk
    for (chunkId <- startChunk until numChunks) {
      val t0 = System.nanoTime()

      // Récupère en cach pour le chunk en cours
      val chunkDF = withChunksDF
        .filter($"chunkId" === chunkId)
        .drop("idx", "chunkId")

      // Ecris le chunk dans le nouveau CSV
      val tmpPath = new Path(s"$outputDir/temp/chunk_$chunkId")
      chunkDF.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(tmpPath.toString)

      // Gestion des .part pour les retirer
      val part = fs.listStatus(tmpPath)
        .find(_.getPath.getName.startsWith("part-"))
        .get
        .getPath
      val finalPath = new Path(s"$outputDir/chunk_$chunkId.csv")
      fs.rename(part, finalPath)
      fs.delete(tmpPath, true)

      // Envoie des chunk a Kafka
      chunkDF.toJSON
        .rdd
        .foreachPartition { iter: Iterator[String] =>
          val producer = new KafkaProducer[String, String](kafkaProps)
          iter.foreach { json =>
            producer.send(new ProducerRecord[String, String](kafkaTopic, json))
          }
          producer.close()
        }

      // Mise a jour progress après la gestion du chunk (En cas de plantage il peut bien reprendre au dernier chunk utilisé)
      writeProgress(chunkId + 1)

      val durationMs = (System.nanoTime() - t0) / 1e6
      println(f"[Chunk #$chunkId] traité en $durationMs%.2f ms")

      Thread.sleep(pauseMs)
    }

    println(s"Les $numChunks ont étaitent traités et envoyés vers Kafka.")
    spark.stop()
  }
}