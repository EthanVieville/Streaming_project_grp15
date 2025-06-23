import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}
import scala.io.Source
import java.io.{File, PrintWriter}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, floor, col, lit}


object MrSplitMyFileIntoCsv {
  // Peut etre utiliser un windows function avec gestion ru rax numb eravec gestin du timestamp
  // Substract sur le df initial du nouveau df bien variabiliser le nb de batch

  // stock l'avancement du traitement
  private val progressFile = "src/main/scala/OUTPUT/progress.txt"

  // Lis le fichier, si rien, on commence à 0, si des lignes sont déjà renseigner, on reprend à partir de la dernière.
  private def readProgress(): Int = {
    val f = new File(progressFile)
    if (!f.exists) 0
    else {
      val lines = Source.fromFile(f).getLines().toList
      if (lines.nonEmpty) lines.head.toInt else 0
    }
  }

  // écrit chunk id dans le txt
  private def writeProgress(nextChunkId: Int): Unit = {
    val pw = new PrintWriter(new File(progressFile))
    pw.println(nextChunkId)
    pw.close()
  }

  def main(args: Array[String]): Unit = {
    // Config de SPARK
    val spark = SparkSession.builder()
      .appName("MrSplitMyFileIntoCsv") // name
      .master("local[*]") // exec local
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    // Empeche la creation des fichier checkpoint CRC (checksum HDFS dont on se passe volontier).
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

    // Charge CSV + mise en cache
    val rawDF = spark.read
      .option("header", "true")
      .csv(inputPath)
      .cache()

    // println(rawDF.count()) // Astuce anti lazy ajouter un print

    /* PAS BIEN , on fait DF => RDD -> DF = redondant et ca coute des rsrc. (donc on as 2 DF qui traine au final)

    // idx à chaque ligne. pour identifier le chunk
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
    */

    // CORRECTION : WF avec lit, ca attribue un num de ligne unique à chaque ligne.
    // pas de RDD, un seul DF, ca tue.
    // faut bougé 2-3 trucs ensuite mais trkl

    val w = Window.orderBy(lit(1))

    val withChunksDF = rawDF
      .withColumn("line_number", row_number().over(w)) // new col line_number avec l'application de la WF
      .withColumn("chunkId", floor(($"line_number" - 1) / chunkSize).cast("int")) // taille du chunk + ID
      .cache()



    // Gestion du nombre total de chunk
    //val totalRows = withChunksDF.agg(max("chunkId")).as[Long].head + 1
    //val numChunks = Math.ceil(totalRows.toDouble / chunkSize).toInt

    val maxChunkId = withChunksDF.agg(max("chunkId")).as[Int].head
    val numChunks = maxChunkId + 1


    // Reprend depuis le dernier chunk dans le progress
    var startChunk = readProgress()
    if (startChunk >= numChunks) {
      println(s"Les $numChunks chunk ont déjà était fait.")
      spark.stop()
      return
    }

    // =========== KAFKA ===========

    // config kafka
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)


    /*
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", kafkaBrokers)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


     */


    // Boucle par chunk
    for (chunkId <- startChunk until numChunks) {
      val t0 = System.nanoTime()

      // get chunk. Drop colonne inutile.
      //val chunkDF = withChunksDF
      //  .filter($"chunkId" === chunkId)
      //  .drop("idx", "chunkId")

      val chunkDF = withChunksDF
        .filter($"chunkId" === chunkId)
        .drop("line_number", "chunkId")


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

      /*
      // Envoie des chunk a Kafka sous JSON form
      chunkDF.toJSON
        .rdd
        .foreachPartition { iter: Iterator[String] =>
          val producer = new KafkaProducer[String, String](kafkaProps)
          iter.foreach { json =>
            producer.send(new ProducerRecord[String, String](kafkaTopic, json))
          }
          producer.close()
        }

      */

      // Prépare les données pour Kafka
      val kafkaDF = chunkDF
        .selectExpr("CAST(null AS STRING) AS key", "to_json(struct(*)) AS value")

      // Écrit dans Kafka
      kafkaDF
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBrokers)
        .option("topic", kafkaTopic)
        .save()





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