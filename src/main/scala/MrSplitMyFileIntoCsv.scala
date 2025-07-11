import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}

import scala.io.Source
import java.io.{File, FileInputStream, PrintWriter}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, floor, lit, row_number}


object MrSplitMyFileIntoCsv {
  // Peut etre utiliser un windows function avec gestion ru rax numb eravec gestin du timestamp
  // Substract sur le df initial du nouveau df bien variabiliser le nb de batch


  // LOAD PROPERTIES ====
  val props = new Properties()
  val stream = new FileInputStream("src/config/app.properties")
  props.load(stream)


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

    // Charge CSV + mise en cache
    val rawDF = spark.read
      .option("header", "true")
      .csv(props.getProperty("input.path"))
      .cache()


    val w = Window.orderBy(lit(1))

    val withChunksDF = rawDF
      .withColumn("line_number", row_number().over(w)) // new col line_number avec l'application de la WF
      .withColumn("chunkId", floor(($"line_number" - 1) / props.getProperty("chunk.size").toInt).cast("int")) // taille du chunk + ID
      .cache()



    // Gestion du nombre total de chunk

    /*
    val maxChunkId = withChunksDF.agg(max("chunkId")).as[Int].head
    val numChunks = maxChunkId + 1

     */

    val chunkStats = withChunksDF.groupBy().agg(max("chunkId").alias("maxChunkId"))
    val numChunks = chunkStats.selectExpr("maxChunkId + 1 as numChunks")
      .first()
      .getAs[Int]("numChunks")



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


    // Boucle par chunk
    for (chunkId <- startChunk until numChunks) {
      val t0 = System.nanoTime()

      val chunkDF = withChunksDF
        .filter($"chunkId" === chunkId)
        .drop("line_number", "chunkId")


      // Ecris le chunk dans le nouveau CSV
      val tmpPath = new Path(s"${props.getProperty("output.dir")}/temp/chunk_$chunkId")
      chunkDF.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(tmpPath.toString)

      // Gestion des .part pour les retirer
      val part = fs.listStatus(tmpPath)
        .find(_.getPath.getName.startsWith("part-"))
        .get
        .getPath

      val finalPath = new Path(s"${props.getProperty("output.dir")}/chunk_$chunkId.csv")
      fs.rename(part, finalPath)
      fs.delete(tmpPath, true)


      // Prépare les données pour Kafka
      val kafkaDF = chunkDF
        .selectExpr("CAST(null AS STRING) AS key", "to_json(struct(*)) AS value")

      // Écrit dans Kafka
      kafkaDF
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", props.getProperty("kafka.brokers"))
        .option("topic", props.getProperty("kafka.topic"))
        .save()



      // Mise a jour progress après la gestion du chunk (En cas de plantage il peut bien reprendre au dernier chunk utilisé)
      writeProgress(chunkId + 1)

      val durationMs = (System.nanoTime() - t0) / 1e6
      println(f"[Chunk #$chunkId] traité en $durationMs%.2f ms")

      Thread.sleep(props.getProperty("pause.ms").toInt)
    }

    println(s"Les $numChunks ont étaitent traités et envoyés vers Kafka.")
    spark.stop()
  }
}