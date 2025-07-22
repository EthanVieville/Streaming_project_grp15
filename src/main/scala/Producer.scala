import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}

import scala.io.Source
import java.io.{File, FileInputStream, PrintWriter}
import java.util.Properties
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{floor, lit, row_number}
import scala.util.Using

object Producer {


  // LOAD PROPERTIES ====
  val props = new Properties()
  val stream = new FileInputStream("src/config/app.properties")
  props.load(stream)


  // stock l'avancement du traitement
  private val progressFile = "src/main/scala/OUTPUT/progress.txt"

  // Read file, si rien, on commence à 0, si lines, on reprend à partir de la dernière.
  private def readProgress(): Int = {
    val f = new File(progressFile) // "load" path file
    if (!f.exists) 0 // si !file, alors chunks = 0
    else {
      Using.resource(Source.fromFile(f)) { source =>
        val lines = source.getLines().toList
        if (lines.nonEmpty) lines.head.toInt else 0 // si une line dans le file, on récup la dernière (head), on return le int.
      }
    }
  }

  // écrit chunk id dans le txt
  private def writeProgress(nextChunkId: Int): Unit = {
    val pw = new PrintWriter(new File(progressFile))
    pw.println(nextChunkId)
    pw.close()
  }

  def main(args: Array[String]): Unit = {

    // == SPARK CONFIG ==
    val spark = SparkSession.builder()
      .appName("MrSplitMyFileIntoCsv") // name
      .master("local[*]") // exec local
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN") // OFF / ERROR / INFO (parle trop) ...
    // Empeche la creation des fichier checkpoint CRC (checksum HDFS dont on se passe volontier).
    spark.sparkContext.hadoopConfiguration.set(
      "fs.file.impl",
      classOf[RawLocalFileSystem].getName
    )


    // Charge CSV + mise en cache
    val rawDF = spark.read
      .option("header", "true")
      .csv(props.getProperty("input.path"))
      .cache()


    // == CHUNKS SPLIT ==

    // WindowFunc (tri window), trie les lignes par ID
    val w = Window.orderBy(col("ID").cast("int")) // Conversion int car col origin en string

    val withChunksDF = rawDF
      .withColumn("line_number", row_number().over(w)) // row_number() ajoute un num, basé sur le trie par ID.
      .withColumn("chunkId", floor(($"line_number" - 1) / props.getProperty("chunk.size").toInt).cast("int")) // divise par le chunks.size. Tant que line_number / chunks.size pas égal à un int. On prend l'int du dessous.
      .cache()



    // == Gestion du nombre total de chunk ==

    // Récupère le max(chunkId)
    val numChunks = withChunksDF
      .agg(max("chunkId").alias("maxChunkId")) // get max chunkId
      .selectExpr("maxChunkId + 1 as numChunks") // +1 (from 0)
      .first() // agg = une seule ligne
      .getAs[Int]("numChunks") // toInt()~


    // Reprend depuis le dernier chunk dans le progress
    val startChunk = readProgress()
    if (startChunk >= numChunks) { // Si chunk dans progress.txt >= maxChunks, déjà done.
      println(s"[INFO] $numChunks chunk ont déjà était fait.")
      spark.stop()
      return
    }

    // =========== KAFKA ===========

    // FileSystem pour manipuler nos files
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Boucle par chunk (from startChunk (progessFile) to numChunks
    for (chunkId <- startChunk until numChunks) {
      val t0 = System.nanoTime() // chrono par chunk

      // DROP useless col
      val chunkDF = withChunksDF
        .filter($"chunkId" === chunkId)
        .drop("line_number", "chunkId")


      // Write chunk into CSV
      val tmpPath = new Path(s"${props.getProperty("output.dir")}/temp/chunk_$chunkId")
      chunkDF.coalesce(1) // 1 CSV file par chunk
        .write.mode("overwrite") // précaution
        .option("header", "true")
        .csv(tmpPath.toString)

      // Spark écrit en part- (auto), on les retires.
      // get path
      val part_path = fs.listStatus(tmpPath)
        .find(_.getPath.getName.startsWith("part-"))
        .get
        .getPath

      // real output file.csv
      val finalPath = new Path(s"${props.getProperty("output.dir")}/chunk_$chunkId.csv")
      fs.rename(part_path, finalPath) // transit
      fs.delete(tmpPath, true) // drop les temp


      // == Prepare to KAFKA ==

      // KAFKA attend key : value
      val kafkaDF = chunkDF
        .selectExpr("CAST(null AS STRING) AS key", "to_json(struct(*)) AS value") // Pas de "key" ici. Aucun partionning spécial. struct(*) toutes les col de la ligne

      // WRITE
      kafkaDF
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", props.getProperty("kafka.brokers"))
        .option("topic", props.getProperty("kafka.topic"))
        .save()


      // Mise a jour progress après la gestion du chunk (En cas de plantage il peut bien reprendre au dernier chunk utilisé)
      writeProgress(chunkId + 1)

      val durationMs = (System.nanoTime() - t0) / 1e6
      println(f"[INFO] Chunk #$chunkId send. ($durationMs%.2f ms)")

      Thread.sleep(props.getProperty("pause.ms").toInt)
    }

    println(s"[INFO] $numChunks send.")
    spark.stop()
  }
}