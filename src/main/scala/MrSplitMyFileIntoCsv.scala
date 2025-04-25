import org.apache.spark.sql.SparkSession
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.io.Source
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MrSplitMyFileIntoCsv {

  private val baseDir = System.getProperty("user.dir")
  private val scriptDir = baseDir + "/src/main/scala/"

  private val inputDir = scriptDir + "INPUT/"
  private val outputDir = scriptDir + "OUTPUT/"

  private val sourceFileName = "marriage_data_india.csv"
  private val progressFilePath = scriptDir + "progress.txt"

  private val numOfLines = 100
  private val kafkaTopic = "your-kafka-topic"
  private val kafkaBootstrapServers = "localhost:9092"

  private def readProgress(): (Int, Int) = {
    if (!Files.exists(Paths.get(progressFilePath))) return (0, 0)
    val lines = Source.fromFile(progressFilePath).getLines().toList
    val currentIndex = if (lines.nonEmpty) lines.head.toInt else 0
    val chunkIndex = if (lines.length > 1) lines(1).toInt else 0
    (currentIndex, chunkIndex)
  }

  private def writeProgress(lineCount: Int, chunkIndex: Int): Unit = {
    val writer = new PrintWriter(new File(progressFilePath))
    writer.println(lineCount)
    writer.println(chunkIndex)
    writer.close()
  }

  private def getOutputFileName(index: Int): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
    val timestamp = LocalDateTime.now().format(formatter)
    s"$outputDir/chunk_${index}_$timestamp.csv"
  }

  private def sendToKafka(topic: String, message: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, null, message)
    producer.send(record)
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("spark.hadoop.io.nativeio.enabled", "false")
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkSession.builder()
      .appName("FileSplitter")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.hadoop.io.native.lib.available", "false")

    val sourcePath = inputDir + sourceFileName

    if (!Files.exists(Paths.get(sourcePath))) {
      println(s"Fichier source $sourcePath non trouvé.")
      return
    }

    val df = spark.read
      .option("header", "true")
      .csv(sourcePath)

    val (savedLineIndex, savedChunkCount) = readProgress()
    var currentIndex = savedLineIndex
    var chunkCount = savedChunkCount

    println(s"Commence à partir de la ligne $currentIndex, numéro de chunk #$chunkCount")

    while (currentIndex < df.count()) {
      val startTime = System.nanoTime()

      val linesToWrite = df.limit(currentIndex + numOfLines).except(df.limit(currentIndex))
      val outputFileName = getOutputFileName(chunkCount)

      val tempDirPath = outputDir + s"temp_chunk_$chunkCount"
      linesToWrite.coalesce(1).write
        .mode("overwrite")
        .option("header", "true")
        .csv(tempDirPath)

      val tempDir = new File(tempDirPath)
      val partFile = tempDir.listFiles().find(_.getName.startsWith("part-")).get

      val finalFile = new File(outputDir + s"chunk_${chunkCount}.csv")
      Files.move(partFile.toPath, finalFile.toPath, StandardCopyOption.REPLACE_EXISTING)

      // Lire le contenu du fichier pour l’envoyer dans Kafka
      val chunkContent = Source.fromFile(finalFile).getLines().mkString("\n")
      sendToKafka(kafkaTopic, chunkContent)

      tempDir.listFiles().foreach(_.delete())
      tempDir.delete()

      val endTime = System.nanoTime()
      val durationMs = (endTime - startTime) / 1000000.0

      currentIndex += numOfLines
      chunkCount += 1
      writeProgress(currentIndex, chunkCount)

      println(s"Ecriture de $outputFileName contenant ${linesToWrite.count()} lignes.")
      println(f"Découpage en $durationMs%.3f ms. Envoi au topic Kafka. Attente avant le prochain fichier...")

      Thread.sleep(200)
    }

    println("Tous les fichiers ont été traités.")
    spark.stop()
  }
}
