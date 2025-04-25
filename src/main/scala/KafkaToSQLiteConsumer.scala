import org.apache.spark.sql.{SparkSession, Row}
import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object KafkaToSQLiteConsumer {

  val kafkaBootstrapServers = "localhost:9092"
  val kafkaTopic = "your-kafka-topic"
  val sqliteDbPath = "jdbc:sqlite:src/main/scala/marriages.db"
  val sqliteTable = "marriages"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaToSQLiteConsumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Kafka envoie les messages sous forme de byte[], donc on convertit
    val messages = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    // Chaque chunk est un bloc CSV. On le transforme en DataFrame avec header.
    val parsedChunks = messages.flatMap { chunk =>
      val lines = chunk.split("\n").toList
      if (lines.isEmpty) None
      else {
        val header = lines.head.split(",").map(_.trim)
        val rows = lines.tail.map(_.split(",").map(_.trim))
        Some((header, rows))
      }
    }

    val query = parsedChunks.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[(Array[String], List[Array[String]])], batchId: Long) =>
        batchDF.collect().foreach { case (header, rows) =>
          val conn = DriverManager.getConnection(sqliteDbPath)
          val stmt = conn.createStatement()

          val columnsSql = header.map(col => s""""$col" TEXT""").mkString(", ")
          stmt.execute(s"""CREATE TABLE IF NOT EXISTS $sqliteTable ($columnsSql)""")

          val placeholders = header.map(_ => "?").mkString(", ")
          val insertSql = s"""INSERT INTO $sqliteTable VALUES ($placeholders)"""
          val pstmt = conn.prepareStatement(insertSql)

          rows.foreach { values =>
            values.zipWithIndex.foreach { case (value, i) =>
              pstmt.setString(i + 1, value)
            }
            pstmt.executeUpdate()
          }

          pstmt.close()
          stmt.close()
          conn.close()
        }
      }
      .start()


    query.awaitTermination()
  }
}
