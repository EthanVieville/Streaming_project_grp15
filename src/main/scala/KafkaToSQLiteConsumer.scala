import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement}

object KafkaToSQLiteConsumer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaToSQLiteConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Configuration de Kafka
    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "your-kafka-topic"

    // Lecture du stream Kafka
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    import spark.implicits._

    // Récupération du message JSON sous forme de string
    val jsonDF = df.selectExpr("CAST(value AS STRING)").as[String]

    // Parsing JSON vers DataFrame avec schéma
    val schema = spark.read
      .option("header", true)
      .json(Seq("""{"ID":"1","Marriage_Type":"Love","Age_at_Marriage":"23","Gender":"Male","Education_Level":"Graduate","Caste_Match":"Different","Religion":"Hindu","Parental_Approval":"No","Urban_Rural":"Urban","Dowry_Exchanged":"No","Marital_Satisfaction":"Medium","Divorce_Status":"Yes","Children_Count":"5","Income_Level":"Middle","Years_Since_Marriage":"34","Spouse_Working":"No","Inter-Caste":"No","Inter-Religion":"No"}""").toDS)
      .schema

    val parsedDF = jsonDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", schema).as("data"))
      .select("data.*")

    // Writer personnalisé pour insérer dans SQLite
    val sqliteWriter = new ForeachWriter[Row] {
      var connection: Connection = _
      var statement: PreparedStatement = _

      def open(partitionId: Long, epochId: Long): Boolean = {
        connection = DriverManager.getConnection("jdbc:sqlite:marriages.db")
        val createTableSQL =
          """CREATE TABLE IF NOT EXISTS marriages (
            ID TEXT,
            Marriage_Type TEXT,
            Age_at_Marriage TEXT,
            Gender TEXT,
            Education_Level TEXT,
            Caste_Match TEXT,
            Religion TEXT,
            Parental_Approval TEXT,
            Urban_Rural TEXT,
            Dowry_Exchanged TEXT,
            Marital_Satisfaction TEXT,
            Divorce_Status TEXT,
            Children_Count TEXT,
            Income_Level TEXT,
            Years_Since_Marriage TEXT,
            Spouse_Working TEXT,
            `Inter-Caste` TEXT,
            `Inter-Religion` TEXT
          )"""
        connection.createStatement().execute(createTableSQL)
        val insertSQL =
          """INSERT INTO marriages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        statement = connection.prepareStatement(insertSQL)
        true
      }

      def process(row: Row): Unit = {
        for (i <- 1 to row.length) {
          statement.setString(i, if (row.isNullAt(i - 1)) null else row.getString(i - 1))
        }
        statement.executeUpdate()
      }

      def close(errorOrNull: Throwable): Unit = {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    }

    // Stream vers SQLite
    val query = parsedDF.writeStream
      .foreach(sqliteWriter)
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}