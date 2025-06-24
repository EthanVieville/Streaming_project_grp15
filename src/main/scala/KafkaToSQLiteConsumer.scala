import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

object KafkaToSQLiteConsumer {

  def main(args: Array[String]): Unit = {

    // LOAD PROPERTIES ====
    val props = new Properties()
    val stream = new FileInputStream("src/config/app.properties")
    props.load(stream)

    val spark = SparkSession.builder()
      .appName("KafkaToSQLiteConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val initConn = DriverManager.getConnection(s"jdbc:sqlite:${props.getProperty("sqlite.path")}")
    val createTableSQL =
      """CREATE TABLE IF NOT EXISTS marriages (
        ID TEXT PRIMARY KEY,
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
    initConn.createStatement().execute(createTableSQL)
    initConn.close()

    // Définition explicite du schéma
    val schema = StructType(Seq(
      StructField("ID", StringType),
      StructField("Marriage_Type", StringType),
      StructField("Age_at_Marriage", StringType),
      StructField("Gender", StringType),
      StructField("Education_Level", StringType),
      StructField("Caste_Match", StringType),
      StructField("Religion", StringType),
      StructField("Parental_Approval", StringType),
      StructField("Urban_Rural", StringType),
      StructField("Dowry_Exchanged", StringType),
      StructField("Marital_Satisfaction", StringType),
      StructField("Divorce_Status", StringType),
      StructField("Children_Count", StringType),
      StructField("Income_Level", StringType),
      StructField("Years_Since_Marriage", StringType),
      StructField("Spouse_Working", StringType),
      StructField("Inter-Caste", StringType),
      StructField("Inter-Religion", StringType)
    ))

    // Lecture du stream Kafka
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", props.getProperty("kafka.brokers"))
      .option("subscribe", props.getProperty("kafka.topic"))
      .option("startingOffsets", "earliest")
      .load()

    import spark.implicits._

    val orderedDF = df
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", schema).as("data"))
      .select(
        $"data.ID",
        $"data.Marriage_Type",
        $"data.Age_at_Marriage",
        $"data.Gender",
        $"data.Education_Level",
        $"data.Caste_Match",
        $"data.Religion",
        $"data.Parental_Approval",
        $"data.Urban_Rural",
        $"data.Dowry_Exchanged",
        $"data.Marital_Satisfaction",
        $"data.Divorce_Status",
        $"data.Children_Count",
        $"data.Income_Level",
        $"data.Years_Since_Marriage",
        $"data.Spouse_Working",
        $"data.`Inter-Caste`",
        $"data.`Inter-Religion`"
      )

    // Writer personnalisé pour insérer dans SQLite
    val sqliteWriter = new ForeachWriter[Row] {
      var connection: Connection = _
      var statement: PreparedStatement = _

      def open(partitionId: Long, epochId: Long): Boolean = {connection = DriverManager.getConnection(s"jdbc:sqlite:${props.getProperty("sqlite.path")}")
        val insertSQL = "INSERT OR IGNORE INTO marriages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
    val query = orderedDF.writeStream
      .foreach(sqliteWriter)
      .outputMode("append")
      .start()


    query.awaitTermination()
  }
}