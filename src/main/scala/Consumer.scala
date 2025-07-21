import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import StatisticsMaker._
import load_schema._
import schemas.schemas._
import org.apache.spark.sql.streaming.StreamingQuery

object Consumer {


  def main(args: Array[String]): Unit = {

    // === LOAD PROPERTIES ====
    val props = new Properties()
    val stream = new FileInputStream("src/config/app.properties")
    props.load(stream)

    // === POSTGRE ACCESS ===
    val pgUrl = props.getProperty("postgres.url")
    val pgUser = props.getProperty("postgres.user")
    val pgPwd = props.getProperty("postgres.password")

    // === Init Spark ===
    val spark = SparkSession.builder()
      .appName("KafkaToPostgreConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._


    // === FUNC pour write DF to postegre
    def writeStatStream(df: DataFrame, checkpointPath: String, tableName: String): StreamingQuery = {
      df.writeStream
        .outputMode("complete") // append (new lines only) / update (lines changed) / complete (re-write all)
        // Techniquement, on drop les tables, on pourrait update. Mais par sécu.
        .option("checkpointLocation", checkpointPath)
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          writeStatToPostgres(batchDF, tableName, pgUrl, pgUser, pgPwd, overwrite = true)
        }
        .start()
    }


    // DROP BEFORE INSERT
    val drop = props.getProperty("drop.tables", "false").toBoolean
    if (drop) dropExistingTables(pgUrl, pgUser, pgPwd)

    // INIT SCHEMA (TABLES) SQL
    executeSqlFile("src/config/schema.sql", pgUrl, pgUser, pgPwd)

    // === STREAM KAFKA
    val parsedDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", props.getProperty("kafka.brokers"))
      .option("subscribe", props.getProperty("kafka.topic"))
      .option("startingOffsets", "latest") // earliest / latest. Si on earliest, on as tout ce qui a été envoyé à kafka depuis le DEBUT.
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", marriageSchema).as("data")) // on utilise bien le schéma importé
      .select("data.*")


    // === WRITER POSTGRES personnalisé pour insérer dans POSTGRE
    val postgresWriter: ForeachWriter[Row] = new ForeachWriter[Row] {
      var connection: Connection = _
      var statement: PreparedStatement = _

      def open(partitionId: Long, epochId: Long): Boolean = {
        connection = DriverManager.getConnection(pgUrl, pgUser, pgPwd)
        val insertSQL =
          """INSERT INTO marriages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
             ON CONFLICT (ID) DO NOTHING""" // Si conflit, on ignore (pour stabilité), ici ca agit + pour doublon, si un doublon est capté, on l'ignore (donc insère pas)
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

    // == INSERT MARRIAGES (full data)
    val InsertFullDataSet = parsedDF.writeStream
      .foreach(postgresWriter)
      .outputMode("append") // pour le full dataset, on append  juste les lignes donc aucun soucis.
      .start()


    // ======= STATISTICS ========

    // Ici on checkpoint + overwrite. On fait les groupby sur les chunks du full dataset. On as donc besoin d'une "save" du groupBy, pour les cumulés (d'ou le checkpoint)
    // Et on overwrite pour cumulé d'un coup.

    val queries = List(
      InsertFullDataSet,
      writeStatStream(satisfactionByMarriageType(parsedDF), "checkpoints/statistics/satisfactionMariage", "satisfaction_by_marriage_type"),
      writeStatStream(satisfactionByAgeRange(parsedDF), "checkpoints/statistics/satisfactionAgeRange", "satisfaction_by_age_range"),
      writeStatStream(satisfactionByEducationLevel(parsedDF), "checkpoints/statistics/satisfactionEducationLevel", "satisfaction_by_education_level"),
      writeStatStream(satisfactionByReligion(parsedDF), "checkpoints/statistics/satisfactionReligionArranged", "satisfaction_by_religion_arranged"),
      writeStatStream(satisfactionByIncomeLevel(parsedDF), "checkpoints/statistics/satisfactionIncomeLevel", "satisfaction_by_income_level"),
      writeStatStream(divorcesByReligion(parsedDF), "checkpoints/statistics/divorcesByReligion", "divorces_by_religion"),
      writeStatStream(divorcesByEducation(parsedDF), "checkpoints/statistics/divorcesByEducation", "divorces_by_education"),
      writeStatStream(divorcesBySpouseWorking(parsedDF), "checkpoints/statistics/divorcesBySpouse", "divorces_by_spouse_working"),
    )

    queries.foreach(_.awaitTermination())
  }
}