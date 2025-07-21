import StatisticsMaker._
import load_schema._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}
import schemas.schemas._

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

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


    //// === FUNC pour write DF to postegre
    //def writeStatStream(df: DataFrame, checkpointPath: String, tableName: String): StreamingQuery = {
    //  df.writeStream
    //    .outputMode("complete") // append (new lines only) / update (lines changed) / complete (re-write all)
    //    // Techniquement, on drop les tables, on pourrait update. Mais par sécu.
    //    .option("checkpointLocation", checkpointPath)
    //    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    //      val cleanedDF = batchDF.cache()
    //      writeStatToPostgres(batchDF, tableName, pgUrl, pgUser, pgPwd, overwrite = true)
    //      writeStatToPostgres(satisfactionByMarriageType(cleanedDF), "satisfaction_by_marriage_type", pgUrl, pgUser, pgPwd)
    //      writeStatToPostgres(satisfactionByAgeRange(cleanedDF), "satisfaction_by_age_range", pgUrl, pgUser, pgPwd)
    //      writeStatToPostgres(satisfactionByEducationLevel(cleanedDF), "satisfaction_by_education_level", pgUrl, pgUser, pgPwd)
    //      writeStatToPostgres(satisfactionByReligion(cleanedDF), "satisfaction_by_religion_arranged", pgUrl, pgUser, pgPwd)
    //      writeStatToPostgres(satisfactionByIncomeLevel(cleanedDF), "satisfaction_by_income_level", pgUrl, pgUser, pgPwd)
    //      writeStatToPostgres(divorcesByReligion(cleanedDF), "divorces_by_religion", pgUrl, pgUser, pgPwd)
    //      writeStatToPostgres(divorcesByEducation(cleanedDF), "divorces_by_education", pgUrl, pgUser, pgPwd)
    //      writeStatToPostgres(divorcesBySpouseWorking(cleanedDF), "divorces_by_spouse_working", pgUrl, pgUser, pgPwd)
    //
    //      cleanedDF.unpersist() // Nettoie la mémoire
    //    }
    //    .start()
    //}


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
        // -1 correction entre JDBC et SPARK car index différent et insère null en base si la valeur de col est nul
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
    val insertFullDataset = parsedDF.writeStream
      .foreach(postgresWriter)
      .outputMode("append")
      .option("checkpointLocation", "checkpoints/full/marriages")
      .start()

    // === UN SEUL STREAM pour toutes les stats
    val statsStream = parsedDF.writeStream
      .outputMode("append")
      .option("checkpointLocation", "checkpoints/statistics/global")
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
        val cleanedDF = batchDF.cache()

        writeStatToPostgres(satisfactionByMarriageType(cleanedDF), "satisfaction_by_marriage_type", pgUrl, pgUser, pgPwd)
        writeStatToPostgres(satisfactionByAgeRange(cleanedDF), "satisfaction_by_age_range", pgUrl, pgUser, pgPwd)
        writeStatToPostgres(satisfactionByEducationLevel(cleanedDF), "satisfaction_by_education_level", pgUrl, pgUser, pgPwd)
        writeStatToPostgres(satisfactionByReligion(cleanedDF), "satisfaction_by_religion_arranged", pgUrl, pgUser, pgPwd)
        writeStatToPostgres(satisfactionByIncomeLevel(cleanedDF), "satisfaction_by_income_level", pgUrl, pgUser, pgPwd)
        writeStatToPostgres(divorcesByReligion(cleanedDF), "divorces_by_religion", pgUrl, pgUser, pgPwd)
        writeStatToPostgres(divorcesByEducation(cleanedDF), "divorces_by_education", pgUrl, pgUser, pgPwd)
        writeStatToPostgres(divorcesBySpouseWorking(cleanedDF), "divorces_by_spouse_working", pgUrl, pgUser, pgPwd)

        cleanedDF.unpersist()
        ()
      }
      .start()



    // ======= STATISTICS ========

    // Ici on checkpoint + overwrite. On fait les groupby sur les chunks du full dataset. On as donc besoin d'une "save" du groupBy, pour les cumulés (d'ou le checkpoint)
    // Et on overwrite pour cumulé d'un coup.

    val queries = List(
      insertFullDataset,
      statsStream
    )


    queries.foreach(_.awaitTermination())
  }
}