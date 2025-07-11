import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import StatisticsMaker._
import load_schema._
import schemas.schemas._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object KafkaToSQLiteConsumer {



  def main(args: Array[String]): Unit = {

    // === LOAD PROPERTIES ====
    val props = new Properties()
    val stream = new FileInputStream("src/config/app.properties")
    props.load(stream)

    // === POSTGRE ACCESS ===
    val pgUrl = props.getProperty("postgres.url")
    val pgUser = props.getProperty("postgres.user")
    val pgPwd = props.getProperty("postgres.password")

    // === FUNC pour write DF to postegre
    def writeStatStream(df: DataFrame, checkpointPath: String, tableName: String): StreamingQuery = {
      df.writeStream
        .outputMode("complete")
        .option("checkpointLocation", checkpointPath)
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          writeStatToPostgres(batchDF, tableName, pgUrl, pgUser, pgPwd, overwrite = true)
        }
        .start()
    }


    // === Init Spark ===
    val spark = SparkSession.builder()
      .appName("KafkaToPostgreConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._


    // DROP BEFORE INSERT
    val drop = 1
    if (drop == 1) dropExistingTables(pgUrl, pgUser, pgPwd)

    // INIT SCHEMA (TABLES) SQL
    executeSqlFile("src/config/schema.sql", pgUrl, pgUser, pgPwd)

    // === STREAM KAFKA
    val parsedDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", props.getProperty("kafka.brokers"))
      .option("subscribe", props.getProperty("kafka.topic"))
      .option("startingOffsets", "latest") // earliest / latest
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", marriageSchema).as("data")) // on utilise bien le schéma importé
      .select("data.*")


    // === WRITER POSTGRES personnalisé pour insérer dans POSTGRE
    val postgresWriter  = new ForeachWriter[Row] {
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
      .foreach(postgresWriter )
      .outputMode("append")
      .start()


    // ======= STATISTICS ========

    // -1
    val statsSatisfactionByMariageType = writeStatStream(satisfactionByMarriageType(parsedDF), "checkpoints/statistics/satisfactionMariage", "satisfaction_by_marriage_type")

    // -2
    val statSatisfactionByAge = writeStatStream(satisfactionByAgeRange(parsedDF), "checkpoints/statistics/satisfactionAgeRange", "satisfaction_by_age_range")

    // -3
    val statSatisfactionByEducation = writeStatStream(satisfactionByEducationLevel(parsedDF), "checkpoints/statistics/satisfactionEducationLevel", "satisfaction_by_education_level")

    // -4
    val statSatisfactionByReligion = writeStatStream(satisfactionByReligionArranged(parsedDF), "checkpoints/statistics/satisfactionReligionArranged", "satisfaction_by_religion_arranged")

    // -5
    val statSatisfactionInterReligion = writeStatStream(satisfactionByInterReligion(parsedDF), "checkpoints/statistics/satisfactionInterReligion", "satisfaction_by_inter_religion")

    // -6
    val statSatisfactionIncomeLevel = writeStatStream(satisfactionByIncomeLevel(parsedDF), "checkpoints/statistics/satisfactionIncomeLevel", "satisfaction_by_income_level")

    // -7
    val statSatisfactionParentalApproval = writeStatStream(satisfactionByParentalApproval(parsedDF), "checkpoints/statistics/satisfactionParentalApproval", "satisfaction_by_parental_approval")

    // -8
    val statQueryDivorcesByReligion = writeStatStream(divorcesByReligion(parsedDF), "checkpoints/statistics/divorcesByReligion", "divorces_by_religion")

    // -9
    val statQueryDivorcesBySpouseWorking = writeStatStream(divorcesByEducation(parsedDF), "checkpoints/statistics/divorcesByEducation", "divorces_by_education")

    // -10
    val statQueryDivorcesByEducation = writeStatStream(divorcesBySpouseWorking(parsedDF), "checkpoints/statistics/divorcesBySpouseWorking", "divorces_by_spouse_working")

    // -11
    val statQueryChildrenByProfile = writeStatStream(childrenCountByIdealProfile(parsedDF), "checkpoints/statistics/childrenByProfile", "children_count_by_profile")


    InsertFullDataSet.awaitTermination()
    statsSatisfactionByMariageType.awaitTermination()
    statSatisfactionByAge.awaitTermination()
    statSatisfactionByEducation.awaitTermination()
    statSatisfactionByReligion.awaitTermination()
    statSatisfactionInterReligion.awaitTermination()
    statSatisfactionIncomeLevel.awaitTermination()
    statSatisfactionParentalApproval.awaitTermination()
    statQueryDivorcesByReligion.awaitTermination()
    statQueryDivorcesByEducation.awaitTermination()
    statQueryDivorcesBySpouseWorking.awaitTermination()
    statQueryChildrenByProfile.awaitTermination()



  }


}