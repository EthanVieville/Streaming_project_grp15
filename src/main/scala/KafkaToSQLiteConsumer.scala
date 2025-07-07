import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}
import java.io.FileInputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import StatisticsMaker._
import load_schema._
import schemas.schemas._

import org.apache.spark.sql.streaming.Trigger

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
    val stats = satisfactionByMarriageType(parsedDF)

    val statsSatisfactionByMariageType = stats.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/satisfactionMariage")
      .foreachBatch { (batchDF: DataFrame,  batchId: Long) =>
        writeStatToPostgres(batchDF, "satisfaction_by_marriage_type", pgUrl, pgUser, pgPwd,  overwrite = true)
      }
      .start()

    // -2
    val statsByAge = satisfactionByAgeRange(parsedDF)

    val statSatisfactionByAge = statsByAge.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/satisfactionAgeRange")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "satisfaction_by_age_range", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -3
    val statsByEducation = satisfactionByEducationLevel(parsedDF)

    val statSatisfactionByEducation = statsByEducation.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/satisfactionEducationLevel")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "satisfaction_by_education_level", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -4
    val statsByReligion = satisfactionByReligionArranged(parsedDF)

    val statSatisfactionByReligion = statsByReligion.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/satisfactionReligionArranged")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "satisfaction_by_religion_arranged", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -5
    val statsInterReligion = satisfactionByInterReligion(parsedDF)

    val statSatisfactionInterReligion = statsInterReligion.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/satisfactionInterReligion")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "satisfaction_by_inter_religion", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -6
    val statsIncomeLevel = satisfactionByIncomeLevel(parsedDF)

    val statSatisfactionIncomeLevel = statsIncomeLevel.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/satisfactionIncomeLevel")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "satisfaction_by_income_level", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -7
    val statsParentalApproval = satisfactionByParentalApproval(parsedDF)

    val statSatisfactionParentalApproval = statsParentalApproval.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/satisfactionParentalApproval")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "satisfaction_by_parental_approval", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -8
    val statsDivorcesByReligion = divorcesByReligion(parsedDF)

    val statQueryDivorcesByReligion = statsDivorcesByReligion.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/divorcesByReligion")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "divorces_by_religion", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -9
    val statsDivorcesByEducation = divorcesByEducation(parsedDF)

    val statQueryDivorcesByEducation = statsDivorcesByEducation.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/divorcesByEducation")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "divorces_by_education", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -10
    val statsDivorcesBySpouseWorking = divorcesBySpouseWorking(parsedDF)

    val statQueryDivorcesBySpouseWorking = statsDivorcesBySpouseWorking.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/divorcesBySpouseWorking")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "divorces_by_spouse_working", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()

    // -11
    val statsChildrenByProfile = childrenCountByIdealProfile(parsedDF)

    val statQueryChildrenByProfile = statsChildrenByProfile.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/statistics/childrenByProfile")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        writeStatToPostgres(batchDF, "children_count_by_profile", pgUrl, pgUser, pgPwd, overwrite = true)
      }
      .start()


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