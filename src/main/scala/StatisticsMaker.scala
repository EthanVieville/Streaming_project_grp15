import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.sql.{Connection, DriverManager, PreparedStatement}


object StatisticsMaker {


  def writeStatToPostgres(df: DataFrame, tableName: String, url: String, user: String, password: String): Unit = {
    df.write
      .mode("append")  // écrase à chaque fois
      .format("jdbc")
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .save()
  }

  // Mariage arrangé et love : satisfaction.
  //Affiché la répartition des niveaux de satisfaction pour Love et Arranged, pour ceux qui ne sont pas divorcés
  def satisfactionByMarriageType(df: DataFrame): DataFrame = {
    df.filter(col("Divorce_Status") =!= "Yes")
      .filter(col("Marriage_Type").isin("Love", "Arranged"))
      .groupBy("Marriage_Type", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  // =================================== DROP
}
