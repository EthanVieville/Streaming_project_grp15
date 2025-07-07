import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.sql.{Connection, DriverManager, PreparedStatement}


object StatisticsMaker {


  def writeStatToPostgres(df: DataFrame, tableName: String, url: String, user: String, password: String, overwrite: Boolean = false): Unit = {
    df.write
      .mode(if (overwrite) "overwrite" else "append")
      .format("jdbc")
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .save()
  }

  def satisfactionByMarriageType(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Marriage_Type").isin("Love", "Arranged")
      )
      .groupBy("Marriage_Type", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  def satisfactionByAgeRange(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Age_at_Marriage").isNotNull &&
          col("Marriage_Type") === "Arranged"
      )
      .withColumn("Age_Int", col("Age_at_Marriage").cast("int"))
      .withColumn("Age_Range", when(col("Age_Int").between(0, 17), "0-17")
        .when(col("Age_Int").between(18, 22), "18-22")
        .when(col("Age_Int").between(23, 26), "23-26")
        .when(col("Age_Int").between(27, 30), "27-30")
        .when(col("Age_Int").between(31, 35), "31-35")
        .when(col("Age_Int") > 35, "36+")
        .otherwise("Unknown")
      )
      .groupBy("Age_Range", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  def satisfactionByEducationLevel(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Education_Level").isNotNull &&
          col("Marriage_Type") === "Arranged"
      )
      .groupBy("Education_Level", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  def satisfactionByReligionArranged(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Marriage_Type") === "Arranged" &&
          col("Religion").isNotNull
      )
      .groupBy("Religion", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  def satisfactionByInterReligion(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Marriage_Type") === "Arranged" &&
          col("Inter-Religion") === "Yes" &&
          col("Religion").isNotNull
      )
      .groupBy("Religion", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  def satisfactionByIncomeLevel(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Marriage_Type") === "Arranged" &&
          col("Income_Level").isNotNull
      )
      .groupBy("Income_Level", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  def satisfactionByParentalApproval(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Marriage_Type") === "Arranged" &&
          col("Parental_Approval").isNotNull
      )
      .groupBy("Parental_Approval", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  def divorcesByReligion(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") === "Yes" &&
          col("Marriage_Type") === "Arranged" &&
          col("Religion").isNotNull
      )
      .groupBy("Religion")
      .agg(count("*").alias("Count"))
  }

  def divorcesByEducation(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") === "Yes" &&
          col("Marriage_Type") === "Arranged" &&
          col("Education_Level").isNotNull
      )
      .groupBy("Education_Level")
      .agg(count("*").alias("Count"))
  }

  def divorcesBySpouseWorking(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") === "Yes" &&
          col("Marriage_Type") === "Arranged" &&
          col("Spouse_Working").isNotNull
      )
      .groupBy("Spouse_Working")
      .agg(count("*").alias("Count"))
  }

  def childrenCountByIdealProfile(df: DataFrame): DataFrame = {
    df.filter(
        col("Marriage_Type") === "Arranged" &&
          col("Marital_Satisfaction") === "High" &&
          col("Age_at_Marriage").cast("int").between(27, 30) &&
          col("Education_Level") === "Postgraduate" &&
          col("Religion") === "Muslim" &&
          col("Income_Level") === "Middle" &&
          col("Parental_Approval") === "Partial" &&
          col("Spouse_Working") === "No"
      )
      .groupBy("Children_Count")
      .agg(count("*").alias("Count"))
  }






}
