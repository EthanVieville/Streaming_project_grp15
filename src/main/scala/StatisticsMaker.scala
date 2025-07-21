import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object StatisticsMaker {


  def writeStatToPostgres(df: DataFrame, tableName: String, url: String, user: String, password: String, overwrite: Boolean = false): Unit = {
    df.write
      .mode(if (overwrite) "overwrite" else "append") // on overwrite pas par défault. (même si on le fait au final mdr)
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
          col("Marriage_Type").isin("Arranged", "Love")
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
      .groupBy("Age_Range", "Marriage_Type", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }


  def satisfactionByEducationLevel(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Education_Level").isNotNull &&
          col("Marriage_Type").isin("Arranged", "Love")
      )
      .groupBy("Education_Level", "Marriage_Type", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }


  def satisfactionByReligion(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Religion").isNotNull &&
          col("Marriage_Type").isin("Arranged", "Love")
      )
      .groupBy("Religion", "Marriage_Type", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }

  def satisfactionByIncomeLevel(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") =!= "Yes" &&
          col("Income_Level").isNotNull &&
          col("Marriage_Type").isin("Arranged", "Love")
      )
      .groupBy("Income_Level", "Marriage_Type", "Marital_Satisfaction")
      .agg(count("*").alias("Count"))
  }



  def divorcesByReligion(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") === "Yes" &&
          col("Religion").isNotNull &&
          col("Marriage_Type").isin("Arranged", "Love")
      )
      .groupBy("Religion", "Marriage_Type")
      .agg(count("*").alias("Count"))
  }


  def divorcesByEducation(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") === "Yes" &&
          col("Education_Level").isNotNull &&
          col("Marriage_Type").isin("Arranged", "Love")
      )
      .groupBy("Education_Level", "Marriage_Type")
      .agg(count("*").alias("Count"))
  }


  def divorcesBySpouseWorking(df: DataFrame): DataFrame = {
    df.filter(
        col("Divorce_Status") === "Yes" &&
          col("Spouse_Working").isNotNull &&
          col("Marriage_Type").isin("Arranged", "Love")
      )
      .groupBy("Spouse_Working", "Marriage_Type")
      .agg(count("*").alias("Count"))
  }







}
