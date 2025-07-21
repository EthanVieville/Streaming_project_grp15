import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object StatisticsMaker {

  def writeStatToPostgres(df: DataFrame, tableName: String, url: String, user: String, password: String, overwrite: Boolean = true): Unit = {
    df.write
      .mode(if (overwrite) "overwrite" else "append")// on overwrite pas par défault. (même si on le fait au final mdr)
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
        col("divorce_status") =!= "Yes" &&
          col("marriage_type").isin("Love", "Arranged")
      )
      .groupBy("marriage_type", "marital_satisfaction")
      .agg(count("*").alias("count"))
  }

  def satisfactionByAgeRange(df: DataFrame): DataFrame = {
    df.filter(
        col("divorce_status") =!= "Yes" &&
          col("age_at_marriage").isNotNull &&
          col("marriage_type").isin("Arranged", "Love")
      )
      .withColumn("age_int", col("age_at_marriage").cast("int"))
      .withColumn("age_range", when(col("age_int").between(0, 17), "0-17")
        .when(col("age_int").between(18, 22), "18-22")
        .when(col("age_int").between(23, 26), "23-26")
        .when(col("age_int").between(27, 30), "27-30")
        .when(col("age_int").between(31, 35), "31-35")
        .when(col("age_int") > 35, "36+")
        .otherwise("unknown")
      )
      .groupBy("age_range", "marriage_type", "marital_satisfaction")
      .agg(count("*").alias("count"))
  }

  def satisfactionByEducationLevel(df: DataFrame): DataFrame = {
    df.filter(
        col("divorce_status") =!= "Yes" &&
          col("education_level").isNotNull &&
          col("marriage_type").isin("Arranged", "Love")
      )
      .groupBy("education_level", "marriage_type", "marital_satisfaction")
      .agg(count("*").alias("count"))
  }

  def satisfactionByReligion(df: DataFrame): DataFrame = {
    df.filter(
        col("divorce_status") =!= "Yes" &&
          col("religion").isNotNull &&
          col("marriage_type").isin("Arranged", "Love")
      )
      .groupBy("religion", "marriage_type", "marital_satisfaction")
      .agg(count("*").alias("count"))
  }

  def satisfactionByIncomeLevel(df: DataFrame): DataFrame = {
    df.filter(
        col("divorce_status") =!= "Yes" &&
          col("income_level").isNotNull &&
          col("marriage_type").isin("Arranged", "Love")
      )
      .groupBy("income_level", "marriage_type", "marital_satisfaction")
      .agg(count("*").alias("count"))
  }

  def divorcesByReligion(df: DataFrame): DataFrame = {
    df.filter(
        col("divorce_status") === "Yes" &&
          col("religion").isNotNull &&
          col("marriage_type").isin("Arranged", "Love")
      )
      .groupBy("religion", "marriage_type")
      .agg(count("*").alias("count"))
  }

  def divorcesByEducation(df: DataFrame): DataFrame = {
    df.filter(
        col("divorce_status") === "Yes" &&
          col("education_level").isNotNull &&
          col("marriage_type").isin("Arranged", "Love")
      )
      .groupBy("education_level", "marriage_type")
      .agg(count("*").alias("count"))
  }

  def divorcesBySpouseWorking(df: DataFrame): DataFrame = {
    df.filter(
        col("divorce_status") === "Yes" &&
          col("spouse_working").isNotNull &&
          col("marriage_type").isin("Arranged", "Love")
      )
      .groupBy("spouse_working", "marriage_type")
      .agg(count("*").alias("count"))
  }
}
