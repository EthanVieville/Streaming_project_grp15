import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.sql.DriverManager

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
        col("Education_Level") === "Postgraduate" &&
          col("Religion") === "Muslim" &&
          col("Income_Level") === "Middle" &&
          col("Parental_Approval") === "High" &&
          col("Age_at_Marriage") === "27–30" &&
          col("Spouse_Working") === "No" &&
          col("Children_Count").isNotNull &&
          trim(col("Children_Count")) =!= "" &&
          col("Children_Count").rlike("^[0-9]+$")
      )
      .groupBy("Children_Count")
      .agg(count("*").alias("Count"))
  }

  // 1. Satisfaction par type de mariage
  def satisfactionByMarriageTypeLong(df: DataFrame): DataFrame = {
    satisfactionByMarriageType(df)
      .withColumn("stat_category", lit("satisfaction_by_marriage_type"))
      .withColumnRenamed("Marriage_Type", "marriages_value")
      .withColumnRenamed("Marital_Satisfaction", "metric_value")
      .withColumn("marriages_col", lit("Marriage_Type"))
      .withColumn("grp_by_name", lit("Marital_Satisfaction"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 2. Satisfaction par tranche d’âge
  def satisfactionByAgeRangeLong(df: DataFrame): DataFrame = {
    satisfactionByAgeRange(df)
      .withColumn("stat_category", lit("satisfaction_by_age_range"))
      .withColumnRenamed("Age_Range", "marriages_value")
      .withColumnRenamed("Marital_Satisfaction", "metric_value")
      .withColumn("marriages_col", lit("Age_Range"))
      .withColumn("grp_by_name", lit("Marital_Satisfaction"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 3. Satisfaction par niveau d'éducation
  def satisfactionByEducationLevelLong(df: DataFrame): DataFrame = {
    satisfactionByEducationLevel(df)
      .withColumn("stat_category", lit("satisfaction_by_education"))
      .withColumnRenamed("Education_Level", "marriages_value")
      .withColumnRenamed("Marital_Satisfaction", "metric_value")
      .withColumn("marriages_col", lit("Education_Level"))
      .withColumn("grp_by_name", lit("Marital_Satisfaction"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 4. Satisfaction par religion (mariage arrangé)
  def satisfactionByReligionArrangedLong(df: DataFrame): DataFrame = {
    satisfactionByReligionArranged(df)
      .withColumn("stat_category", lit("satisfaction_by_religion_arranged"))
      .withColumnRenamed("Religion", "marriages_value")
      .withColumnRenamed("Marital_Satisfaction", "metric_value")
      .withColumn("marriages_col", lit("Religion"))
      .withColumn("grp_by_name", lit("Marital_Satisfaction"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 5. Satisfaction inter-religion
  def satisfactionByInterReligionLong(df: DataFrame): DataFrame = {
    satisfactionByInterReligion(df)
      .withColumn("stat_category", lit("satisfaction_by_inter_religion"))
      .withColumnRenamed("Religion", "marriages_value")
      .withColumnRenamed("Marital_Satisfaction", "metric_value")
      .withColumn("marriages_col", lit("Religion"))
      .withColumn("grp_by_name", lit("Marital_Satisfaction"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 6. Satisfaction par niveau de revenu
  def satisfactionByIncomeLevelLong(df: DataFrame): DataFrame = {
    satisfactionByIncomeLevel(df)
      .withColumn("stat_category", lit("satisfaction_by_income"))
      .withColumnRenamed("Income_Level", "marriages_value")
      .withColumnRenamed("Marital_Satisfaction", "metric_value")
      .withColumn("marriages_col", lit("Income_Level"))
      .withColumn("grp_by_name", lit("Marital_Satisfaction"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 7. Satisfaction selon approbation parentale
  def satisfactionByParentalApprovalLong(df: DataFrame): DataFrame = {
    satisfactionByParentalApproval(df)
      .withColumn("stat_category", lit("satisfaction_by_parental_approval"))
      .withColumnRenamed("Parental_Approval", "marriages_value")
      .withColumnRenamed("Marital_Satisfaction", "metric_value")
      .withColumn("marriages_col", lit("Parental_Approval"))
      .withColumn("grp_by_name", lit("Marital_Satisfaction"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 8. Divorces par religion
  def divorcesByReligionLong(df: DataFrame): DataFrame = {
    divorcesByReligion(df)
      .withColumn("stat_category", lit("divorces_by_religion"))
      .withColumnRenamed("Religion", "marriages_value")
      .withColumn("marriages_col", lit("Religion"))
      .withColumn("grp_by_name", lit("Divorce_Count"))
      .withColumn("metric_value", lit("count")) // Valeur arbitraire, car pas de second groupement
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 9. Divorces par éducation
  def divorcesByEducationLong(df: DataFrame): DataFrame = {
    divorcesByEducation(df)
      .withColumn("stat_category", lit("divorces_by_education"))
      .withColumnRenamed("Education_Level", "marriages_value")
      .withColumn("marriages_col", lit("Education_Level"))
      .withColumn("grp_by_name", lit("Divorce_Count"))
      .withColumn("metric_value", lit("count"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }

  // 10. Divorces par statut pro du conjoint
  def divorcesBySpouseWorkingLong(df: DataFrame): DataFrame = {
    divorcesBySpouseWorking(df)
      .withColumn("stat_category", lit("divorces_by_spouse_working"))
      .withColumnRenamed("Spouse_Working", "marriages_value")
      .withColumn("marriages_col", lit("Spouse_Working"))
      .withColumn("grp_by_name", lit("Divorce_Count"))
      .withColumn("metric_value", lit("count"))
      .select("stat_category", "marriages_col", "marriages_value", "grp_by_name", "metric_value", "Count")
  }



}
