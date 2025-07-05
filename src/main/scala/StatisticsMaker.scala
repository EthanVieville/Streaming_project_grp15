import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object StatisticsMaker {

  /*
  // Func d'écriture de la nouvelle table en DB.
  def writeStatToSQLite(df: DataFrame, tableName: String, dbPath: String): Unit = {
    df.write
      .mode("overwrite")
      .format("jdbc")
      .option("url", s"jdbc:sqlite:$dbPath")
      .option("dbtable", tableName)
      .option("driver", "org.sqlite.JDBC")
      .save()
  }

   */

  def writeStatToSQLite(df: DataFrame, tableName: String, dbPath: String): Unit = {
    val dfWithTimestamp = df.withColumn("updated_at", current_timestamp())
    dfWithTimestamp.write
      .mode("append") // pas overwrite
      .format("jdbc")
      .option("url", s"jdbc:sqlite:$dbPath")
      .option("dbtable", tableName)
      .option("driver", "org.sqlite.JDBC")
      .save()
  }



  // Combien de mariages LOVE / ARRANGED on as
  def computeBasicStats(df: DataFrame): DataFrame = {
    df.groupBy("Marriage_Type").count()
  }
  // Mariage arrangé et love : satisfaction.
  //Affiché la répartition des niveaux de satisfaction pour Love et Arranged, pour ceux qui ne sont pas divorcés
  def satisfactionByMarriageType(df: DataFrame): DataFrame = {
    df.filter(col("Divorce_Status") =!= "Yes")
      .filter(col("Marriage_Type").isin("Love", "Arranged"))
      .groupBy("Marriage_Type", "Marital_Satisfaction")
      .count()
      .orderBy("Marriage_Type", "Marital_Satisfaction")
  }

  def satisfactionByAgeBracket(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    val toInt = udf((s: String) => {
      try {
        Some(s.toInt)
      } catch {
        case _: Exception => None
      }
    })

    val withAge = df
      .withColumn("AgeInt", toInt(col("Age_at_Marriage")))
      .filter(col("AgeInt").isNotNull)
      .filter(col("Divorce_Status") =!= "Yes")
      .withColumn("age_bracket", when(col("AgeInt") < 20, "<20")
        .when(col("AgeInt").between(20, 24), "20-24")
        .when(col("AgeInt").between(25, 29), "25-29")
        .when(col("AgeInt").between(30, 34), "30-34")
        .otherwise("35+")
      )

    val grouped = withAge
      .groupBy("age_bracket", "Marital_Satisfaction")
      .count()
      .withColumn("count", col("count").cast("int")) // ✅ ici on cast bien le count en int
      .orderBy("age_bracket", "Marital_Satisfaction")

    grouped
  }




}
