import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object StatisticsMaker {

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




}
