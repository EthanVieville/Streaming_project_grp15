package schemas

import org.apache.spark.sql.types._

object schemas {
  val marriageSchema: StructType = StructType(Seq(
    StructField("ID", StringType),
    StructField("Marriage_Type", StringType),
    StructField("Age_at_Marriage", StringType),
    StructField("Gender", StringType),
    StructField("Education_Level", StringType),
    StructField("Caste_Match", StringType),
    StructField("Religion", StringType),
    StructField("Parental_Approval", StringType),
    StructField("Urban_Rural", StringType),
    StructField("Dowry_Exchanged", StringType),
    StructField("Marital_Satisfaction", StringType),
    StructField("Divorce_Status", StringType),
    StructField("Children_Count", StringType),
    StructField("Income_Level", StringType),
    StructField("Years_Since_Marriage", StringType),
    StructField("Spouse_Working", StringType),
    StructField("Inter-Caste", StringType),
    StructField("Inter-Religion", StringType)
  ))

}
