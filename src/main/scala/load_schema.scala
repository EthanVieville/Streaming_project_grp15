import java.sql.{Connection, DriverManager}
import scala.io.Source

object load_schema {
  def executeSqlFile(path: String, pgUrl: String, pgUser: String, pgPwd: String): Unit = {
    val conn = DriverManager.getConnection(pgUrl, pgUser, pgPwd)
    val stmt = conn.createStatement()

    try {
      val sql = Source.fromFile(path).mkString
      stmt.execute(sql)
    } finally {
      stmt.close()
      conn.close()
    }
  }

  // ===== (PRE) DROP =====

  def dropExistingTables(pgUrl: String, pgUser: String, pgPwd: String): Unit = {
    val conn = DriverManager.getConnection(pgUrl, pgUser, pgPwd)
    val stmt = conn.createStatement()

    try {
      println(" [CLEANING] Drop tables before insert ....")
      val tablesToDrop = Seq(
        "marriages",
        "satisfaction_by_marriage_type",
        "satisfaction_by_age_range",
        "satisfaction_by_education_level",
        "satisfaction_by_religion_arranged",
        "satisfaction_by_inter_religion",
        "satisfaction_by_income_level",
        "satisfaction_by_parental_approval",
        "divorces_by_religion",
        "divorces_by_education",
        "divorces_by_spouse_working",
        "children_count_by_profile",
        "aggregated_stats"
      )

      tablesToDrop.foreach { table =>
        stmt.execute(s"DROP TABLE IF EXISTS $table CASCADE;")
      }
      println(" [CLEANING] Cleaning done.")
    } catch {
      case e: Exception =>
        println(s"[ERROR] DROP TABLE FAILED : ${e.getMessage}")
    } finally {
      stmt.close()
      conn.close()
    }
  }


}
