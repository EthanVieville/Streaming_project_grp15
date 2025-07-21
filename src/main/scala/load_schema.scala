import java.sql.DriverManager
import scala.io.Source
import scala.util.Using


object load_schema {
  def executeSqlFile(path: String, pgUrl: String, pgUser: String, pgPwd: String): Unit = {
    val conn = DriverManager.getConnection(pgUrl, pgUser, pgPwd)
    val stmt = conn.createStatement() // statement, pour exec une rqt SQL

    try {
      val sql = Using.resource(Source.fromFile(path)) { source =>
        source.mkString // lis tout le schema.sql comme une seule str
      }
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
