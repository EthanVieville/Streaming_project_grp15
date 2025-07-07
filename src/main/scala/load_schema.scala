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
      stmt.execute("DROP TABLE IF EXISTS marriages CASCADE;")
      stmt.execute("DROP TABLE IF EXISTS satisfaction_by_marriage_type CASCADE;")
      stmt.execute("DROP TABLE IF EXISTS meta CASCADE;")
      println(" [CLEANING] Cleaning done.")
    } catch {
      case e: Exception =>
        println(s"PROBLEME ALED ALED ALED : ${e.getMessage}")
    } finally {
      stmt.close()
      conn.close()
    }
  }
}
