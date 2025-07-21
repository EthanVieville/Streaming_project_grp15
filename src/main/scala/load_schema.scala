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
      println(" [CLEANING] Drop ALL user tables...")

      // Tape le schéma public pas sytème
      val rs = stmt.executeQuery(
        """
           SELECT tablename FROM pg_tables WHERE schemaname = 'public';
        """.stripMargin)

      val tables = Iterator.continually((rs.next(), rs))
        .takeWhile(_._1)
        .map(_._2.getString("tablename"))
        .toList

      tables.foreach { table =>
        stmt.execute(s"""DROP TABLE IF EXISTS "$table" CASCADE;""")
      }

      println(" [CLEANING] All tables dropped.")
    } catch {
      case e: Exception =>
        println(s"[ERROR] DROP TABLE FAILED : ${e.getMessage}")
    } finally {
      stmt.close()
      conn.close()
    }
  }


}
