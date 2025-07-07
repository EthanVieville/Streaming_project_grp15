ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.4",
      "org.apache.spark" %% "spark-sql" % "3.4.4",
      "org.apache.kafka" % "kafka-clients" % "3.7.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1",
      "org.slf4j" % "slf4j-api" % "1.7.36",
      "ch.qos.logback" % "logback-classic" % "1.2.11",
      "org.xerial" % "sqlite-jdbc" % "3.45.3.0",
      "io.spray" %% "spray-json" % "1.3.6",
      "org.slf4j" % "slf4j-simple" % "1.7.36",
      "org.postgresql" % "postgresql" % "42.7.1"
    )

  )