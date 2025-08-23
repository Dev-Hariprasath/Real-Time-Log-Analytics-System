ThisBuild / scalaVersion := "2.12.18"
val sparkVer = "3.5.1"

lazy val root = (project in file(".")).settings(
  name := "real-time-log-analytics",
  version := "0.1.0",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVer,
    "io.delta" %% "delta-spark" % "3.2.0",
    "com.typesafe" % "config" % "1.4.3",

    // Logging
    "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
    "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
    "org.slf4j" % "slf4j-api" % "1.7.36",
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.23.1",

    // Kafka + DB
    "org.apache.kafka" % "kafka-clients" % "3.7.0",
    "org.postgresql" % "postgresql" % "42.7.4"
  )
)
