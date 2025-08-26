ThisBuild / scalaVersion := "2.12.18"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
  "org.postgresql"   %  "postgresql" % "42.7.3",
  "com.typesafe"     %  "config" % "1.4.3",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0"
)

lazy val root = (project in file(".")).settings(
  name := "real-time-log-analytics",
  version := "0.1.0"
)
