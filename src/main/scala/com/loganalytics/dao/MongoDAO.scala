package com.loganalytics.dao

import com.loganalytics.config.AppConfig
import com.loganalytics.SchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoDAO {
  def loadLogs(spark: SparkSession): DataFrame = {
    println(s"[MongoDAO] ✅ Attempting to load '${AppConfig.mongoDb}.${AppConfig.mongoColl}'")

    val df = spark.read
      .format("mongodb")
      .option("spark.mongodb.connection.uri", AppConfig.mongoUri)
      .option("database", AppConfig.mongoDb)
      .option("collection", AppConfig.mongoColl)
      .schema(SchemaUtils.logSchema) // enforce schema
      .load()

    println(s"[MongoDAO] Count = ${df.count()}")
    df.printSchema()
    df.show(5, truncate = false)

    df
  }

  def loadServiceMetadata(spark: SparkSession): Map[String, String] = {
    try {
      val df = spark.read
        .format("mongodb")
        .option("spark.mongodb.connection.uri", AppConfig.mongoUri)
        .option("database", AppConfig.mongoDb)
        .option("collection", "service_metadata")
        .load()

      if (df.columns.contains("service") && df.columns.contains("label")) {
        df.select("service", "label")
          .collect()
          .map(r => r.getString(0) -> r.getString(1))
          .toMap
      } else {
        println(s"[MongoDAO] ⚠️ service_metadata collection missing required columns [service,label]")
        Map.empty[String, String]
      }
    } catch {
      case e: Throwable =>
        println(s"[MongoDAO] ⚠️ Could not load service metadata: ${e.getMessage}")
        Map.empty[String, String]
    }
  }
}
