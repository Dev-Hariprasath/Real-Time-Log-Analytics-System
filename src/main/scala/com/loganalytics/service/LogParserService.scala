package com.loganalytics.service

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.loganalytics.SchemaUtils
import org.apache.spark.sql.functions._

object LogParserService {
  def parse(spark: SparkSession, kafkaDf: DataFrame): DataFrame = {
    import spark.implicits._

    val parsed = kafkaDf
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), SchemaUtils.logSchema).as("data"))
      .select("data.*")
      .filter(col("timestamp").isNotNull)

    // Create event_time from timestamp string
    parsed
      .withColumn("event_time",
        coalesce(
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          to_timestamp(col("timestamp"))
        )
      )
      .filter(col("event_time").isNotNull)
      .drop("timestamp")  // Keep only event_time
  }
}