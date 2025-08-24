package com.loganalytics.service

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object LogProcessingService {
  def aggregate(df: DataFrame): DataFrame = {
    // Require that event_time exists; if not, create from timestamp as fallback
    val withEventTime = if (df.columns.contains("event_time")) df else df.withColumn("event_time", to_timestamp(col("timestamp")))

    withEventTime
      .withWatermark("event_time", AppConfig.watermark)
      .groupBy(window(col("event_time"), AppConfig.window), col("service"))
      .agg(
        count("*").as("events"),
        sum(when(col("status") >= 500, 1).otherwise(0)).as("errors"),
        avg("latencyMs").as("avgLatency")
      )
      .select(
        col("window.start").as("windowStart"),
        col("window.end").as("windowEnd"),
        col("service"),
        col("events"),
        col("errors"),
        col("avgLatency")
      )
  }
}
