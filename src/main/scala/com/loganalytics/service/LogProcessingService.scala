package com.loganalytics.service

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object LogProcessingService {
  def aggregate(df: DataFrame): DataFrame = {
    // expects df to contain event_time: TimestampType and service column
    df.withWatermark("event_time", AppConfig.watermark)
      .groupBy(window(col("event_time"), AppConfig.window), col("service"))
      .agg(
        count("*").as("events"),
        sum(when(col("status") >= 500, 1).otherwise(0)).as("errors"),
        avg("latencyMs").as("avg_latency")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("service"),
        col("events"),
        col("errors"),
        col("avg_latency")
      )
  }
}
