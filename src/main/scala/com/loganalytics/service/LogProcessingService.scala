package com.loganalytics.service

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object LogProcessingService {
  def aggregate(df: DataFrame): DataFrame = {
    val withEventTime =
      if (df.columns.contains("event_time")) df
      else df.withColumn("event_time", to_timestamp(col("timestamp")))

    val withLatency =
      if (df.columns.contains("latency_ms")) {
        withEventTime.withColumn(
          "latency_ms",
          when(
            col("latency_ms").isNotNull && trim(col("latency_ms").cast("string")) =!= "",
            col("latency_ms").cast("double")
          )
        )
      } else {
        withEventTime.withColumn("latency_ms", lit(null).cast("double"))
      }


    // Pre-compute a Long 0/1 for errors to stabilize sum buffer type
    val withErrorFlag =
      withLatency.withColumn("error_flag", when(col("status") >= 500, lit(1L)).otherwise(lit(0L)))

    withErrorFlag
      .withWatermark("event_time", AppConfig.watermark)
      .groupBy(window(col("event_time"), AppConfig.window), col("service"))
      .agg(
        count(lit(1)).as("events"),
        sum(col("error_flag")).cast("long").as("errors"),
        avg(col("latencyMs")).as("latency_ms") // avg ignores NULLs
      )
      .select(
        col("window.start").as("windowStart"),
        col("window.end").as("windowEnd"),
        col("service"),
        col("events"),
        col("errors"),
        col("latency_ms")
      )
  }
}
