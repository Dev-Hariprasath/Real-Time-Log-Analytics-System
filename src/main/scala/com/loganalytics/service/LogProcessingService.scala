package com.loganalytics.service

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object LogProcessingService {
  def aggregate(df: DataFrame): DataFrame = {
    // Ensure event_time exists
    val withEventTime =
      if (df.columns.contains("event_time")) df
      else df.withColumn("event_time", to_timestamp(col("timestamp")))

    // Ensure latency_ms is double (and tolerate strings/empties)
    val withLatency =
      if (withEventTime.columns.contains("latency_ms"))
        withEventTime.withColumn(
          "latency_ms",
          when(trim(col("latency_ms").cast("string")) === "" || col("latency_ms").isNull, lit(null).cast("double"))
            .otherwise(col("latency_ms").cast("double"))
        )
      else if (withEventTime.columns.contains("latencyMs"))
        withEventTime.withColumn(
          "latency_ms",
          when(trim(col("latencyMs").cast("string")) === "" || col("latencyMs").isNull, lit(null).cast("double"))
            .otherwise(col("latencyMs").cast("double"))
        )
      else
        withEventTime.withColumn("latency_ms", lit(null).cast("double"))

    // Ensure status is numeric for comparisons
    val withStatusNum =
      if (withLatency.schema("status").dataType.typeName == "integer" || withLatency.schema("status").dataType.typeName == "long")
        withLatency.withColumn("status_num", col("status").cast("long"))
      else
        withLatency.withColumn("status_num",
          when(trim(col("status").cast("string")) === "" || col("status").isNull, lit(null).cast("long"))
            .otherwise(col("status").cast("long"))
        )

    // Stable error flag as Long to keep agg buffer type consistent
    val withErrorFlag = withStatusNum.withColumn(
      "error_flag",
      when(col("status_num") >= 500, lit(1L)).otherwise(lit(0L))
    )

    withErrorFlag
      .withWatermark("event_time", AppConfig.watermark)
      .groupBy(window(col("event_time"), AppConfig.window), col("service"))
      .agg(
        count(lit(1)).as("events"),
        sum(col("error_flag")).cast("long").as("errors"),
        avg(col("latency_ms")).as("latency_ms") // <-- FIXED: use latency_ms
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
