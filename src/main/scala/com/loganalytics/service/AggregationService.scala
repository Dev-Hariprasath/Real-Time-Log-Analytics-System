package com.loganalytics.service

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AggregationService {
  def buildAggregates(df: DataFrame, withWatermark: Boolean, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val watermarkDur = "5 minutes"
    val windowDur    = "1 minute"
    val slideDur     = "1 minute"

    val base = if (withWatermark) df.withWatermark("event_time", watermarkDur) else df

    base
      .groupBy(
        window($"event_time", windowDur, slideDur),
        $"service"
      )
      .agg(
        count(lit(1)).as("events"),
        sum(when($"status" >= 500, 1).otherwise(0)).cast("long").as("errors"),
        avg($"latency_ms").cast("double").as("latency_ms")
      )
      .select(
        col("window.start").as("windowStart"),
        col("window.end").as("windowEnd"),
        col("service"),
        col("events").cast("long"),
        col("errors").cast("long"),
        col("latency_ms").cast("double")
      )
  }
}

