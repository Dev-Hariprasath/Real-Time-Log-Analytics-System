package com.loganalytics.service

import com.loganalytics.config.AppConfig
import com.loganalytics.utils.LogLevels
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object LogProcessingService {
  def errorsAndWarnings(df: DataFrame): DataFrame =
    df.filter(col("level").isin(LogLevels.ERROR, LogLevels.WARN))

  def metricsPerWindow(df: DataFrame): DataFrame = {
    df.withWatermark("ts", AppConfig.watermark)
      .groupBy(
        window(col("ts"), AppConfig.windowDuration, AppConfig.windowSlide),
        col("service"),
        col("level")
      )
      .agg(
        count(lit(1)).as("count"),
        avg(col("latencyMs")).cast("double").as("avg_latency"),
        expr("percentile_approx(latencyMs, 0.95)").cast("double").as("p95_latency")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("service"), col("level"), col("count"), col("avg_latency"), col("p95_latency")
      )
  }

  def errorRateFromMetrics(metrics: DataFrame): DataFrame = {
    metrics
      .groupBy(col("window_start"), col("window_end"), col("service"))
      .agg(
        sum(when(col("level").isin(LogLevels.ERROR, LogLevels.WARN), col("count")).otherwise(lit(0))).as("error_count"),
        sum(col("count")).as("total_count"),
        avg(col("p95_latency")).as("p95_latency_avg")
      )
      .withColumn("error_rate", when(col("total_count") === 0, lit(0.0))
        .otherwise(col("error_count") / col("total_count")))
  }
}

