package com.loganalytics.service

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AlertingService {
  def buildAlertsFrom(metricsByLevel: DataFrame): DataFrame = {
    val rolled = LogProcessingService.errorRateFromMetrics(metricsByLevel)

    val highErrorRate = rolled
      .filter(col("error_rate") > AppConfig.alertErrorRateThreshold)
      .withColumn("kind", lit("HIGH_ERROR_RATE"))
      .withColumn("value", to_json(struct(
        col("window_start"), col("window_end"), col("service"), col("error_rate"), col("p95_latency_avg"), col("kind")
      )))
      .select("value")

    val highLatency = rolled
      .filter(col("p95_latency_avg") > AppConfig.alertP95LatencyThresholdMs)
      .withColumn("kind", lit("HIGH_P95_LATENCY"))
      .withColumn("value", to_json(struct(
        col("window_start"), col("window_end"), col("service"), col("error_rate"), col("p95_latency_avg"), col("kind")
      )))
      .select("value")

    highErrorRate.unionByName(highLatency)
  }
}

