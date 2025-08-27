package com.loganalytics.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AlertingService {
  /** Detect alerts based on error status or high latency */
  def detectAlerts(df: DataFrame, latencyThresholdMs: Int): DataFrame = {
    df.filter(col("status") >= 500 || col("latency_ms") > latencyThresholdMs)
      .withColumn("alert_time", current_timestamp())
      .select(
        col("event_time"),
        col("alert_time"),
        col("service"),
        col("status"),
        col("msg"),
        col("request_id"),
        col("host")
      )
  }
}
