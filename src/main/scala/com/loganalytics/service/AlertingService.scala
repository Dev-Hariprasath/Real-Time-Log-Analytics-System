package com.loganalytics.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AlertingService {
  // simple example: create small alert rows for error statuses
  def detectErrors(df: DataFrame): DataFrame = {
    val withEventTime = if (df.columns.contains("event_time")) df else df.withColumn("event_time", to_timestamp(col("timestamp")))
    withEventTime
      .filter(col("status") >= 500)
      .select(
        col("event_time"),
        col("service"),
        col("status"),
        col("msg"),
        col("requestId"),
        col("host")
      )
      .withColumn("alert_time", current_timestamp())
  }
}
