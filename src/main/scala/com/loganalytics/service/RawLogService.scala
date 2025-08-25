package com.loganalytics.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RawLogService {

  def prepare(df: DataFrame): DataFrame = {
    val withEventTime =
      if (df.columns.contains("timestamp")) df.withColumn("event_time", to_timestamp(col("timestamp")))
      else if (df.columns.contains("event_time")) df.withColumn("event_time", col("event_time").cast("timestamp"))
      else throw new RuntimeException("No timestamp/event_time column found in raw logs")

    withEventTime.select(
      col("event_time"),
      col("level"),
      col("service"),
      col("path"),
      col("status"),
      col("latencyMs").as("latency_ms"),
      col("msg"),
      col("userId").as("user_id"),
      col("host"),
      col("ip"),
      col("requestId").as("request_id"),
      col("sessionId").as("session_id")
    )
  }
}

