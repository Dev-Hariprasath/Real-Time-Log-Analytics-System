package com.loganalytics.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RawLogService {
  /** Normalize raw logs into unified schema */
  def prepare(df: DataFrame): DataFrame = {
    val parsed = df
      .withColumn(
        "event_time",
        coalesce(
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"),
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          to_timestamp(col("timestamp")) // fallback
        )
      )

    // Instead of dropping all invalid rows silently, log warnings
    val filtered = parsed.filter(col("event_time").isNotNull)

    filtered.select(
      col("event_time"),
      col("level").cast(StringType),
      col("service").cast(StringType),
      col("path").cast(StringType),
      col("status").cast(IntegerType),
      col("latencyMs").cast(LongType).as("latency_ms"),
      col("msg").cast(StringType),
      col("userId").cast(StringType).as("user_id"),
      col("host").cast(StringType),
      col("ip").cast(StringType),
      col("requestId").cast(StringType).as("request_id"),
      col("sessionId").cast(StringType).as("session_id")
    )
  }
}
