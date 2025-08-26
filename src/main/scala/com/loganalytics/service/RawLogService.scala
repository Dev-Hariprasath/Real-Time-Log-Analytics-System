package com.loganalytics.service

import com.loganalytics.SchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object RawLogService {

  def prepare(df: DataFrame): DataFrame = {
    val tz = df.sparkSession.sessionState.conf.sessionLocalTimeZone

    val parsed = df
      .withColumn(
        "event_time_parsed",
        coalesce(
          // 9-digit fraction with 'Z' (e.g., 2025-08-26T04:35:31.694520700Z)
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"),
          // 6-digit fraction with 'Z'
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
          // 3-digit fraction with 'Z'
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
          // No fraction with 'Z'
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          // Fallback: let Spark try
          to_timestamp(col("timestamp"))
        )
      )
      .filter(col("event_time_parsed").isNotNull)

    // Conform exactly to SchemaUtils.rawUnifiedSchema
    val normalized = parsed.select(
      col("event_time_parsed").cast(TimestampType).as("event_time"),
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

    normalized.select(SchemaUtils.rawUnifiedSchema.fieldNames.map(col): _*)
  }
}
