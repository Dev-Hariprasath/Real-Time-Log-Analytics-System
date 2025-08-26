package com.loganalytics.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MongoLogService {

  def prepare(df: DataFrame): DataFrame = {
    df
      .drop("_id", "_class")
      .select(
        col("timestamp").cast("string"),
        col("level").cast("string"),
        col("service").cast("string"),
        col("path").cast("string"),
        col("status").cast("int"),
        col("latencyMs").cast("long"),
        col("msg").cast("string"),
        col("userId").cast("string"),
        col("host").cast("string"),
        col("ip").cast("string"),
        col("requestId").cast("string"),
        col("sessionId").cast("string")
      )
  }
}
