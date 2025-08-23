package com.loganalytics.utils

import org.apache.spark.sql.types._

object SchemaUtils {
  // JSON schema expected from producers
  val logSchema: StructType = StructType(Seq(
    StructField("timestamp", StringType),
    StructField("level", StringType),
    StructField("service", StringType),
    StructField("path", StringType),
    StructField("status", IntegerType),
    StructField("latencyMs", IntegerType),
    StructField("msg", StringType),
    StructField("userId", StringType),
    StructField("host", StringType),
    StructField("ip", StringType),
    StructField("requestId", StringType),
    StructField("sessionId", StringType)
  ))
}

