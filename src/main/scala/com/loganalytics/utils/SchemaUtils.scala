package com.loganalytics

import org.apache.spark.sql.types._

object SchemaUtils {
  val logSchema: StructType = StructType(Seq(
    StructField("timestamp", StringType, true),
    StructField("level", StringType, true),
    StructField("service", StringType, true),
    StructField("path", StringType, true),
    StructField("status", IntegerType, true),
    StructField("latencyms", IntegerType, true),
    StructField("msg", StringType, true),
    StructField("userId", StringType, true),
    StructField("host", StringType, true),
    StructField("ip", StringType, true),
    StructField("requestId", StringType, true),
    StructField("sessionId", StringType, true)
  ))
}
