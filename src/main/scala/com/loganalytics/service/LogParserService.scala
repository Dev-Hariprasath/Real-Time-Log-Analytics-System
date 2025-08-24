package com.loganalytics.service

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import com.loganalytics.SchemaUtils
import org.apache.spark.sql.functions._

object LogParserService {

  def parse(spark: SparkSession, kafkaDf: DataFrame): DataFrame = {

    import spark.implicits._

    val parsed = kafkaDf
      .selectExpr("CAST(value AS STRING) as json")
      .select(F.from_json(F.col("json"), SchemaUtils.logSchema).as("data"))
      .select("data.*")

    // create typed columns and parse timestamp to event_time
    parsed
      .withColumn("event_time", to_timestamp(col("timestamp"))) // expects ISO-like timestamp; adapt format if needed
      .withColumn("status", col("status").cast("int"))
      .withColumn("latencyMs", col("latencyMs").cast("int"))
  }
}
