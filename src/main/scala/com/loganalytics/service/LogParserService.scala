package com.loganalytics.service

import com.loganalytics.SchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LogParserService {
  /** Parse Kafka JSON payload into structured DataFrame */
  def parse(spark: SparkSession, kafkaStream: DataFrame): DataFrame = {
    kafkaStream
      .select(col("value").cast("string").as("json"))
      .withColumn(
        "data",
        from_json(
          col("json"),
          SchemaUtils.logSchema,
          Map("timezone" -> spark.sessionState.conf.sessionLocalTimeZone)
        )
      )
      .select("data.*")
  }
}
