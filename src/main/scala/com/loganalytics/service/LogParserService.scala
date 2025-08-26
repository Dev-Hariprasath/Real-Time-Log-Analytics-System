package com.loganalytics.service

import com.loganalytics.SchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LogParserService {

  def parse(spark: SparkSession, kafkaStream: DataFrame): DataFrame = {
    val parsed = kafkaStream
      .select(col("value").cast("string").as("json"))
      .withColumn(
        "data",
        from_json(
          col("json"),
          SchemaUtils.logSchema,
          Map("timezone" -> spark.sessionState.conf.sessionLocalTimeZone) // keep TZ consistent
        )
      )
      .select("data.*")

    parsed
  }
}
