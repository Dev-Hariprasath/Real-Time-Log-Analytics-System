package com.loganalytics.service

import com.loganalytics.utils.SchemaUtils
import com.loganalytics.utils.LogLevels
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LogParserService {
  def parse(spark: SparkSession, raw: DataFrame): DataFrame = {
    import spark.implicits._

    val parsed = raw
      .select(from_json($"raw", SchemaUtils.logSchema).as("d"))
      .select("d.*")
      .withColumn("level", upper(coalesce(col("level"), lit(LogLevels.INFO))))
      //robust timestamp parsing
      .withColumn(
        "ts",
        coalesce(
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
          to_timestamp(col("timestamp"))
        )
      )
      .filter(col("ts").isNotNull)


      .withColumn("service", coalesce(col("service"), lit("unknown")))
      .withColumn("latencyMs", coalesce(col("latencyMs"), lit(0)))
      .filter(col("ts").isNotNull)

    parsed.withColumn("date", to_date(col("ts"))).withColumn("hour", hour(col("ts")))
  }
}
