package com.loganalytics.service

import com.loganalytics.SchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Parses Kafka value (JSON) into columns and preserves `timestamp`. */
object LogParserService {

  def parse(spark: SparkSession, kafkaStream: DataFrame): DataFrame = {
    import spark.implicits._

    // kafkaStream has columns: key, value (binary), topic, partition, offset, timestamp, timestampType
    // We must NOT rely on Kafka's own 'timestamp' (ingest time). We parse our payload's 'timestamp' field.
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
      .select("data.*") // => timestamp, level, service, path, status, latencyMs, msg, userId, host, ip, requestId, sessionId

    parsed
  }
}
