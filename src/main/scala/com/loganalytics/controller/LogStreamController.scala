package com.loganalytics.controller

import com.loganalytics.SchemaUtils
import com.loganalytics.config.AppConfig
import com.loganalytics.dao.{KafkaDAO, MongoDAO, PostgresDAO}
import com.loganalytics.service.{LogParserService, MongoLogService, RawLogService}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object LogStreamController {

  private def hasRows(df: org.apache.spark.sql.DataFrame): Boolean =
    try df.take(1).nonEmpty catch { case _: Throwable => false }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealTimeLogAnalytics")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", s"${AppConfig.checkpointDir}/main")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // Tunables (consider moving to AppConfig)
    val watermarkDur        = "5 minutes"
    val windowDur           = "1 minute"
    val slideDur            = "1 minute"
    val latencyThresholdMs  = 2000 // alerts when latency_ms > this

    // ---------- Helper builders ----------

    def buildAggregates(df: DataFrame, withWatermark: Boolean): DataFrame = {
      val base = if (withWatermark) df.withWatermark("event_time", watermarkDur) else df
      base
        .groupBy(
          window(col("event_time"), windowDur, slideDur),
          col("service")
        )
        .agg(
          count(lit(1)).as("events"),
          sum(when(col("status") >= 500, 1).otherwise(0)).cast("long").as("errors"),
          avg(col("latency_ms")).cast("double").as("latency_ms")
        )
        .select(
          col("window.start").as("windowStart"),
          col("window.end").as("windowEnd"),
          col("service"),
          col("events").cast("long").as("events"),
          col("errors").cast("long").as("errors"),
          col("latency_ms").cast("double").as("latency_ms")
        )
    }

    def buildAlerts(df: DataFrame): DataFrame = {
      df
        .filter(col("status") >= 500 || col("latency_ms") > lit(latencyThresholdMs))
        .withColumn("alert_time", current_timestamp())
        .select(
          col("event_time"),
          col("alert_time"),
          col("service"),
          col("status").cast("int").as("status"),
          col("msg"),
          col("request_id"),
          col("host")
        )
    }

    try {
      // ---------- Kafka (streaming) ----------
      val kafkaStream = KafkaDAO.readLogs(spark)
      val parsedKafka = LogParserService.parse(spark, kafkaStream)        // your parser
      val rawKafka    = RawLogService.prepare(parsedKafka)                // canonical schema

      // Kafka: raw → Postgres
      val qRawKafka   = PostgresDAO.writeRawLogs(rawKafka, source = "kafka")

      // Kafka: aggregates & alerts (streaming)
      val kafkaAggs   = buildAggregates(rawKafka, withWatermark = true)
      val kafkaAlerts = buildAlerts(rawKafka)

      val qAggsKafka  = PostgresDAO.writeAggregates(kafkaAggs)
      val qAlertsKafka= PostgresDAO.writeAlerts(kafkaAlerts)

      // ---------- Mongo (batch, optional) ----------
      val mongoBatch = try {
        MongoDAO.loadLogs(spark)
      } catch {
        case e: Throwable =>
          println(s"[Mongo] ❌ Failed to load: ${e.getMessage}")
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SchemaUtils.logSchema)
      }

      if (hasRows(mongoBatch)) {
        // Normalize and compute analytics
        val normalizedMongo = MongoLogService.prepare(mongoBatch) // ensure event_time exists
        val rawMongo        = RawLogService.prepare(normalizedMongo).cache()

        println(s"[Mongo] ✅ Loaded ${rawMongo.count()} rows; writing raw+aggs+alerts to Postgres…")

        // Mongo: raw → Postgres (batch)
        PostgresDAO.writeMongoLogs(rawMongo)

        // Mongo: aggregates & alerts (batch)
        val mongoAggs   = buildAggregates(rawMongo, withWatermark = false)
        val mongoAlerts = buildAlerts(rawMongo)

        PostgresDAO.writeMongoAggregates(mongoAggs)
        PostgresDAO.writeMongoAlerts(mongoAlerts)

        rawMongo.unpersist()
      } else {
        println("[Mongo] ℹ️ No rows available")
      }

      // Keep streaming queries alive
      spark.streams.awaitAnyTermination()

    } catch {
      case e: Exception =>
        println(s"❌ Application failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
