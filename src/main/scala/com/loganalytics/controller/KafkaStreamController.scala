package com.loganalytics.controller

import com.loganalytics.dao.{KafkaDAO, PostgresDAO}
import com.loganalytics.service.{LogParserService, RawLogService, AlertingService, AggregationService}
import org.apache.spark.sql.SparkSession

object KafkaStreamController {

  def start(spark: SparkSession, latencyThresholdMs: Int): Unit = {
    println("[KafkaController] Starting Kafka streaming pipeline…")

    // 1) Read Kafka
    val kafkaStream = KafkaDAO.readLogs(spark)

    // 2) Parse + normalize
    val parsed   = LogParserService.parse(spark, kafkaStream)
    val rawKafka = RawLogService.prepare(parsed)

    // 3) Write raw logs → Postgres
    PostgresDAO.writeRawLogs(rawKafka, source = "kafka")

    // 4) Build aggregates from Kafka logs
    val kafkaAggs = AggregationService.buildAggregates(rawKafka, withWatermark = true, spark)
    PostgresDAO.writeAggregates(kafkaAggs)

    // 5) Detect alerts → Postgres
    val kafkaAlerts = AlertingService.detectAlerts(rawKafka, latencyThresholdMs)
    PostgresDAO.writeAlerts(kafkaAlerts)

    println("[KafkaController] Streaming queries started")
  }
}
