package com.loganalytics.controller

import com.loganalytics.dao.{KafkaDAO, PostgresDAO}
import com.loganalytics.service.{AlertingService, EnrichmentService, LogParserService, LogProcessingService, RawLogService}
import com.loganalytics.utils.DBInit
import org.apache.spark.sql.SparkSession

object LogStreamController {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealTimeLogAnalytics")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      println("Initializing database tables...")
      DBInit.createTables()

      println("Starting streaming pipeline...")

      // Read from Kafka
      val rawKafka = KafkaDAO.readLogs(spark)
      println("Connected to Kafka successfully")

      // Parse logs
      val parsed = LogParserService.parse(spark, rawKafka)

      val enriched = EnrichmentService.enrich(spark, parsed)

      // Start raw logs stream
      val rawPrepared = RawLogService.prepare(enriched)
      val rawQuery = PostgresDAO.writeRawLogs(rawPrepared)
      println("Started raw logs stream")

      // Start aggregation stream
      val aggs = LogProcessingService.aggregate(enriched)
      val aggsQuery = PostgresDAO.writeAggregates(aggs)
      println("Started aggregation stream")

      // Start alerts stream
      val alerts = AlertingService.detectErrors(enriched)
      val alertsQuery = PostgresDAO.writeAlerts(alerts)
      println("Started alerts stream")

      println("All streaming queries started. Waiting for data...")

      // Wait for termination
      spark.streams.awaitAnyTermination()

    } catch {
      case e: Exception =>
        println(s"Application failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
