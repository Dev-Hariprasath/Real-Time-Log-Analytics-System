package com.loganalytics.controller

import com.loganalytics.dao.{KafkaDAO, PostgresDAO}
import com.loganalytics.service.{EnrichmentService, LogParserService, LogProcessingService}
import org.apache.spark.sql.SparkSession

object LogStreamController {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LogMonitoringDebug")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val rawKafka = KafkaDAO.readLogs(spark)
    println(rawKafka)
    val parsedLogs = LogParserService.parse(spark, rawKafka)
    println(parsedLogs)
    val enrichedLogs = EnrichmentService.enrich(spark, parsedLogs)
    println(enrichedLogs)
    PostgresDAO.writeRawLogs(enrichedLogs)
    PostgresDAO.writeAggregates(LogProcessingService.aggregate(enrichedLogs))

    spark.streams.awaitAnyTermination()
  }
}
