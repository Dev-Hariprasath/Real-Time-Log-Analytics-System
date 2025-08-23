package com.loganalytics.controller

import org.apache.spark.sql.SparkSession
import com.loganalytics.config.AppConfig
import com.loganalytics.dao.{KafkaDAO, DeltaLakeDAO}
import com.loganalytics.service.{LogParserService, LogProcessingService, AlertingService}

object LogStreamController {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(AppConfig.appName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      // ===== Delta Lake configuration =====
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // ===================================
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val raw     = KafkaDAO.readFromKafka(spark)
    val parsed  = LogParserService.parse(spark, raw)
    val metrics = LogProcessingService.metricsPerWindow(parsed)
    metrics.writeStream
      .format("console")
      .outputMode("update")       // "update" shows data even before watermark expires
      .option("truncate", false)
      .start()
    val alerts  = AlertingService.buildAlertsFrom(metrics)
    val errs    = LogProcessingService.errorsAndWarnings(parsed)

    DeltaLakeDAO.writeAppend(metrics, "metrics_minute", "metrics")
    DeltaLakeDAO.writeAppend(errs, "error_logs", "errors")


    println("✅ Streams started: metrics, errors, alerts. Press Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()
  }
}
