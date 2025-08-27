package com.loganalytics.controller

import org.apache.spark.sql.SparkSession

object LogStreamController {

  def start(spark: SparkSession, latencyThresholdMs: Int = 2000): Unit = {
    println("[Controller] Starting all pipelines…")

    // Kafka (streaming)
    KafkaStreamController.start(spark, latencyThresholdMs)

    // Mongo (batch)
    MongoBatchController.start(spark, latencyThresholdMs)

    println("[Controller] Pipelines initialized")
  }
}
