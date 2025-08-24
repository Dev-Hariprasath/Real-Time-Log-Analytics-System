package com.loganalytics.dao

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaDAO {
  def readLogs(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafkaBrokers)
      .option("subscribe", AppConfig.kafkaTopic)
      .option("startingOffsets", AppConfig.kafkaOffsets)
      .option("maxOffsetsPerTrigger", AppConfig.kafkaMaxOffsetsPerTrigger)
      .load()
  }
}
