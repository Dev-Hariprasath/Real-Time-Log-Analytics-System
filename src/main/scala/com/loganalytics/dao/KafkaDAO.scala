package com.loganalytics.dao

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.loganalytics.config.AppConfig

object KafkaDAO {
  /** Read raw JSON string messages from Kafka; returns DataFrame with column `raw` */
  def readFromKafka(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafkaBootstrap)
      .option("subscribe", AppConfig.kafkaSourceTopic)
      .option("startingOffsets", AppConfig.kafkaStartingOffsets)
      .load()
      .selectExpr("CAST(value AS STRING) AS raw")
  }

  /** Write a DataFrame with a string column named `value` to Kafka topic. */
  def writeToKafkaString(df: DataFrame, topic: String) = {
    df.selectExpr("CAST(value AS STRING) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafkaBootstrap)
      .option("topic", topic)
      .option("checkpointLocation", s"${AppConfig.checkpointBasePath}/kafka-$topic")
      .start()
  }
}
