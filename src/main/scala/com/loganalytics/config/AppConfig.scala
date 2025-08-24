package com.loganalytics.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  private val conf: Config = ConfigFactory.load()

  // Kafka
  val kafkaBrokers: String = conf.getString("app.kafka.brokers")
  val kafkaTopic: String   = conf.getString("app.kafka.topic")
  val kafkaOffsets: String = conf.getString("app.kafka.startingOffsets") // earliest/latest
  val kafkaMaxOffsetsPerTrigger: Int = conf.getInt("app.kafka.maxOffsetsPerTrigger")

  // Postgres
  val pgUrl: String    = conf.getString("app.postgres.url")
  val pgUser: String   = conf.getString("app.postgres.user")
  val pgPass: String   = conf.getString("app.postgres.password")
  val pgLogsTable: String = conf.getString("app.postgres.logsTable")
  val pgAggsTable: String = conf.getString("app.postgres.aggsTable")

  // Streaming
  val checkpointDir: String = conf.getString("app.streaming.checkpointDir")
  val trigger: String       = conf.getString("app.streaming.trigger") // e.g. "30 seconds"
  val watermark: String     = conf.getString("app.streaming.watermark")
  val window: String        = conf.getString("app.streaming.window")

  // Mongo (for enrichment)
  val mongoUri: String = conf.getString("app.mongo.uri")
  val mongoDb: String  = conf.getString("app.mongo.db")
  val mongoColl: String = conf.getString("app.mongo.coll")

}
