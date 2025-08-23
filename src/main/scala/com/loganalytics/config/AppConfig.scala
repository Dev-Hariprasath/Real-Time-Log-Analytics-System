package com.loganalytics.config

import com.typesafe.config.ConfigFactory

object AppConfig {
  private val conf = ConfigFactory.load()
  private val app  = conf.getConfig("app")

  val appName: String = app.getString("name")

  val kafkaBootstrap: String   = app.getString("kafka.bootstrap")
  val kafkaSourceTopic: String = app.getString("kafka.sourceTopic")
  val kafkaStartingOffsets: String = app.getString("kafka.startingOffsets")

  val deltaBasePath: String      = app.getString("delta.basePath")
  val checkpointBasePath: String = app.getString("checkpoint.basePath")

  val windowDuration: String = app.getString("window.duration")
  val windowSlide: String    = app.getString("window.slide")
  val watermark: String      = app.getString("watermark")

  val alertErrorRateThreshold: Double = app.getDouble("alerts.errorRateThreshold")
  val alertP95LatencyThresholdMs: Int = app.getInt("alerts.p95LatencyThresholdMs")
  val alertsToKafka: Boolean          = app.getBoolean("alerts.toKafka")
  val alertsKafkaTopic: String        = app.getString("alerts.kafkaTopic")

  // Postgres
  //val pgUrl: String   = app.getConfig("postgres").getString("url")
  //val pgUser: String  = app.getConfig("postgres").getString("user")
  //val pgPass: String  = app.getConfig("postgres").getString("password")
  //val pgTable: String = app.getConfig("postgres").getString("table")
}

