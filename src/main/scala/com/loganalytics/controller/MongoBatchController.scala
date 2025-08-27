package com.loganalytics.controller

import com.loganalytics.SchemaUtils
import com.loganalytics.dao.{MongoDAO, PostgresDAO}
import com.loganalytics.service.{MongoLogService, RawLogService, AlertingService}
import org.apache.spark.sql.{Row, SparkSession}

object MongoBatchController {

  private def hasRows(df: org.apache.spark.sql.DataFrame): Boolean =
    try df.take(1).nonEmpty catch { case _: Throwable => false }

  def start(spark: SparkSession, latencyThresholdMs: Int): Unit = {
    println("[MongoController] Loading batch data from Mongo…")

    val mongoBatch = try {
      MongoDAO.loadLogs(spark)
    } catch {
      case e: Throwable =>
        println(s"[MongoController] Failed to load: ${e.getMessage}")
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SchemaUtils.logSchema)
    }

    if (!hasRows(mongoBatch)) {
      println("[MongoController] ℹ️ No rows available in MongoDB")
      return
    }

    val normalized = MongoLogService.prepare(mongoBatch)
    val rawMongo   = RawLogService.prepare(normalized).cache()

    println(s"[MongoController] Loaded ${rawMongo.count()} rows; writing raw+aggs+alerts to Postgres…")

    // Raw logs → Postgres
    PostgresDAO.writeMongoLogs(rawMongo)

    // Aggregates → Postgres
    val mongoAggs = MongoDAO.buildAggregates(rawMongo, withWatermark = false, spark)
    PostgresDAO.writeMongoAggregates(mongoAggs)

    // Alerts → Postgres
    val mongoAlerts = AlertingService.detectAlerts(rawMongo, latencyThresholdMs)
    PostgresDAO.writeMongoAlerts(mongoAlerts)

    rawMongo.unpersist()
    println("[MongoController] Batch load complete")
  }
}
