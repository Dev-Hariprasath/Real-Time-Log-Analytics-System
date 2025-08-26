package com.loganalytics.dao

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._

object PostgresDAO {

  private def camelToSnake(name: String): String =
    name.replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase

  private def prepareForJdbc(df: DataFrame): DataFrame = {
    val renameCols = df.columns.map(c => col(c).as(camelToSnake(c)))
    df.select(renameCols: _*)
  }

  // ---------- STREAMING WRITERS (Kafka) ----------

  def writeRawLogs(df: DataFrame, source: String = "unknown"): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val count = batchDF.count()
        println(s"[JdbcWriter][$source] Processing batch $batchId for table ${AppConfig.pgRawTable} with $count records")
        if (count > 0) {
          try {
            val prepared = prepareForJdbc(batchDF)
            prepared.write
              .format("jdbc")
              .option("url", AppConfig.pgUrl)
              .option("dbtable", AppConfig.pgRawTable)
              .option("user", AppConfig.pgUser)
              .option("password", AppConfig.pgPass)
              .option("driver", "org.postgresql.Driver")
              .mode(SaveMode.Append)
              .save()
            println(s"[JdbcWriter][$source] ✅ Wrote batch $batchId to ${AppConfig.pgRawTable}")
          } catch {
            case e: Exception =>
              println(s"[JdbcWriter][$source] ❌ Failed to write batch $batchId to ${AppConfig.pgRawTable}: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/raw_$source")
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .outputMode("append")
      .start()
  }

  def writeAggregates(df: DataFrame): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val c = batchDF.count()
        println(s"[JdbcWriter][aggs] Processing aggregates batch $batchId with $c records")
        if (c > 0) {
          try {
            val prepared = prepareForJdbc(batchDF)
            prepared.write
              .format("jdbc")
              .option("url", AppConfig.pgUrl)
              .option("dbtable", AppConfig.pgAggsTable)
              .option("user", AppConfig.pgUser)
              .option("password", AppConfig.pgPass)
              .option("driver", "org.postgresql.Driver")
              .mode(SaveMode.Append)
              .save()
            println(s"[JdbcWriter][aggs] ✅ Wrote batch $batchId to ${AppConfig.pgAggsTable}")
          } catch {
            case e: Exception =>
              println(s"[JdbcWriter][aggs] ❌ Failed aggregates batch $batchId: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/aggs")
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .outputMode("update")
      .start()
  }

  def writeAlerts(df: DataFrame): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val c = batchDF.count()
        println(s"[JdbcWriter][alerts] Processing alerts batch $batchId with $c records")
        if (c > 0) {
          try {
            val prepared = prepareForJdbc(batchDF)
            prepared.write
              .format("jdbc")
              .option("url", AppConfig.pgUrl)
              .option("dbtable", AppConfig.pgAlertsTable)
              .option("user", AppConfig.pgUser)
              .option("password", AppConfig.pgPass)
              .option("driver", "org.postgresql.Driver")
              .mode(SaveMode.Append)
              .save()
            println(s"[JdbcWriter][alerts] ✅ Wrote batch $batchId to ${AppConfig.pgAlertsTable}")
          } catch {
            case e: Exception =>
              println(s"[JdbcWriter][alerts] ❌ Failed alerts batch $batchId: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/alerts")
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .outputMode("append")
      .start()
  }

  // ---------- BATCH WRITERS (Mongo) ----------

  /** Generic batch writer for Mongo-derived DataFrames */
  def writeBatch(df: DataFrame, table: String, source: String): Unit = {
    val count = df.count()
    println(s"[JdbcWriter][$source] Preparing to write $count records to $table")
    if (count > 0) {
      try {
        val prepared = prepareForJdbc(df)
        prepared.write
          .format("jdbc")
          .option("url", AppConfig.pgUrl)
          .option("dbtable", table)
          .option("user", AppConfig.pgUser)
          .option("password", AppConfig.pgPass)
          .option("driver", "org.postgresql.Driver")
          .mode(SaveMode.Append)
          .save()
        println(s"[JdbcWriter][$source] ✅ Successfully wrote $count records to $table")
      } catch {
        case e: Exception =>
          println(s"[JdbcWriter][$source] ❌ Failed to write to $table: ${e.getMessage}")
          e.printStackTrace()
      }
    } else {
      println(s"[JdbcWriter][$source] ℹ️ No records to write")
    }
  }

  // Convenience wrappers for Mongo batch paths
  def writeMongoLogs(df: DataFrame): Unit =
    writeBatch(df, AppConfig.pgRawTable, "mongo-raw")

  def writeMongoAggregates(df: DataFrame): Unit =
    writeBatch(df, AppConfig.pgAggsTable, "mongo-aggs")

  def writeMongoAlerts(df: DataFrame): Unit =
    writeBatch(df, AppConfig.pgAlertsTable, "mongo-alerts")

  // ---------- READERS ----------

  def readAggregates(spark: SparkSession): DataFrame = {
    val df = spark.read
      .format("jdbc")
      .option("url", AppConfig.pgUrl)
      .option("dbtable", AppConfig.pgAggsTable)
      .option("user", AppConfig.pgUser)
      .option("password", AppConfig.pgPass)
      .option("driver", "org.postgresql.Driver")
      .load()

    df.select(
      col("window_start").as("windowStart"),
      col("window_end").as("windowEnd"),
      col("service"),
      col("events").cast("long"),
      col("errors").cast("long"),
      col("latency_ms").cast("double")
    )
  }

  def readAlerts(spark: SparkSession): DataFrame = {
    val df = spark.read
      .format("jdbc")
      .option("url", AppConfig.pgUrl)
      .option("dbtable", AppConfig.pgAlertsTable)
      .option("user", AppConfig.pgUser)
      .option("password", AppConfig.pgPass)
      .option("driver", "org.postgresql.Driver")
      .load()

    df.select(
      col("event_time").cast("timestamp").as("event_time"),
      col("alert_time").cast("timestamp").as("alert_time"),
      col("service"),
      col("status").cast("int").as("status"),
      col("msg"),
      col("request_id").as("request_id"),
      col("host")
    )
  }
}
