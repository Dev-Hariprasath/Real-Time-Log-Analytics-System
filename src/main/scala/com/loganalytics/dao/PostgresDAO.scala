package com.loganalytics.dao

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._

object PostgresDAO {

  /** Convert camelCase → snake_case for JDBC table columns */
  private def camelToSnake(name: String): String =
    name.replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase

  /** Normalize column names for Postgres */
  private def prepareForJdbc(df: DataFrame): DataFrame = {
    val renamedCols = df.columns.map(c => col(c).as(camelToSnake(c)))
    df.select(renamedCols: _*)
  }

  // =====================================================
  // =============== STREAMING WRITERS ===================
  // =====================================================

  /** Write Kafka → Postgres (raw logs) */
  def writeRawLogs(df: DataFrame, source: String = "kafka"): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val count = batchDF.count()
        println(s"[JdbcWriter][$source] Processing RAW batch $batchId with $count records → ${AppConfig.pgRawTable}")

        if (count > 0) {
          try {
            prepareForJdbc(batchDF)
              .write
              .format("jdbc")
              .option("url", AppConfig.pgUrl)
              .option("dbtable", AppConfig.pgRawTable)
              .option("user", AppConfig.pgUser)
              .option("password", AppConfig.pgPass)
              .option("driver", "org.postgresql.Driver")
              .mode(SaveMode.Append)
              .save()

            println(s"[JdbcWriter][$source] ✅ Wrote RAW batch $batchId to ${AppConfig.pgRawTable}")
          } catch {
            case e: Exception =>
              println(s"[JdbcWriter][$source] ❌ Failed RAW batch $batchId: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/raw_$source")
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .outputMode("append")
      .start()
  }

  /** Write Kafka aggregates → Postgres */
  def writeAggregates(df: DataFrame, source: String = "kafka"): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val c = batchDF.count()
        println(s"[JdbcWriter][$source] Processing AGGREGATES batch $batchId with $c records → ${AppConfig.pgAggsTable}")

        if (c > 0) {
          try {
            prepareForJdbc(batchDF)
              .write
              .format("jdbc")
              .option("url", AppConfig.pgUrl)
              .option("dbtable", AppConfig.pgAggsTable)
              .option("user", AppConfig.pgUser)
              .option("password", AppConfig.pgPass)
              .option("driver", "org.postgresql.Driver")
              .mode(SaveMode.Append)
              .save()

            println(s"[JdbcWriter][$source] ✅ Wrote AGGREGATES batch $batchId to ${AppConfig.pgAggsTable}")
          } catch {
            case e: Exception =>
              println(s"[JdbcWriter][$source] ❌ Failed AGGREGATES batch $batchId: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/aggs_$source") // unique per source
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .outputMode("append") // ✅ FIXED
      .start()
  }

  /** Write Kafka alerts → Postgres */
  def writeAlerts(df: DataFrame, source: String = "kafka"): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val c = batchDF.count()
        println(s"[JdbcWriter][$source] Processing ALERTS batch $batchId with $c records → ${AppConfig.pgAlertsTable}")

        if (c > 0) {
          try {
            prepareForJdbc(batchDF)
              .write
              .format("jdbc")
              .option("url", AppConfig.pgUrl)
              .option("dbtable", AppConfig.pgAlertsTable)
              .option("user", AppConfig.pgUser)
              .option("password", AppConfig.pgPass)
              .option("driver", "org.postgresql.Driver")
              .mode(SaveMode.Append)
              .save()

            println(s"[JdbcWriter][$source] ✅ Wrote ALERTS batch $batchId to ${AppConfig.pgAlertsTable}")
          } catch {
            case e: Exception =>
              println(s"[JdbcWriter][$source] ❌ Failed ALERTS batch $batchId: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/alerts_$source") // unique per source
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .outputMode("append")
      .start()
  }

  // =====================================================
  // =============== BATCH WRITERS (Mongo) ===============
  // =====================================================

  /** Generic batch writer (Mongo → Postgres) */
  private def writeBatch(df: DataFrame, table: String, source: String): Unit = {
    val count = df.count()
    println(s"[JdbcWriter][$source] Preparing to write $count records → $table")

    if (count > 0) {
      try {
        prepareForJdbc(df)
          .write
          .format("jdbc")
          .option("url", AppConfig.pgUrl)
          .option("dbtable", table)
          .option("user", AppConfig.pgUser)
          .option("password", AppConfig.pgPass)
          .option("driver", "org.postgresql.Driver")
          .mode(SaveMode.Append)
          .save()

        println(s"[JdbcWriter][$source] ✅ Successfully wrote $count records → $table")
      } catch {
        case e: Exception =>
          println(s"[JdbcWriter][$source] ❌ Failed to write batch to $table: ${e.getMessage}")
          e.printStackTrace()
      }
    } else {
      println(s"[JdbcWriter][$source] ℹ️ No records to write → $table")
    }
  }

  // === Convenience wrappers for Mongo batch paths ===
  def writeMongoLogs(df: DataFrame): Unit =
    writeBatch(df, AppConfig.pgRawTable, "mongo-raw")

  def writeMongoAggregates(df: DataFrame): Unit =
    writeBatch(df, AppConfig.pgAggsTable, "mongo-aggs")

  def writeMongoAlerts(df: DataFrame): Unit =
    writeBatch(df, AppConfig.pgAlertsTable, "mongo-alerts")
}
