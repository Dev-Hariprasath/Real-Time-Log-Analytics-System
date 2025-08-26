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

  def writeRawLogs(df: DataFrame, source: String = "unknown"): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val count = batchDF.count()
        println(s"[JdbcWriter][$source] Processing batch $batchId for table ${AppConfig.pgRawTable} with $count records")

        if (count > 0) {
          try {
            val prepared = prepareForJdbc(batchDF) // ✅ no .withColumn("source")
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
        println(s"Processing aggregates batch $batchId with ${batchDF.count()} records")

        if (!batchDF.isEmpty) {
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
            println(s"Successfully wrote batch $batchId to aggregated_logs table")
          } catch {
            case e: Exception =>
              println(s"Failed to write aggregates batch $batchId: ${e.getMessage}")
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
        println(s"Processing alerts batch $batchId with ${batchDF.count()} records")

        if (!batchDF.isEmpty) {
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
            println(s"Successfully wrote batch $batchId to alerts table")
          } catch {
            case e: Exception =>
              println(s"Failed to write alerts batch $batchId: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/alerts")
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .start()
  }

  def readAggregates(spark: SparkSession): DataFrame = {
    val df = spark.read
      .format("jdbc")
      .option("url", AppConfig.pgUrl)
      .option("dbtable", AppConfig.pgAggsTable)
      .option("user", AppConfig.pgUser)
      .option("password", AppConfig.pgPass)
      .option("driver", "org.postgresql.Driver")
      .load()

    // Convert snake_case from Postgres back to the streaming camelCase schema
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