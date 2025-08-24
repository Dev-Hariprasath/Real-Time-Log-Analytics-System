package com.loganalytics.dao

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object PostgresDAO {

  private def camelToSnake(name: String): String =
    name.replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase

  private def prepareForJdbc(df: DataFrame): DataFrame = {
    val renameCols = df.columns.map(c => col(c).as(camelToSnake(c)))
    df.select(renameCols: _*)
  }

  def writeRawLogs(df: DataFrame): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"Processing raw logs batch $batchId with ${batchDF.count()} records")

        if (!batchDF.isEmpty) {
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
            println(s"Successfully wrote batch $batchId to raw_logs table")
          } catch {
            case e: Exception =>
              println(s"Failed to write batch $batchId: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/raw")
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
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
              .mode(SaveMode.Overwrite)
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
}