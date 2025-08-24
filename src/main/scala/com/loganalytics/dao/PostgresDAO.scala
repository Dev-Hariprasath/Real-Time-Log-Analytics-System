package com.loganalytics.dao

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object PostgresDAO {

  private def writeBatchToPostgres(batch: DataFrame, table: String): Unit = {
    batch.write
      .format("jdbc")
      .option("url", AppConfig.pgUrl)
      .option("dbtable", table)
      .option("user", AppConfig.pgUser)
      .option("password", AppConfig.pgPass)
      .mode(SaveMode.Append)
      .save()
  }

  def writeRawLogs(df: DataFrame): StreamingQuery = {
    df.writeStream
      .outputMode("append")
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        println(s"[PostgresDAO] Writing raw logs, batchId=$batchId, rows=${batch.count()}")
        writeBatchToPostgres(batch, AppConfig.pgLogsTable)
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/raw")
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .start()
  }

  def writeAggregates(df: DataFrame): StreamingQuery = {
    df.writeStream
      .outputMode("update")
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        println(s"[PostgresDAO] Writing aggregates, batchId=$batchId, rows=${batch.count()}")
        writeBatchToPostgres(batch, AppConfig.pgAggsTable)
      }
      .option("checkpointLocation", s"${AppConfig.checkpointDir}/aggs")
      .trigger(Trigger.ProcessingTime(AppConfig.trigger))
      .start()
  }
}
