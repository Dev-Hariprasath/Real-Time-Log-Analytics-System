package com.loganalytics.dao

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import com.loganalytics.config.AppConfig

object DeltaLakeDAO {
  /** Write a streaming DataFrame to Delta with append mode using a checkpoint. */
  def writeAppend(df: DataFrame, subPath: String, chkName: String): StreamingQuery = {
    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"${AppConfig.checkpointBasePath}/$chkName")
      .option("path", s"${AppConfig.deltaBasePath}/$subPath")
      .start()
  }
}

