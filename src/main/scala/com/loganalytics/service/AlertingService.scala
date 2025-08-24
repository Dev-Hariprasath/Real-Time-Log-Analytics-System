package com.loganalytics.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AlertingService {
  def detectErrors(df: DataFrame): DataFrame = {
    df.filter(col("status") >= 500)
  }
}
