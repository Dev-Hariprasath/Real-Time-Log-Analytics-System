package com.loganalytics.controller

import com.loganalytics.config.AppConfig
import org.apache.spark.sql.SparkSession

object MainApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealTimeLogAnalytics")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", s"${AppConfig.checkpointDir}/main")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      LogStreamController.start(spark)
      spark.streams.awaitAnyTermination()
    } catch {
      case e: Exception =>
        println(s"[MainApp] Application failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      println("[MainApp] Shutting down Spark")
      spark.stop()
    }
  }
}
