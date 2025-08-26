package com.loganalytics.controller

import com.loganalytics.SchemaUtils
import com.loganalytics.config.AppConfig
import com.loganalytics.dao.{KafkaDAO, MongoDAO, PostgresDAO}
import com.loganalytics.service.{LogParserService, MongoLogService, RawLogService}
import org.apache.spark.sql.{Row, SparkSession}

object LogStreamController {

  private def hasRows(df: org.apache.spark.sql.DataFrame): Boolean =
    try df.take(1).nonEmpty catch { case _: Throwable => false }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealTimeLogAnalytics")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", s"${AppConfig.checkpointDir}/main")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // --- Kafka (streaming)
      val kafkaStream = KafkaDAO.readLogs(spark)
      val parsedKafka = LogParserService.parse(spark, kafkaStream)
      val rawKafkaLogs = RawLogService.prepare(parsedKafka)

      // --- Mongo (batch, optional)
      val mongoBatch = try {
        MongoDAO.loadLogs(spark)
      } catch {
        case e: Throwable =>
          println(s"[Mongo] ❌ Failed to load: ${e.getMessage}")
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SchemaUtils.logSchema)
      }

      if (hasRows(mongoBatch)) {
        val rawMongoLogs = RawLogService.prepare(MongoLogService.prepare(mongoBatch))
        println(s"[Mongo] ✅ Writing ${rawMongoLogs.count()} rows to Postgres (batch, source=mongo)")
        rawMongoLogs
          .write
          .format("jdbc")
          .option("url", AppConfig.pgUrl)
          .option("dbtable", AppConfig.pgRawTable)
          .option("user", AppConfig.pgUser)
          .option("password", AppConfig.pgPass)
          .option("driver", "org.postgresql.Driver")
          .mode("append")
          .save()
        println(s"[JdbcWriter][mongo] ✅ Wrote ${rawMongoLogs.count()} rows to ${AppConfig.pgRawTable}")
      } else {
        println("[Mongo] ℹ️ No rows available")
      }

      // --- Kafka continues streaming
      PostgresDAO.writeRawLogs(rawKafkaLogs, source = "kafka")

      spark.streams.awaitAnyTermination()

    } catch {
      case e: Exception =>
        println(s"❌ Application failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
