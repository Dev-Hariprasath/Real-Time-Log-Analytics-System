package com.loganalytics.service

import com.loganalytics.dao.MongoDAO
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object EnrichmentService {

  def enrich(spark: SparkSession, df: DataFrame, broadcastedMetadata: Option[Broadcast[Map[String, String]]] = None): DataFrame = {

    // load metadata from provided broadcast or Mongo; swallow errors and proceed without enrichment
    val metadata = try {
      broadcastedMetadata.map(_.value).getOrElse(MongoDAO.loadServiceMetadata())
    } catch {
      case e: Throwable =>
        println(s"[EnrichmentService] Mongo unavailable, skipping enrichment. Reason: ${e.getMessage}")
        Map.empty[String, String]
    }

    if (metadata.isEmpty) {
      println("[EnrichmentService] No metadata found in Mongo. Proceeding without enrichment.")
      df
    } else {
      val bc = spark.sparkContext.broadcast(metadata)
      val enrichUDF = udf((service: String) => bc.value.getOrElse(service, "Unknown"))
      // Keep all existing columns (including event_time) and add owner
      df.withColumn("owner", enrichUDF(col("service")))
    }
  }
}
