package com.loganalytics.dao

import com.loganalytics.config.AppConfig
import org.mongodb.scala._
import scala.concurrent.Await
import scala.concurrent.duration._

object MongoDAO {
  // Create Mongo client using values from AppConfig (non-blocking for initialization)
  private val client: MongoClient = MongoClient(AppConfig.mongoUri)
  private val database: MongoDatabase = client.getDatabase(AppConfig.mongoDb)
  private val collection: MongoCollection[Document] = database.getCollection(AppConfig.mongoColl)

  def loadServiceMetadata(): Map[String, String] = {
    val docsFuture = collection.find().toFuture()
    val docs = Await.result(docsFuture, 10.seconds)

    docs.flatMap { doc =>
      val service = Option(doc.getString("service"))
      val owner   = Option(doc.getString("owner"))
      service.map(svc => svc -> owner.getOrElse("Unknown"))
    }.toMap
  }
}
