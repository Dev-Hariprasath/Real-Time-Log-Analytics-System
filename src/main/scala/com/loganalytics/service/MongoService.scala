package com.loganalytics.service

import org.mongodb.scala._

import com.loganalytics.config.AppConfig
import scala.concurrent.Await
import scala.concurrent.duration._

object MongoService {
  def loadServiceOwners(): Map[String, String] = {
    val client = MongoClient(AppConfig.mongoUri)
    val db = client.getDatabase(AppConfig.mongoDb)
    val coll = db.getCollection(AppConfig.mongoColl)

    val fut = coll.find().toFuture()
    val docs = Await.result(fut, 10.seconds)

    docs.map { d =>
      val service = d.get("service").map(_.asString().getValue).getOrElse("unknown")
      val owner   = d.get("owner").map(_.asString().getValue).getOrElse("n/a")
      service -> owner
    }.toMap
  }
}
