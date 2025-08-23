package com.loganalytics.generator

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random
import java.time.Instant
import scala.util.control.NonFatal

object LogGenerator {
  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "log-topic")

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")

    val producer = new KafkaProducer[String, String](props)
    val services = Array("web","orders","payments","auth","users")
    val r = new Random()

    try {
      while(true) {
        val ts = Instant.now().toString
        val lvl = {
          val x = r.nextInt(100)
          if (x < 70) "INFO" else if (x < 90) "WARN" else "ERROR"
        }
        val svc = services(r.nextInt(services.length))
        val lat = r.nextInt(1500) + 5
        val status = if (lvl == "ERROR") Seq(500,503,504).apply(r.nextInt(3)) else Seq(200,201,204,404).apply(r.nextInt(4))
        val msg = if (lvl == "ERROR") "simulated failure" else "ok"
        val user = s"u${r.nextInt(200)}"
        val host = s"${svc}-host-${r.nextInt(3)+1}"
        val ip = s"10.0.0.${r.nextInt(254)+1}"
        val reqId = java.util.UUID.randomUUID().toString
        val sessionId = s"sess-${r.nextInt(10000)}"

        val json = s"""{"timestamp":"$ts","level":"$lvl","service":"$svc","path":"/api/v1/test","status":$status,"latencyMs":$lat,"msg":"$msg","userId":"$user","host":"$host","ip":"$ip","requestId":"$reqId","sessionId":"$sessionId"}"""

        val rec = new ProducerRecord[String,String](topic, null, json)
        producer.send(rec)
        Thread.sleep(300) // ~3 messages/sec
      }
    } catch {
      case NonFatal(e) =>
        println(s"Generator stopped: ${e.getMessage}")
    } finally {
      producer.close()
    }
  }
}

