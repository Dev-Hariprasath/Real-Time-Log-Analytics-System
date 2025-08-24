package com.loganalytics.utils

import com.loganalytics.config.AppConfig
import java.sql.{Connection, DriverManager, Statement}

object DBInit {
  def createTables(): Unit = {
    var conn: Connection = null
    var stmt: Statement = null

    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(AppConfig.pgUrl, AppConfig.pgUser, AppConfig.pgPass)
      stmt = conn.createStatement()

      // Raw logs table
      val logsTable =
        s"""
           |CREATE TABLE IF NOT EXISTS ${AppConfig.pgRawTable} (
           |  id SERIAL PRIMARY KEY,
           |  event_time TIMESTAMP,
           |  level VARCHAR(20),
           |  service VARCHAR(100),
           |  path TEXT,
           |  status INT,
           |  latency_ms INT,
           |  msg TEXT,
           |  user_id VARCHAR(100),
           |  host VARCHAR(100),
           |  ip VARCHAR(50),
           |  request_id VARCHAR(100),
           |  session_id VARCHAR(100),
           |  owner VARCHAR(100),
           |  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           |);
           |""".stripMargin

      // Aggregates table
      val aggsTable =
        s"""
           |CREATE TABLE IF NOT EXISTS ${AppConfig.pgAggsTable} (
           |  window_start TIMESTAMP,
           |  window_end TIMESTAMP,
           |  service VARCHAR(100),
           |  events BIGINT,
           |  errors BIGINT,
           |  avg_latency DOUBLE PRECISION,
           |  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
           |  PRIMARY KEY (window_start, window_end, service)
           |);
           |""".stripMargin

      // Alerts table
      val alertsTable =
        s"""
           |CREATE TABLE IF NOT EXISTS ${AppConfig.pgAlertsTable} (
           |  id SERIAL PRIMARY KEY,
           |  event_time TIMESTAMP,
           |  alert_time TIMESTAMP,
           |  service VARCHAR(100),
           |  status INT,
           |  msg TEXT,
           |  request_id VARCHAR(100),
           |  host VARCHAR(100),
           |  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           |);
           |""".stripMargin

      stmt.executeUpdate(logsTable)
      stmt.executeUpdate(aggsTable)
      stmt.executeUpdate(alertsTable)

      println(s"[DBInit] ✅ All tables created successfully")

    } catch {
      case e: Exception =>
        println(s"[DBInit] ❌ Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    createTables()
  }
}