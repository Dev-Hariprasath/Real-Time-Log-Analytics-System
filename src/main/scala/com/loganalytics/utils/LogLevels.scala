package com.loganalytics.utils

object LogLevels {
  val INFO  = "INFO"
  val WARN  = "WARN"
  val ERROR = "ERROR"
  val DEBUG = "DEBUG"
  val ALL   = Set(INFO, WARN, ERROR, DEBUG)
}
