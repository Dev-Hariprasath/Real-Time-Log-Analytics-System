package com.loganalytics.model


case class LogEvent(
                     timestamp: String,
                     level: String,
                     service: String,
                     path: String,
                     status: Int,
                     latencyMs: Int,
                     msg: String,
                     userId: String,
                     host: String,
                     ip: String,
                     requestId: String,
                     sessionId: String
                   )
