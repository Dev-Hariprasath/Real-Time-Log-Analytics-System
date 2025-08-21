//package com.loganalytics.dao

//import org.apache.spark.sql.DataFrame
//import com.loganalytics.config.AppConfig

//object PostgresDAO {
  /** Simple batch write to Postgres. Intended to be used from foreachBatch. */
  //def writeBatchToPostgres(batchDf: DataFrame): Unit = {
    //batchDf.write
      //.format("jdbc")
      //.option("url", AppConfig.pgUrl)
      //.option("dbtable", AppConfig.pgTable)
      //.option("user", AppConfig.pgUser)
      //.option("password", AppConfig.pgPass)
      //.mode("append")
      //.save()
  //}
//}
