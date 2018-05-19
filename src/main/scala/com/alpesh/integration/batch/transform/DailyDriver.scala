package com.alpesh.integration.batch.transform

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import com.alpesh.integration.batch.common.utils.DataframeUtils

object DailyDriver {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    //Parse argument/s
    if(args.size<3){
      log.warn("Warehouse location, ingested data path, transformation path and data set id ")
      System.exit(1)
    }
    val ingestPath = args(0)
    val transformationPath = args(1)
    val datasetId = args(2)
    val spark =
      SparkSession.builder.appName("Integration Test: Transform")
        .getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)



    spark.read.parquet(ingestPath)
      .transform(DataframeUtils.transform(datasetId))
      .write.parquet(transformationPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
