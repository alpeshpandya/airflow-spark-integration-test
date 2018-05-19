package com.alpesh.integration.batch.ingest

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, LogManager}
import com.alpesh.integration.batch.common.utils.DataframeUtils

object DailyDriver {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    //Parse argument/s
    if(args.size<2){
      log.warn("Input source and output path are required")
      System.exit(1)
    }
    val inputSource = args(0)
    val outputPath = args(1)

    val spark =
      SparkSession.builder.appName("Integration Test: Ingest")
        .getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)


    spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .csv(inputSource)
      .transform(DataframeUtils.formatColumnHeaders)
      .write
      .parquet(outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
