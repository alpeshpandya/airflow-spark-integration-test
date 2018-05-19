package com.alpesh.integration.batch.app

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

/*
This app generates count of Uber rides in NYC by Humidity Range
Input data frames: uber rides & weather
Dependency: Upstream ingest and tranform processes
 */

object UberRidesByHumidityRange {
  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    //Parse argument/s
    if(args.size<3){
      log.warn("Warehouse location, data paths and output path are required")
      System.exit(1)
    }
    val uberDataPath = args(0)
    val weatherDataPath = args(1)
    val outputPath = args(2)

    val spark =
      SparkSession.builder.appName("Uber Rides By Humidity: Data App")
        .getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val uberData = spark.read.parquet(uberDataPath)
    val weatherData = spark.read.parquet(weatherDataPath)

    uberData
      .join(weatherData, uberData("DATE") <=> weatherData("date"))
      .groupBy("humidity_range")
      .count()
      .repartition(1)
      .write
      .option("header", "true")
      .csv(outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
