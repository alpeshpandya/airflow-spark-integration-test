package com.alpesh.integration.batch.common.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, to_date, udf}

object DataframeUtils {
  def formatColumnHeaders(dataFrame: DataFrame): DataFrame = {
    var retDf = dataFrame
    for (column <- retDf.columns) {
      retDf = retDf.withColumnRenamed(column, column.replaceAll("\\s", "_"))
    }
    retDf.printSchema()
    retDf
  }

  def humidityToRangeUdf = udf((humidity: Int) => ((humidity + 5) / 10) * 10)

  //Dataframe transformation functions
  def uberTransformation(dataFrame: DataFrame) = {
    dataFrame
      .withColumn("DATE", to_date(col("DATE"), "MM/dd/yyyy"))
      .withColumn("dayofweek", date_format(col("DATE"), "EEEE"))
  }
  def weatherTransformation(dataFrame: DataFrame) = {
    dataFrame
      .withColumn("date", to_date(col("date"), "yyyyMMdd"))
      .withColumn("dayofweek", date_format(col("date"), "EEEE"))
      .withColumn("humidity_range", humidityToRangeUdf(col("hum_avg")))
  }

  //Mapping of transformation functions to date set ids
  val transformationMap = Map[String, (DataFrame) => DataFrame](
    "uberdata" -> uberTransformation,
    "weatherdata" -> weatherTransformation
  )

  def transform(datasetId: String)(dataFrame: DataFrame): DataFrame = {
    dataFrame.printSchema()
    transformationMap.get(datasetId).get(dataFrame)
  }
}
