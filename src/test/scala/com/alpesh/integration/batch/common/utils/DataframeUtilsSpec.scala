package com.alpesh.integration.batch.common.utils

import org.scalatest._
import org.apache.spark.sql.SparkSession

class DataframeUtilsSpec extends FeatureSpec with GivenWhenThen {
  info("As a user of Dataframe Utilities")
  info("I want to be able to use utilities to tranform Dataframes")
  info("So it can be used for further processing or persitence")

  lazy val spark =
    SparkSession.builder
      .appName("Credential Test App")
      .master("local")
      .getOrCreate()

  feature("Format column headers in dataframe") {
    scenario("Dataframe is passed to utility") {

      Given("Data frame that contains column headers with white spaces")

      import spark.implicits._
      val testDF = Seq(
        ("20180301", 9089),
        ("20180302", 6787),
        ("20180302", 10987)
      ).toDF("financial day", "total revenue")
      val expectedColumns = Array("financial_day", "total_revenue")

      When("Dataframe utils is used")
      val resultDF = DataframeUtils.formatColumnHeaders(testDF)

      Then("Column headers are cleaned of white spaces")
      assert(expectedColumns.deep == resultDF.columns.deep)
    }
  }

  feature("Apply transformations to data frame") {
    scenario("Uber rides sample data frame") {

      Given("Sample data from uber rides open data")
      val spark =
        SparkSession.builder
          .appName("Credential Test App")
          .master("local")
          .getOrCreate()
      import spark.implicits._
      val testDF = Seq(
        ("7/1/2017", "12:00:00 AM", " 874 E 139th St Mott Haven, BX"),
        ("7/1/2017", "12:01:00 AM", " 628 E 141st St Mott Haven, BX"),
        ("7/1/2017", "12:01:00 AM", " 601 E 156th St South Bronx, BX")
      ).toDF("DATE", "TIME", "PICK_UP_ADDRESS")

      When("Transformations are applied")
      val resultDF = testDF.transform(DataframeUtils.transform("uberdata"))

      Then(
        "DATE column is converted to date type and new dayofweek column is added")
      assert(resultDF.schema.fields(0).dataType.typeName.equals("date"))
      assert(resultDF.schema.fields(3).name.equals("dayofweek"))
      assert(resultDF.schema.fields(3).dataType.typeName.equals("string"))
    }
  }
}
