package com.aws.weather.mogreps.main

import org.apache.spark.sql.SparkSession
import com.aws.weather.mogreps.etl.ServingLayer

object App extends Serializable  {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Weather Mogreps ETL")
      .enableHiveSupport()
      .getOrCreate()

    ServingLayer.transformAndStore(spark)

    spark.stop()
  }
}
