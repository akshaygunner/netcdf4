package com.aws.weather.mogreps.etl

import org.apache.spark.sql.SparkSession
import org.dia.core.SciSparkContext
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ServingLayer extends Serializable {

  @throws(classOf[RuntimeException])
  def transformAndStore(spark: SparkSession) : Unit = {
    val sc = spark.sparkContext
    val ssc = new SciSparkContext(sc)
    @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

    val lstVariables = List("convective_cloud_area_fraction", "latitude", "longitude", "forecast_period","forecast_reference_time", "time")
    val scientificRDD = ssc.netcdfDFSFiles("s3://weather.uk/data/24dcebed708609f13f82312d02c669ad58d2a76f.nc", lstVariables)
    val sciTen = scientificRDD.collect.take(1)(0)
    val sciTenVar = sciTen.variables
    val sciTenVarAbTen_out = sciTenVar("convective_cloud_area_fraction").data

    val sciTenVarAbTen_lat = sciTenVar("latitude").data
    val sciTenVarAbTen_lon = sciTenVar("longitude").data
    val sciTenVarAbTen_fp = sciTenVar("forecast_period").data
    val sciTenVarAbTen_frt = sciTenVar("forecast_reference_time").data
    val sciTenVarAbTen_time = sciTenVar("time").data

    log.info("SciSpark functions executed")

    import spark.implicits._
    val windowSpec = Window.orderBy("index")

    def addTime(seconds: Double): java.lang.String = new DateTime("1970-01-01T00:00:00.000Z").plusSeconds(((seconds.toFloat.toInt/100.0).round*100).toInt).toString
    spark.udf.register("addTime", addTime _)

    val timeDf = sc.parallelize(sciTenVarAbTen_time).toDF("time_temp").selectExpr("cast(addTime(time_temp) as timestamp) as time")
    val frtDf = sc.parallelize(sciTenVarAbTen_frt).toDF("frt_temp").selectExpr("cast(addTime(frt_temp) as timestamp) as forecast_reference_time")

    val gridLatDf = sc.parallelize(sciTenVarAbTen_lat).toDF("latitude")
    val gridLonDf = sc.parallelize(sciTenVarAbTen_lon).toDF("longitude")
    val gridFpDf = sc.parallelize(sciTenVarAbTen_fp).toDF("forecast_period")


    val gridDf = timeDf.crossJoin(frtDf).crossJoin(gridLatDf).crossJoin(gridLonDf).crossJoin(gridFpDf)
      .withColumn("index", monotonically_increasing_id)
      .withColumn("idx", row_number().over(windowSpec))
      .filter($"idx" <= 10)
      .drop("index")

    gridDf.cache()
    log.info("grid" + gridDf.count())

    val outDf = sc.parallelize(sciTenVarAbTen_out)
      .toDF("convective_cloud_area_fraction")
      .withColumn("index", monotonically_increasing_id)
      .withColumn("idx", row_number().over(windowSpec))
      .filter($"idx" <= 10)
      .drop("index")

    outDf.cache()
    log.info("out" + outDf.count())


    val resultDf = gridDf.join(outDf, outDf("idx") === gridDf("idx"), "inner")
      .drop("idx")

    resultDf.createOrReplaceTempView("resultDf")
    val finalDf = spark.sql("select a.*,date(a.forecast_reference_time) as forecase_date from resultDf a limit 5")


    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    log.info("Now Writing to Hive")
    log.info(resultDf.schema)
    finalDf.write.mode("append")
      .format("parquet")
      .insertInto("datamart_mogrepsuk.convective_cloud_area_fraction")

    log.info("Write Complete")

    //TODO: generalize some components to auto detect grids
  }
}
