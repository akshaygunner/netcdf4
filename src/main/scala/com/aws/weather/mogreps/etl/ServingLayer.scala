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

    val filePath = "/prods_op_mogreps-uk_20130101_03_00_006.nc"

    val variableDf = spark.sql("select * from reference_mogrepsuk.variables")
    val varDimsDf = spark.sql("select * from reference_mogrepsuk.variable_dimensions where variable_id is not null")
    varDimsDf.cache()

    varDimsDf.createOrReplaceTempView("varDims")
    spark.sql("select variable_id,collect_set(dimension) as set from varDims group by variable_id").createOrReplaceTempView("varDimGridsDf")
    spark.sql("select x.set,count(distinct x.variable_id) from varDimGridsDf x group by x.set")


    val startIndex = filePath.indexOf("uk_") + "uk_".length
    val endIndex = filePath.lastIndexOf(".nc")

    val offsetValue = filePath.substring(startIndex, endIndex)
    val partitionDate = offsetValue.substring(0, 8)
    val partitionHour = offsetValue.substring(9, 11)
    val partitionRealization = offsetValue.substring(12, 14)
    val partitionForecastPeriod = offsetValue.substring(15, 18)
    log.info("Initial declarations done")

    val lstVariables = List("wet_bulb_freezing_level_altitude", "time_0", "grid_latitude", "grid_longitude")
    val scientificRDD = ssc.netcdfDFSFiles("s3://weather.mogreps.uk/inbox/prods_op_mogreps-uk_20130101_03_00_006.nc", lstVariables)
    val sciTen = scientificRDD.collect.take(1)(0)
    val sciTenVar = sciTen.variables
    val sciTenVarAbTen_time0 = sciTenVar("time_0").data
    val sciTenVarAbTen_gridLatitude = sciTenVar("grid_latitude").data
    val sciTenVarAbTen_gridLongitude = sciTenVar("grid_longitude").data
    val sciTenVarAbTen_wetbulbfreezinglevelaltitude = sciTenVar("wet_bulb_freezing_level_altitude").data
    log.info("SciSpark functions executed")

    import spark.implicits._

    val windowSpec = Window.orderBy("index")

    def addTime(hours: Float): java.lang.String = new DateTime("1970-01-01T00:00:00.000Z").plusHours(hours.toInt).toString
    spark.udf.register("addTime", addTime _)

    val time0Df = sc.parallelize(sciTenVarAbTen_time0).toDF("time_temp").selectExpr("cast(addTime(time_temp) as timestamp) as time_0")
    val gridLatDf = sc.parallelize(sciTenVarAbTen_gridLatitude).toDF("grid_latitude")
    val gridLonDf = sc.parallelize(sciTenVarAbTen_gridLongitude).toDF("grid_longitude")

    val gridDf = time0Df.crossJoin(gridLatDf).crossJoin(gridLonDf)
      .withColumn("index", monotonically_increasing_id)
      .withColumn("idx", row_number().over(windowSpec))
      .drop("index")

    val wetbulbDf = sc.parallelize(sciTenVarAbTen_wetbulbfreezinglevelaltitude)
      .toDF("wet_bulb_freezing_level_altitude")
      .withColumn("index", monotonically_increasing_id)
      .withColumn("idx", row_number().over(windowSpec))
      .drop("index")

    val resultDf = gridDf.join(wetbulbDf, wetbulbDf("idx") === gridDf("idx"), "inner")
      .withColumn("forecast_realization", lit(partitionRealization))
      .withColumn("forecast_period", lit(partitionForecastPeriod))
      .withColumn("forecast_time", lit(partitionDate + partitionHour))
      .drop("idx")


    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    log.info("Now Writing to Hive")
    resultDf.write.mode("append").format("parquet")
      .insertInto("datamart_mogrepsuk.grid1")

    log.info("Write Complete")

    //TODO: generalize some components to auto detect grids
  }
}
