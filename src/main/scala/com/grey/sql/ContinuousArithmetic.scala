package com.grey.sql

import org.apache.spark.sql.SparkSession

class ContinuousArithmetic(spark: SparkSession) {

  def continuousArithmetic(): Unit = {

    println("\n\nCase: SQL, Continuous Arithmetic")

    // Continuous riding time
    val continuous = spark.sql("SELECT bike_number, start_date, duration, " +
      "SUM(duration) over " +
      "(PARTITION BY bike_number ORDER BY start_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
      "AS total_sec FROM rides")
    println("\nContinuous")
    println(s"# of records: ${continuous.count()}")

    // Cf.
    val discrete = spark.sql("SELECT bike_number, " +
      "SUM(duration) AS total_sec FROM rides GROUP BY bike_number")
    println("\nDiscrete")
    println(s"# of records ${discrete.count()}")


    continuous.createOrReplaceTempView("continuous")
    discrete.createOrReplaceTempView("discrete")
    val intersection = spark.sql("SELECT discrete.bike_number, discrete.total_sec FROM discrete INNER JOIN continuous " +
      "ON (discrete.bike_number = continuous.bike_number AND discrete.total_sec = continuous.total_sec)")
    println("\nIntersection")
    println(s"# of records ${intersection.count()}")
    intersection.show(3)

  }

}
