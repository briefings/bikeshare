package com.grey.queries

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.sum

/**
  *
  * @param spark: An instance of SparkSession
  */
class ContinuousArithmetic(spark: SparkSession) {

  /**
    * Focus: window
    *
    * @param rides: The Dataset[Row] of rides
    */
  def continuousArithmetic(rides: Dataset[Row]): Unit = {


    println("\n\nContinuous Arithmetic\n")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * Example: window
      *
      * Imagine a bike riding firm that services a bike as as soon as the energy expended by
      *
      *   [bike weight (kg)] * [distance travelled thus far (m)]^2 / [travel time thus far (sec)]^2
      *
      * relative to the last bike service date & time.  Then a value of interest, per bike, is the
      * cumulative duration value w.r.t. ascending start_date
      */
    println("Each bicycle's cumulative duration values wherein the cumulative values are w.r.t. " +
      "ascending start time.  The previews are w.r.t. sql & dataset, respectively")

    /**
      * SQL
      */
    val partitionClause = "PARTITION BY bike_number"
    val orderClause = "ORDER BY start_date ASC NULLS LAST"
    val frameClause = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

    spark.sql("SELECT bike_number, start_date, duration, " +
      s"SUM(duration) over ($partitionClause $orderClause $frameClause) AS c_duration_of_bike " +
      "FROM rides " +
      "ORDER BY bike_number ASC NULLS LAST, start_date ASC NULLS LAST").show(3)

    /**
      * Dataset[Row]
      */
    val windowSpec = Window.partitionBy($"bike_number").orderBy($"start_date".asc_nulls_last)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    rides.select($"bike_number", $"start_date", $"duration",
      sum($"duration").over(windowSpec).as("c_duration_of_bike")
    ).orderBy($"bike_number".asc_nulls_last, $"start_date".asc_nulls_last).show(3)


  }

}
