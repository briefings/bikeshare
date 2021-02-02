package com.grey.sets

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class ContinuousArithmetic(spark: SparkSession) {

  def continuousArithmetic(rides: Dataset[Row]): Unit = {

    println("\n\nCase: Sets, Continuous Arithmetic")

    // Import implicits for
    //    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    //    implicit conversions, e.g., converting a RDD to a DataFrames.
    //    access to the "$" notation.
    import spark.implicits._

    // Window
    // Imagine a bike riding firm that services a bike as as soon as the energy expended by
    // the bike exceeds a joules level -
    //    [bike weight (kg)] * [distance travelled thus far (m)]^2 / [travel time thus far (sec)]^2
    // all relative to the last bike service date.
    // We do not have distance details yet, hence the initial focus is time.
    val windowSpec = Window.partitionBy($"bike_number").orderBy($"start_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Continuous riding time
    val continuous: DataFrame = rides.select($"bike_number", $"start_date", $"duration",
      sum($"duration").over(windowSpec).as("total_sec"))
    println("\nContinuous")
    println(s"# of records: ${continuous.count()}")

    // Cf.
    // The maximum continuous_sec per unique bike_number must be equal to the value in the discrete table
    val discrete: DataFrame = rides.select($"bike_number", $"duration")
      .groupBy($"bike_number")
      .agg(sum($"duration").as("total_sec"))
    println("\nDiscrete")
    println(s"# of records ${discrete.count()}")

    // Verify
    val intersection = discrete.join(continuous.select($"bike_number", $"total_sec"),
      Seq("bike_number", "total_sec"), joinType = "inner")
    println("\nIntersection")
    println(s"# of records ${intersection.count()}")
    intersection.show(3)

  }

}
