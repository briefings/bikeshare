package com.grey.sets

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, rank}

class Rankings(spark: SparkSession) {

  /**
    * Of interest: The ranking of bikes w.r.t. total riding time thus far
    *
    * @param rides: The dataset of rides
    */
  def rankings(rides: Dataset[Row]): Unit = {

    println("\n\nCase: Sets, Rankings")

    // Import implicits for
    //    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    //    implicit conversions, e.g., converting a RDD to a DataFrames.
    //    access to the "$" notation.
    import spark.implicits._

    // A summary of total riding times per bike
    val summary: Dataset[Row] = rides.select($"bike_number", $"duration")
      .groupBy($"bike_number").agg(sum($"duration")
      .as("total_sec"))

    // Window of calculation
    val windowSpec = Window.orderBy($"total_sec".desc_nulls_last)
      //.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    // Hence
    summary.select($"bike_number", $"total_sec",
      rank().over(windowSpec).as("rank")).show(5)

  }

}
