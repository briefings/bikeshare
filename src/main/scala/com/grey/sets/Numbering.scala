package com.grey.sets

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, row_number}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Numbering(spark: SparkSession) {

  def numbering(rides: Dataset[Row]): Unit = {

    println("\n\nCase: Sets, Numbering")

    // Import implicits for
    //    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    //    implicit conversions, e.g., converting a RDD to a DataFrames.
    //    access to the "$" notation.
    import spark.implicits._

    // Window
    val windowSpec = Window.partitionBy($"bike_number").orderBy($"start_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Plain row number
    rides.select($"bike_number", $"start_date", $"duration",
      sum($"duration").over(windowSpec).as("total_sec"),
      row_number().over(windowSpec).as("total_row_number")).show(5)

  }

}
