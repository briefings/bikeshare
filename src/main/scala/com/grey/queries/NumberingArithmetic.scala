package com.grey.queries

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class NumberingArithmetic(spark: SparkSession) {

  /**
    * Focus: row_number()
    *
    * @param rides: The Dataset[Row] of rides
    */
  def numberingArithmetic(rides: Dataset[Row]): Unit = {


    println("\n\nNumbering Arithmetic\n")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * The example
      */
    println("The row number of each bicycle's cumulative duration value wherein " +
      "the cumulative values are w.r.t. ascending start time.  The previews are w.r.t. " +
      "sql & dataset, respectively")


    /**
      * SQL
      * 
      * window specification: [partition clause] [order clause] [frame clause]
      * 
      */
    val partitionClause = "PARTITION BY bike_number"
    val orderClause = "ORDER BY start_date ASC NULLS LAST"
    val frameClause = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

    spark.sql("SELECT bike_number, start_date, duration, " +
      s"SUM(duration) over ($partitionClause $orderClause $frameClause) AS c_duration_of_bike, " +
      s"ROW_NUMBER() over ($partitionClause $orderClause $frameClause) AS c_row_number_of_bike " +
      "FROM rides").show(3)


    /**
      * Dataset[Row]
      */
    val windowSpec = Window.partitionBy($"bike_number").orderBy($"start_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    rides.select($"bike_number", $"start_date", $"duration",
      sum($"duration").over(windowSpec).as("c_duration_of_bike"),
      row_number().over(windowSpec).as("c_row_number_of_bike")).show(3)



  }

}
