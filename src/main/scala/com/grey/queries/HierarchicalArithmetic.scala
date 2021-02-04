package com.grey.queries

import org.apache.spark.sql.functions.{count, month}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class HierarchicalArithmetic(spark: SparkSession) {

  /**
    * Focus: rollup()
    *
    * @param rides: The Dataset[Row] of rides
    */
  def hierarchicalArithmetic(rides: Dataset[Row]): Unit = {


    println("\n\nHierarchical Arithmetic\n")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * Example: rollup()
      */
    println("The number of ride departures per station per month.  " +
      "The previews are w.r.t. sql & dataset, respectively")

    /**
      * SQL
      */
    spark.sql("SELECT start_station, month(start_date) AS start_month, COUNT(*) AS departures " +
      "FROM rides GROUP BY start_station, start_month WITH ROLLUP " +
      "ORDER BY start_station ASC NULLS LAST, start_month ASC NULLS LAST").show(39)

    /**
      * Dataset[Row]
      */
    rides.select($"start_station", month($"start_date").as("start_month"))
      .rollup($"start_station", $"start_month").agg(count("*").as("departures"))
      .sort($"start_station".asc_nulls_last, $"start_month".asc_nulls_last)
      .show(39)

    /**
      * A Verification
      */
    rides.select($"start_station", month($"start_date").as("start_month"))
      .groupBy($"start_station").agg(count("*").as("departures"))
      .orderBy($"start_station".asc_nulls_last).show(3)

  }

}
