package com.grey.queries

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, dense_rank, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class RankingArithmetic(spark: SparkSession) {

  /**
    * Focus: rank(), dense_rank()
    *
    * @param rides: The Dataset[Row] of rides
    */
  def rankingArithmetic(rides: Dataset[Row]): Unit = {

    println("\n\nRanking Arithmetic\n")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * Example: rank(), dense_rank()
      */
    println("Rankings of bicycles in relation to -descending total ride time- per bicycle.  The " +
      "previews are w.r.t. sql & dataset, respectively")


    /**
      * SQL
      */
    spark.sql("SELECT bike_number, sum(duration) as total_duration FROM rides " +
      "GROUP BY bike_number").cache().createOrReplaceTempView(viewName = "summary")

    spark.sql("SELECT bike_number, total_duration, " +
      "RANK() OVER (ORDER BY total_duration DESC NULLS LAST) AS ranks,  " +
      "DENSE_RANK() OVER (ORDER BY total_duration DESC NULLS LAST) AS dense_ranks " +
      "FROM summary").show(5)


    /**
      * Dataset[Row]
      */
    val summary: Dataset[Row] = rides.select($"bike_number", $"duration")
      .groupBy($"bike_number").agg(sum($"duration").as("total_duration"))

    val windowSpec = Window.orderBy($"total_duration".desc_nulls_last)

    summary.select($"bike_number", $"total_duration",
      rank().over(windowSpec).as("rank"),
      dense_rank().over(windowSpec).as("dense_asc")
    ).show(5)


  }

}
