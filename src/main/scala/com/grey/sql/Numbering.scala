package com.grey.sql

import org.apache.spark.sql.SparkSession

class Numbering(spark: SparkSession) {

  def numbering(): Unit = {

    println("\n\nCase: SQL, Numbering")

    // window specification:
    //  [partition clause] [order clause] [frame clause]
    val partitionClause = "PARTITION BY bike_number"
    val orderClause = "ORDER BY start_date DESC NULLS LAST"
    val frameClause = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

    // Plain row number
    spark.sql("SELECT bike_number, start_date, duration, " +
      s"SUM(duration) over ($partitionClause $orderClause $frameClause) AS total_sec, " +
      s"ROW_NUMBER() over ($partitionClause $orderClause $frameClause) AS total_row_number " +
      "FROM rides").show(5)

  }

}
