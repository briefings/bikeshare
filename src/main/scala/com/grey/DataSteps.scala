package com.grey

import com.grey.inspectors.InspectArguments
import com.grey.sources.DataRead
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class DataSteps(spark: SparkSession) {

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {

    // Read the BikeShare data
    val (ridesFrame: DataFrame, ridesSet: Dataset[Row]) = new DataRead(spark = spark)
      .dataRead(parameters = parameters)

    // ... persistence
    ridesFrame.persist(StorageLevel.MEMORY_ONLY)
    ridesSet.persist(StorageLevel.MEMORY_ONLY)
    ridesFrame.createOrReplaceTempView(viewName = "rides")

    // Previews
    println(s"\nThere are ${ridesSet.count()} records.\n")
    spark.sql("SHOW TABLES").show()

    // Queries
    new com.grey.queries.NumberingArithmetic(spark = spark).numberingArithmetic(rides = ridesSet)
    new com.grey.queries.ContinuousArithmetic(spark = spark).continuousArithmetic(rides = ridesSet)
    new com.grey.queries.RankingArithmetic(spark = spark).rankingArithmetic(rides = ridesSet)
    new com.grey.queries.HierarchicalArithmetic(spark = spark).hierarchicalArithmetic(rides = ridesSet)
    
  }

}
