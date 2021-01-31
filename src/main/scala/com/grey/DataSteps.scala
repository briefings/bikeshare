package com.grey

import com.grey.inspectors.InspectArguments
import com.grey.sources.DataRead
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class DataSteps(spark: SparkSession) {

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {

    val (ridesFrame: DataFrame, ridesSet: Dataset[Row]) = new DataRead(spark = spark)
      .dataRead(parameters = parameters)

    println("\nFrame")
    ridesFrame.show(5)
    ridesFrame.printSchema()

    println("\nAnd")
    ridesSet.show(5)
    ridesSet.printSchema()


  }

}
