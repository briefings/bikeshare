package com.grey

import com.grey.inspectors.InspectArguments
import com.grey.sources.DataRead
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataSteps(spark: SparkSession) {

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {

    val frame: DataFrame = new DataRead(spark = spark).dataRead(parameters = parameters)
    frame.show()

  }

}
