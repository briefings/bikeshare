package com.grey.sources

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType

class DataTypes() {

  def dataTypes(data: DataFrame): DataFrame = {

    // timestamp fields
    val timestamps = List("start_date", "end_date")
    var frame = data

    timestamps.foreach { field =>
      val temporary = s"temp_$field"
      frame = frame.withColumn(temporary, col(field).cast(TimestampType)).drop(field)
        .withColumnRenamed(temporary, field)

    }
    frame

  }

}
