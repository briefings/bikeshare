package com.grey.sources

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, TimestampType}

class DataTypes() {

  def dataTypes(data: DataFrame): DataFrame = {

    // a mutable form of the data frame
    var frame = data

    // timestamp fields
    val timestamps = List("start_date", "end_date")
    timestamps.foreach { field =>
      val temporary = s"temp_$field"
      frame = frame.withColumn(temporary, col(field).cast(TimestampType)).drop(field)
        .withColumnRenamed(temporary, field)

    }

    // integer fields
    val integers = List("duration", "start_station_number", "end_station_number")
    integers.foreach{field =>
      val temporary = s"temp_$field"
      frame = frame.withColumn(temporary, col(field).cast(IntegerType)).drop(field)
        .withColumnRenamed(temporary, field)
    }

    frame

  }

}
