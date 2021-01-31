package com.grey.sources

import org.apache.spark.sql.DataFrame

class DataFields {

  def dataFields(data: DataFrame): DataFrame = {

    // The data fields
    val fields: Array[String] = data.columns

    // A mutable form of the data
    var frame: DataFrame = data

    // field.trim.toLowerCase.replaceAll(" +", "_")
    // field.trim.toLowerCase.replaceAll("\\s+", "_")
    // Hence, rename each column to its lower case form wherein the space between words is replaced with an underscore
    fields.foreach{field =>
      frame = frame.withColumnRenamed(field,
        field.trim.toLowerCase.replaceAll("\\s+", "_"))
    }

    frame

  }

}
