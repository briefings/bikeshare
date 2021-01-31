package com.grey.sources

import java.nio.file.Paths

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.util.Try
import scala.util.control.Exception

/**
  *
  * @param spark : An instance of SparkSession
  */
class DataRead(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def dataRead(parameters: InspectArguments.Parameters): (DataFrame, Dataset[Row]) = {

    // Implicits
    // import spark.implicits._

    // Schema of data
    val schemaOf: Try[StructType] = new SchemaOf(spark = spark).schemaOf(parameters = parameters)


    // Path to data files
    val dataPathString: String = Paths.get(localSettings.resourcesDirectory,
      parameters.dataSet: _*).toString
    println(dataPathString)

    // Sections: This expression will load data from several files because dataPathString is a wildcard path
    val data: Try[DataFrame] = Exception.allCatch.withTry(
      spark.read.schema(schemaOf.get)
        .format("csv")
        .option("header", value = true)
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("encoding", "UTF-8")
        .load(dataPathString)
    )

    // Hence
    if (data.isSuccess) {
      var frame: DataFrame = new DataFields().dataFields(data = data.get)
      frame = new DataTypes().dataTypes(data = frame)
      val caseClassOf = CaseClassOf.caseClassOf(schema = frame.schema)

      (frame, frame.as(caseClassOf))
    } else {
      sys.error(data.failed.get.getMessage)
    }

  }

}
