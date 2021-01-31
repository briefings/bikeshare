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

  def dataRead(parameters: InspectArguments.Parameters): DataFrame = {

    // Implicits
    // import spark.implicits._

    // Schema of data
    val schemaOf: Try[StructType] = new SchemaOf(spark = spark).schemaOf(parameters = parameters)
    val caseClassOf = CaseClassOf.caseClassOf(schema = schemaOf.get)

    // Path to data files
    val dataPathString: String = Paths.get(localSettings.resourcesDirectory,
      parameters.dataSet: _*).toString

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
      data.get
      // (data.get, data.get.as(caseClassOf))
    } else {
      sys.error(data.failed.get.getMessage)
    }

  }

}
