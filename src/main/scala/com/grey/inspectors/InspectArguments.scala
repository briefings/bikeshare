package com.grey.inspectors

import java.net.URL
import java.util.{Objects, List => JList}

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import scala.util.Try
import scala.util.control.Exception
import scala.collection.JavaConverters._

object InspectArguments {

  def inspectArguments(args: Array[String]): Parameters = {

    // Is the string args(0) a URL?
    val isURL: Try[Boolean] = new IsURL().isURL(args(0))

    // If the string is a valid URL parse & verify its parameters
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())

    val getParameters: Try[Parameters] = if (isURL.isSuccess){
      Exception.allCatch.withTry(
        mapper.readValue(new URL(args(0)), classOf[Parameters])
      )
    } else {
      sys.error(isURL.failed.get.getMessage)
    }

    if (getParameters.isSuccess) {
      getParameters.get
    } else {
      sys.error(getParameters.failed.get.getMessage)
    }

  }

  // Verification of parameters
  class Parameters(@JsonProperty("dataSet") _dataSet: JList[String],
                   @JsonProperty("typeOf") _typeOf: String,
                   @JsonProperty("schemaOf") _schemaOf: JList[String]){


    /*
    require(_dataSet != null, "The parameter dataSet, i.e., the list of folders that lead to the data set of " +
      "interest is required.  The list items must be sequentially relative to the resources directory.")
    */
    Objects.requireNonNull(_dataSet, "The parameter dataSet, i.e., the list of folders that lead to the data set of " +
      "interest is required.  The list items must be sequentially relative to the resources directory.")
    val dataSet: List[String] = _dataSet.asScala.toList

    require(_typeOf != null, "The parameter typeOf, i.e., the extension string of the files, is required.")
    val typeOf: String = _typeOf

    /*
    require(_schemaOf != null, "The parameter schemaOf, i.e., the list of folders + schema file name, " +
      "is required.  The list items must be sequentially relative to the resources directory.")
    */
    Objects.requireNonNull(_schemaOf, "The parameter schemaOf, i.e., the list of folders + schema file name, " +
      "is required.  The list items must be sequentially relative to the resources directory.")
    val schemaOf: List[String] = _schemaOf.asScala.toList

  }

}
