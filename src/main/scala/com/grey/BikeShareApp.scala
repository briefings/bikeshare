package com.grey

import com.grey.inspectors.InspectArguments
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Bicycle Sharing!
 *
 */
object BikeShareApp {

  def main(args: Array[String]): Unit = {

    // Arguments
    if (args.length == 0) {
      sys.error("The YAML of parameters is required.")
    }


    // Inspect
    val inspectArguments = InspectArguments
    val parameters: InspectArguments.Parameters = inspectArguments.inspectArguments(args)


    // Limiting log data streams
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark Session Instance
    val spark = SparkSession.builder().appName("bikeshare")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", "")
      .getOrCreate()

    // Spark Context Level Logging
    spark.sparkContext.setLogLevel("ERROR")

    // Configurations
    spark.conf.set("spark.kryoserializer.buffer.max", "2048m")

    // Graphs Model Checkpoint Directory
    spark.sparkContext.setCheckpointDir("/tmp")

    // Hence
    new DataSteps(spark = spark).dataSteps(parameters = parameters)

  }

}
