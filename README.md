## SQL & Datasets

**Brief Investigations**: via the Washington D.C. Capital BikeShare Data

<br/>

* [Sources](#sources)
* [Development Notes](#development-notes)
  * [Logging](#logging)
  * [Software](#software)

<br/>

### Sources

[Washington D.C. Capital BikeShare](https://www.capitalbikeshare.com)

* Specifically [2014 Capital BikeShare Trip Data](https://s3.amazonaws.com/capitalbikeshare-data/2014-capitalbikeshare-tripdata.zip)
* https://www.capitalbikeshare.com/system-data

<br/>

### Development Notes

#### Logging

* [scala-logging](https://index.scala-lang.org/lightbend/scala-logging/scala-logging/3.9.2?target=_2.11) <br/>
    ```
    <groupId>com.typesafe.scala-logging</groupId>
    <artifactId>scala-logging_2.11</artifactId>
    <version>3.9.2</version>
    
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.2.3</version>
    ```
            
    ```import com.typesafe.scalalogging.Logger```

* [ScalaLogging](https://www.playframework.com/documentation/2.6.x/ScalaLogging) <br/>
    ```
    <groupId>com.typesafe.play</groupId>
    <artifactId>play_2.11</artifactId>
    <version>2.7.7</version>
    ```
    
    ```import play.api.Logger```
    
* [Log4j](https://logging.apache.org/log4j/2.x/)
  * [Scala API](https://logging.apache.org/log4j/scala/)
  * [Tutorials](https://howtodoinjava.com/log4j/)
  * [Console Appender](https://howtodoinjava.com/log4j/log4j-console-appender-example/)


<br/>

#### Software

*  Java <br/> 
    ```
    $ java -version
    
      java version "1.8.0_181"
      Java(TM) SE Runtime Environment (build 1.8.0_181-b13) <br/> 
      Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
    ```

* Scala <br/> 
    ```bash
    $ scala -version
    
      Scala code runner version 2.11.12 -- Copyright 2002-2017, LAMP/EPFL
    ```

* Spark <br/> 
    ```bash
    $ spark-submit.cmd --version # **if** Operating System **is** Windows
    
      Spark version 2.4.7
    ```

<br/> 

In terms of packaging, **Maven**, instead of **Scala Build Tool** (SBT), is now being used for all projects
  
* Maven <br/>
    ```bash
    $ mvn -version
    
      Apache Maven 3.6.3 
    
    # Packaging: either
    $ mvn clean package 
    
    # or 
    $ mvn clean install
    ```

<br/>

Also of import w.r.t. Maven's pom, the [spark packages hosted by bintray](https://dl.bintray.com/spark-packages/maven/), 
e.g., packaging details w.r.t. the 
[graphframes package](https://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.8.1-spark2.4-s_2.11/)

<br/>

### Test Notes

```
    // Continuous riding time
    val windowSpec: WindowSpec = Window.partitionBy($"bike_number").orderBy($"start_date")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val continuous: DataFrame = rides.select($"bike_number", $"start_date", $"duration",
        sum($"duration").over(windowSpec).as("total_sec"))
      
    // Cf.
    val discrete: Dataset[Row] = rides.select($"bike_number", $"duration")
        .groupBy($"bike_number")
        .agg(sum($"duration").as("total_sec"))
    
    // Determine
    val intersection: Dataset[Row] = discrete.join(continuous.select($"bike_number", $"total_sec"),
        Seq("bike_number", "total_sec"), joinType = "inner")
        
    // Expected
    discrete.count() === intersection.count()
```

And

```
    // Continuous riding time
    val continuous: DataFrame = spark.sql("SELECT bike_number, start_date, duration, " +
      "SUM(duration) over " +
      "(PARTITION BY bike_number ORDER BY start_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
      "AS total_sec FROM rides")

    // Cf.
    val discrete: DataFrame = spark.sql("SELECT bike_number, " +
      "SUM(duration) AS total_sec FROM rides GROUP BY bike_number")

    // Determine
    continuous.createOrReplaceTempView("continuous")
    discrete.createOrReplaceTempView("discrete")
    val intersection: DataFrame = spark.sql("SELECT discrete.bike_number, discrete.total_sec " +
        "FROM discrete INNER JOIN continuous " +
        "ON (discrete.bike_number = continuous.bike_number AND discrete.total_sec = continuous.total_sec)")
      
    // Expected
    discrete.count() === intersection.count()
```













