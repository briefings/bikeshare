## SQL & Datasets

**Brief Investigations**: via the Washington D.C. Capital BikeShare Data

<br/>

* [Sources](#sources)
* [Development Notes](#development-notes)
  * [Software](#software)
* [Test Notes](#test-notes)  
* [Logging](#logging-options)

<br/>

### Sources

[Washington D.C. Capital BikeShare](https://www.capitalbikeshare.com)
* Specifically [2014 Capital BikeShare Trip Data](https://s3.amazonaws.com/capitalbikeshare-data/2014-capitalbikeshare-tripdata.zip)
* https://www.capitalbikeshare.com/system-data

<br/>

### Development Notes

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

In terms of packaging, **Maven**, instead of **Scala Build Tool** (SBT), is now being used for all projects.  Also of import w.r.t. Maven's 
pom, the [spark packages hosted by bintray](https://dl.bintray.com/spark-packages/maven/), e.g., packaging details w.r.t. 
the [graphframes package](https://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.8.1-spark2.4-s_2.11/)
  
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

### Test Notes

```shell

spark-submit --class com.grey.BikeShareApp --name "bikeshare" --master spark://... --num-executors 2 
  target/bikeshare-1.0.47-jar-with-dependencies.jar https://raw.githubusercontent.com/briefings/bikeshare/develop/arguments.yaml
  
spark-submit --class com.grey.BikeShareApp --name "bikeshare" --master spark://... --total-executor-cores 2 --num-executors 2 
  target/bikeshare-1.0.47-jar-with-dependencies.jar https://raw.githubusercontent.com/briefings/bikeshare/develop/arguments.yaml
  
```

<br>

### Logging Options

 ```
    <groupId>com.typesafe.scala-logging</groupId>
    <artifactId>scala-logging_2.11</artifactId>
    <version>3.9.2</version>
    
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.2.3</version>
      
    <groupId>com.typesafe.play</groupId>
    <artifactId>play_2.11</artifactId>
    <version>2.7.7</version>
```

* [scala-logging](https://index.scala-lang.org/lightbend/scala-logging/scala-logging/3.9.2?target=_2.11)
* [ScalaLogging](https://www.playframework.com/documentation/2.6.x/ScalaLogging) <br/>
* [Log4j](https://logging.apache.org/log4j/2.x/)
  * [Scala API](https://logging.apache.org/log4j/scala/)
  * [Tutorials](https://howtodoinjava.com/log4j/)
  * [Console Appender](https://howtodoinjava.com/log4j/log4j-console-appender-example/)



