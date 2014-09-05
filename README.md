sparkstreaming-algebird-algorithms-demo
=======================================

## Install the followings:

* [Scala 2.10+] (http://www.scala-sbt.org/)
* [SBT] (http://www.scala-sbt.org/)
* [Apache Kafka  0.8+] (http://kafka.apache.org/)

## Buiding source jar

    sbt package

## Starting Kafka Zookeeper:

    ./bin/zookeeper-server-start.sh config/zookeeper.properties
    
## Starting Kafka Server:

    ./bin/kafka-server-start.sh config/server.properties

## Starting Kafka Producer

    sbt "run-main io.traintracks.demo.spark.streaming.algebird.KafkaLogProducer"
    
## Starting SparkMeetupDemo

Our SparkMeetupDemo will run the HyperLogLog algorithm and Count-Min Skech both with one passing Spark Streaming.
It'll receive messages from Kafka's topic called _sparkmeetup_

    sbt "run-main io.traintracks.demo.spark.streaming.algebird.SparkMeetupDemo" > SparkMeetupDemo.log

## View the result
    
    tail -f SparkMeetupDemo.log
