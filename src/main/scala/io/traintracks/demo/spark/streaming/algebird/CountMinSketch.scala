package io.traintracks.demo.spark.streaming.algebird

import com.twitter.algebird._
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._
import spark.SparkContext._

/**
 * Created by boson on 9/3/14.
 */

object CountMinSketch extends App {
  val master = "local[4]"
  val kafkaParams = Map(
    "zookeeper.connect" -> "localhost:2181",
    "zookeeper.connection.timeout.ms" -> "10000",
    "group.id" -> "test-consumer-group"
  )
  val topics = Map(
    "meetup" -> 1
  )
  val conf = new SparkConf()
  conf.setMaster(master)
  conf.setAppName("ActualCardinality")
  val ssc = new StreamingContext(conf, Seconds(4))
  val lines = KafkaUtils.createStream(ssc, "localhost:2181", "meetup", topics).map(_._2)

  // CMS parameters
  val DELTA = 1E-3
  val EPS = 0.01
  val SEED = 1
  val PERC = 0.001

  // K highest frequency elements to take
  val TOPK = 10

  val cmsMonoid = new CountMinSketchMonoid(DELTA, EPS, SEED, PERC)
  var globalCMS = cmsMonoid.zero
  var globalExact = Map[Long, Int]()
  val monoidMap = new MapMonoid[Long, Int]()

  val exactVisitors = lines.map(id => (id.toLong, 1)).reduceByKey((a, b) => (a + b))
  exactVisitors.foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partialMap = rdd.collect().toMap
      val partialTopK = partialMap.toSeq.sortBy(_._2).reverse.slice(0, TOPK)
      globalExact = monoidMap.plus(globalExact.toMap, partialMap)
      val globalTopK = globalExact.toSeq.sortBy(_._2).reverse.slice(0, TOPK)
      println("Exact heavy hittters this batch: %s".format(partialTopK.mkString("[", ",", "]")))
      println("Exact heavy hitters overall: %s".format(globalTopK.mkString("[", ",", "]")))
    }
  })

  val approxVisitors = lines.mapPartitions(ids => {
    val cms = new CountMinSketchMonoid(DELTA, EPS, SEED, PERC)
    ids.map(id => cms.create(id.toLong))
  }).reduce(_ ++ _)

  approxVisitors.foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val partialTopK = partial.heavyHitters.map(id =>
        (id, partial.frequency(id).estimate)).toSeq.sortBy(_._2).reverse.slice(0, TOPK)
      globalCMS ++= partial
      val globalTopK = globalCMS.heavyHitters.map(id =>
        (id, globalCMS.frequency(id).estimate)).toSeq.sortBy(_._2).reverse.slice(0, TOPK)
      println("Approx heavy hitters at %2.2f%% threshold this batch: %s".format(PERC,
        partialTopK.mkString("[", ",", "]")))
      println("Approx heavy hitters at %2.2f%% threshold overall: %s".format(PERC,
        globalTopK.mkString("[", ",", "]")))

    }
  })
  ssc.start()
  ssc.awaitTermination()

}
