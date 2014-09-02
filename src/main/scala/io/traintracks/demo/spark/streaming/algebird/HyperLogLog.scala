package io.traintracks.demo.spark.streaming.algebird


import com.twitter.algebird.HyperLogLogMonoid
import org.apache.commons.io.Charsets
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.twitter.algebird.HyperLogLog._

/**
 * Created by boson on 8/31/14.
 */
object HyperLogLog extends App {
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

  val hyperLogLog = new HyperLogLogMonoid(12)
  val approxVisitors = lines.mapPartitions(ids => {
    ids.map(id => hyperLogLog(id.getBytes(Charsets.UTF_8)))
  }).reduce(_ + _)

  val globalHll = new HyperLogLogMonoid(12)
  var h = globalHll.zero
  approxVisitors.foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      h += partial
      println("Approx distinct visitors this batch: %d".format(partial.estimatedSize.toInt))
      println("Approx distinct visitors total: %d".format(globalHll.estimateSize(h).toInt))
    }
  })

  val exactVisitors = lines.map(id => Set(id)).reduce(_ ++ _)
  var globalVisitorSet: Set[String] = Set()
  exactVisitors.foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      globalVisitorSet ++= partial
      println("Exact distinct visitors this batch: %d".format(partial.size))
      println("Exact distinct visitors total: %d".format(globalVisitorSet.size))
    }
  })
  ssc.start()
  ssc.awaitTermination()
}
