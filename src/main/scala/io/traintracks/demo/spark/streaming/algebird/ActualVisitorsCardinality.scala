package io.traintracks.demo.spark.streaming.algebird



import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io._, java.nio.channels._, java.nio._
/**
 * Created by boson on 8/29/14.
 */
object ActualVisitorsCardinality {
  def main(args: Array[String]) {
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
    var globalVisitorSet: Set[String] = Set()
    val visitors = lines.map(id => Set(id)).reduce(_ ++ _)
    visitors.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        globalVisitorSet ++= partial
        println("visitors this batch: %d".format(partial.size))
        println("globalVisitorSet.size: %d".format(globalVisitorSet.size))
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
