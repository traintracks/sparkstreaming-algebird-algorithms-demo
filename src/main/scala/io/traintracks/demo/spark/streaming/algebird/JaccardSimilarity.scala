package io.traintracks.demo.spark.streaming.algebird

import com.twitter.algebird.MinHasher32
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by boson on 9/4/14.
 *
 * The targetThreshold controls
 * the desired level of similarity - the higher the threshold, the more efficiently
 * you can find all the similar sets.
 *
 *
 *
 * A signature is represented by a byte array of approx maxBytes size.
 *
 *
 * set the targetThreshold 0.8
 * set the signature       1024
 */
object JaccardSimilarity extends App {
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

  val mhMonoid = new MinHasher32(0.5, 1024)
  var globalSet: Set[String] = Set()
  val batchSet = lines.map(id => Set(id)).reduce(_ ++ _)
  batchSet.foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val exactSimilarity = (partial & globalSet).size.toDouble / (partial ++ globalSet).size
      println("Exact similarity is %.3f".format(exactSimilarity.toDouble))
      val sig1 = partial.map { l => mhMonoid.init(l.toString)}.reduce { (a, b) => mhMonoid.plus(a, b)}
      var sig2 = mhMonoid.zero
      if (globalSet.size != 0) {
        sig2 = globalSet.map { l => mhMonoid.init(l.toString)}.reduce { (a, b) => mhMonoid.plus(a, b)}
      }
      val approxSimilarity = mhMonoid.similarity(sig1, sig2)
      println("Approx similarity is %.3f".format(approxSimilarity.toDouble))
      globalSet ++= partial
    }
  })

  ssc.start()
  ssc.awaitTermination()

}
