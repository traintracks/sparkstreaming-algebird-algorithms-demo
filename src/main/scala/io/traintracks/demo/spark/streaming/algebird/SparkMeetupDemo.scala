package io.traintracks.demo.spark.streaming.algebird

import com.twitter.algebird.{HLL, HyperLogLogMonoid, SketchMapParams, SketchMap}
import org.apache.commons.io.Charsets
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


/**
 * Created by boson on 9/5/14.
 */
object SparkMeetupDemo extends App {
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
  conf.setAppName("SparkMeetup")
  val ssc = new StreamingContext(conf, Seconds(4))
  val lines = KafkaUtils.createStream(ssc, "localhost:2181", "sparkmeetup", topics).map(_._2)
  ssc.checkpoint("sparkmeetup")

  // SketchMap
  val DELTA = 1E-8
  val EPS = 0.001
  val SEED = 1
  val HEAVY_HITTERS_COUNT = 10

  implicit def string2Bytes(i: String) = i.toCharArray.map(_.toByte)

  val SM_PARAMS = SketchMapParams[String](SEED, EPS, DELTA, HEAVY_HITTERS_COUNT)
  val smMonoid = SketchMap.monoid[String, Long](SM_PARAMS)

  val smUpdateFunc = (values: Seq[SketchMap[String, Long]], state: Option[SketchMap[String, Long]]) => {
    val curValues = if (values.nonEmpty) values.reduce(smMonoid.plus(_, _)) else smMonoid.zero
    val preState = state.getOrElse(smMonoid.zero)
    Some(smMonoid.plus(curValues, preState))
  }

  val smDstream = lines.map(id => (id, smMonoid.create((id, 1L))))
  val smStateDstream = smDstream.updateStateByKey[SketchMap[String, Long]](smUpdateFunc)
  smStateDstream.map(m => m._2).reduce(smMonoid.plus(_, _)).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val heavyHitters = smMonoid.heavyHitters(partial)
      println(s"SketchMap heavyHitters: ${heavyHitters}")
    }
  })
  smStateDstream.checkpoint(Seconds(12))


  // HyperLogLog
  val BITS = 12
  val hllMonoid = new HyperLogLogMonoid(BITS)

  val hllUpdateFunc = (values: Seq[HLL], state: Option[HLL]) => {
    val curHLL = if (values.nonEmpty) values.reduce(hllMonoid.plus(_, _)) else hllMonoid.zero
    val preHLL = state.getOrElse[HLL](hllMonoid.zero)
    Some(hllMonoid.plus(curHLL, preHLL))
  }
  val hllDstream = lines.map(id => (id -> hllMonoid(id.getBytes(Charsets.UTF_8))))
  val hllStateDstream = hllDstream.updateStateByKey[HLL](hllUpdateFunc)
  hllStateDstream.map(m => m._2).reduce(hllMonoid.plus(_, _)).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val approxSize = partial.estimatedSize.toLong
      println(s"HyperLogLog approxSize: ${approxSize}")
    }
  })
  hllStateDstream.checkpoint(Seconds(12))
  ssc.start()
  ssc.awaitTermination()
}
