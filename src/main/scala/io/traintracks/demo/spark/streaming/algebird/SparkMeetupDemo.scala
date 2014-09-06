package io.traintracks.demo.spark.streaming.algebird

import com.twitter.algebird._
import com.twitter.algebird.Monoid._
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
    "sparkmeetup" -> 1
  )
  val monoidMap = new MapMonoid[String, Long]()

  val conf = new SparkConf()
  conf.setMaster(master)
  conf.setAppName("SparkMeetup")
  val ssc = new StreamingContext(conf, Seconds(4))
  val lines = KafkaUtils.createStream(ssc, "localhost:2181", "sparkmeetup", topics).map(_._2)
  ssc.checkpoint("sparkmeetup")

  implicit def string2Bytes(i: String) = i.toCharArray.map(_.toByte)

  def accumulateMonoid[T](values: Seq[T], state: Option[T])(implicit monoid: Monoid[T]) = {
    monoid.sumOption(Seq(state.getOrElse(monoid.zero)) ++ values)
  }

  // SketchMap
  val (seed, eps, delta, heavyHittersCount) = (1, 0.001, 1E-8, 5)
  val smParams = SketchMapParams[String](seed, eps, delta, heavyHittersCount)
  implicit val smMonoid = SketchMap.monoid[String, Long](smParams)
  val smDStream = lines.map(id => (id, smMonoid.create((id, 1L))))
  val smStateDStream = smDStream.updateStateByKey[SketchMap[String, Long]](accumulateMonoid[SketchMap[String, Long]] _)

  val exactMapDStream = lines.map(id => (id, Map(id -> 1L)))
  val exactMapUpdateFunc = (values: Seq[Map[String, Long]], state: Option[Map[String,Long]]) => {
    val curValues = if (values.nonEmpty) values.reduce(monoidMap.plus) else Map[String,Long]()
    val preValues = state.getOrElse[Map[String, Long]](Map[String, Long]())
    Some(monoidMap.plus(curValues, preValues))
  }
  val exactMapStateDStream = exactMapDStream.updateStateByKey[Map[String, Long]](exactMapUpdateFunc)

  exactMapStateDStream.map(m => m._2).reduce(monoidMap.plus).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val topK = partial.toSeq.sortBy(_._2).reverse.slice(0, heavyHittersCount)
      println(s"Exact heavyHitters (video_id, view_count): $topK\n")
    }
  })
  smStateDStream.map(m => m._2).reduce(smMonoid.plus).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val heavyHitters = smMonoid.heavyHitters(partial)
      println(s"SketchMap heavyHitters (video_id, view_count): $heavyHitters\n")
    }
  }
  )
  smStateDStream.checkpoint(Seconds(12))
  exactMapStateDStream.checkpoint(Seconds(12))

  // HyperLogLog
  val bits = 12
  implicit val hllMonoid = new HyperLogLogMonoid(bits)
  val hllDStream = lines.map(id => id -> hllMonoid(id.getBytes(Charsets.UTF_8)))
  val hllStateDStream = hllDStream.updateStateByKey[HLL](accumulateMonoid[HLL] _)
  val exactHLLDStream = lines.map(id => id -> Set(id))
  val exactHLLStateDStream = exactHLLDStream.updateStateByKey[Set[String]](accumulateMonoid[Set[String]] _)

  exactHLLStateDStream.map(m => m._2).reduce(_ union _).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val exactSize = partial.size
      println(s"Exact Set Number (user_count): $exactSize\n")
    }
  })

  hllStateDStream.map(m => m._2).reduce(hllMonoid.plus).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val approxSize = partial.estimatedSize.toLong
      println(s"HyperLogLog Set approxSize (user_count): $approxSize\n\n===\n")
    }
  })
  hllStateDStream.checkpoint(Seconds(12))
  exactHLLStateDStream.checkpoint(Seconds(12))
  ssc.start()
  ssc.awaitTermination()
}
