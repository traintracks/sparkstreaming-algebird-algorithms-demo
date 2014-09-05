package io.traintracks.demo.spark.streaming.algebird

import com.twitter.algebird._
import com.twitter.algebird.Monoid._
import org.apache.commons.io.Charsets
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import scalaz.Scalaz._

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

  // SketchMap
  val DELTA = 1E-8
  val EPS = 0.001
  val SEED = 1
  val HEAVY_HITTERS_COUNT = 5

  implicit def string2Bytes(i: String) = i.toCharArray.map(_.toByte)

  def accumulateMonoid[T](values: Seq[T], state: Option[T])(implicit monoid: Monoid[T]) = {
    monoid.sumOption(Seq(state.getOrElse(monoid.zero)) ++ values)
    //    Option(values.fold[T](state.getOrElse(Monoid.zero[T]))(Monoid.plus[T]))
  }

  val SM_PARAMS = SketchMapParams[String](SEED, EPS, DELTA, HEAVY_HITTERS_COUNT)
  implicit val smMonoid = SketchMap.monoid[String, Long](SM_PARAMS)
  val smDstream = lines.map(id => (id, smMonoid.create((id, 1L))))
  val smStateDstream = smDstream.updateStateByKey[SketchMap[String, Long]](accumulateMonoid[SketchMap[String, Long]] _)

  val exactMapDstream = lines.map(id => (id, Map(id -> 1L)))

  val exactMapUpdateFunc = (values: Seq[Map[String, Long]], state: Option[Map[String,Long]]) => {
    val curValues = if (values.nonEmpty) values.reduce(monoidMap.plus(_, _)) else Map[String,Long]()
    val preValues = state.getOrElse[Map[String, Long]](Map[String, Long]())
    Some(monoidMap.plus(curValues, preValues))
  }
  val exactMapStateDstream = exactMapDstream.updateStateByKey[Map[String, Long]](exactMapUpdateFunc)

  exactMapStateDstream.map(m => m._2).reduce(monoidMap.plus(_,_)).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val topK = partial.toSeq.sortBy(_._2).reverse.slice(0, HEAVY_HITTERS_COUNT)
      println(s"Exact heavyHitters: ${topK}\n")
    }
  })
  smStateDstream.map(m => m._2).reduce(smMonoid.plus(_, _)).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val heavyHitters = smMonoid.heavyHitters(partial)
      println(s"SketchMap heavyHitters: ${heavyHitters}\n")
    }
  }
  )
  smStateDstream.checkpoint(Seconds(12))
  exactMapStateDstream.checkpoint(Seconds(12))

  // HyperLogLog
  val BITS = 12
  implicit val hllMonoid = new HyperLogLogMonoid(BITS)
  val hllDstream = lines.map(id => (id -> hllMonoid(id.getBytes(Charsets.UTF_8))))
  val hllStateDstream = hllDstream.updateStateByKey[HLL](accumulateMonoid[HLL] _)
  val exactHLLDstream = lines.map(id => (id -> Set(id)))
  val exactHLLStateDstream = exactHLLDstream.updateStateByKey[Set[String]](accumulateMonoid[Set[String]] _)

  exactHLLStateDstream.map(m => m._2).reduce(_ union _).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val exactSize = partial.size
      println(s"Exact Set Number: ${exactSize}\n")
    }
  })

  hllStateDstream.map(m => m._2).reduce(hllMonoid.plus(_, _)).foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val partial = rdd.first()
      val approxSize = partial.estimatedSize.toLong
      println(s"HyperLogLog Set approxSize: ${approxSize}\n\n===\n")
    }
  })
  hllStateDstream.checkpoint(Seconds(12))
  exactHLLStateDstream.checkpoint(Seconds(12))
  ssc.start()
  ssc.awaitTermination()
}
