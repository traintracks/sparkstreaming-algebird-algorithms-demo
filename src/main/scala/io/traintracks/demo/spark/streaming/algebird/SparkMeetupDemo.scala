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
    "demotopic" -> 1
  )

  val conf = new SparkConf()
  conf.setMaster(master)
  conf.setAppName("SparkMeetup")
  val ssc = new StreamingContext(conf, Seconds(4))
  val lines = KafkaUtils.createStream(ssc, "localhost:2181", "demotopic", topics).map(_._2)
  ssc.checkpoint("demotopic")

  implicit def string2Bytes(i: String) = i.toCharArray.map(_.toByte)

  def accumulateMonoid[T](values: Seq[T], state: Option[T])(implicit monoid: Monoid[T]) = {
    monoid.sumOption(Seq(state.getOrElse(monoid.zero)) ++ values)
  }

  // SketchMap
  val (seed, eps, delta, heavyHittersCount) = (1, 0.001, 1E-8, 5)
  val smParams = SketchMapParams[String](seed, eps, delta, heavyHittersCount)
  implicit val smMonoid = SketchMap.monoid[String, Long](smParams)
  val smDStream = lines.map(id => smMonoid.create((id, 1L))).reduce(smMonoid.plus).map(sm => "key" -> sm)
  val smStateDStream = smDStream.updateStateByKey[SketchMap[String, Long]](accumulateMonoid[SketchMap[String, Long]] _)
  val mapMonoid = new MapMonoid[String, Long]()
  val exactMapDStream = lines.map(id => Map(id -> 1L)).reduce(mapMonoid.plus).map(map => "key" -> map)
  val exactMapUpdateFunc = (values: Seq[Map[String, Long]], state: Option[Map[String, Long]]) => {
    val curValues = if (values.nonEmpty) values.reduce(mapMonoid.plus) else Map[String, Long]()
    val preValues = state.getOrElse[Map[String, Long]](Map[String, Long]())
    Some(mapMonoid.plus(curValues, preValues))
  }
  val exactMapStateDStream = exactMapDStream.updateStateByKey[Map[String, Long]](exactMapUpdateFunc)
  val joinedSMStateDStream = exactMapStateDStream.join(smStateDStream)
  joinedSMStateDStream.foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val mapSM = rdd.first()._2
      val map = mapSM._1
      val sm = mapSM._2
      val mapHH = map.toSeq.sortBy(_._2).reverse.slice(0, heavyHittersCount)
      val smHH = smMonoid.heavyHitters(sm)
      println(s"SketchMap heavyHitters (video_id, view_count): $smHH \n" +
        s"Exact heavyHitters (video_id, view_count): $mapHH\n")
    }
  })
  joinedSMStateDStream.checkpoint(Seconds(12))
  smStateDStream.checkpoint(Seconds(12))
  exactMapStateDStream.checkpoint(Seconds(12))

  // HyperLogLog
  val bits = 12
  implicit val hllMonoid = new HyperLogLogMonoid(bits)
  val hllDStream = lines.map(id => hllMonoid(id.getBytes(Charsets.UTF_8))).reduce(hllMonoid.plus).map(hll => "key" -> hll)
  val hllStateDStream = hllDStream.updateStateByKey[HLL](accumulateMonoid[HLL] _)
  val exactHLLDStream = lines.map(id => Set(id)).reduce(_ union _).map(idSet => "key" -> idSet)
  val exactHLLStateDStream = exactHLLDStream.updateStateByKey[Set[String]](accumulateMonoid[Set[String]] _)
  val joinedHLLStream = hllStateDStream.join(exactHLLStateDStream)
  joinedHLLStream.foreachRDD(rdd => {
    if (rdd.count() != 0) {
      val hllSet = rdd.first()._2
      val hll = hllSet._1
      val set = hllSet._2
      val approxSize = hll.estimatedSize.toInt
      val exactSize = set.size
      val error = (approxSize - exactSize).abs.toDouble / exactSize.toDouble * 100
      println(f"HyperLogLog Set approxSize (user_count): ${approxSize}%d," +
        f" Exact Set Number (user_count): $exactSize%d," +
        f" (error): $error%.2f%%\n\n===\n")
    }
  })

  hllStateDStream.checkpoint(Seconds(12))
  exactHLLStateDStream.checkpoint(Seconds(12))
  joinedHLLStream.checkpoint(Seconds(12))
  ssc.start()
  ssc.awaitTermination()
}
