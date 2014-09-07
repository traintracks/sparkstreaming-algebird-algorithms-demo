package io.traintracks.demo.spark.streaming.algebird

import java.util.Properties

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Created by boson on 9/1/14.
 */
object KafkaLogProducer extends App {
  val props = new Properties()
  props.put("metadata.broker.list", "127.0.0.1:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)
  val messagesBatchSize = 5
  val topic = "demotopic"

  while (true) {
    val messages = (1 to messagesBatchSize).map { messageNum =>
      val randomId = scala.util.Random.nextInt(100).abs
      val str = randomId.toString
      new KeyedMessage[String, String](topic, str)
    }.toArray

    producer.send(messages: _*)
    Thread.sleep(1000)
  }
}
