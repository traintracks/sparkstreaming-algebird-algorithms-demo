package io.traintracks.demo.spark.streaming.algebird

/**
 * Created by boson on 8/28/14.
 */

import com.twitter.algebird.Max
import org.apache.spark.streaming._
import com.twitter.algebird.Operators._

object AlgebirdExampleDemo extends App {
  def getMap(line: String): Map[Int, Max[Int]] = {
    val arr = line.split(" ")
    if (arr.length == 2) {
      return Map(arr(0).toInt -> Max(arr(1).toInt))
    }
    return Map()
  }

  val master = "local[2]"
  // Create a StreamingContext with a local master
  val ssc = new StreamingContext(master, "AlgebirdExampleDemo", Seconds(4))
  // Create a Dstream that will connect to serverIP:serverPort, like localhost:9999
  val lines = ssc.socketTextStream("localhost", 9999)
  lines.map(line => getMap(line)).reduce(_ + _).print()
  ssc.start()
  ssc.awaitTermination()
}
