package io.traintracks.demo.spark.streaming.algebird

/**
 * Created by boson on 8/28/14.
 */
import com.twitter.algebird._
import com.twitter.algebird.Operators._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.api.java.function._
import org.apache.spark.streaming._


object AlgebirdExampleDemo {
  def main(args: Array[String]): Unit = {

    val master = "local"
    // Create a StreamingContext with a local master
    val ssc = new StreamingContext(master, "AlgebirdExampleDemo", Seconds(1))
    // Create a Dstream that will connect to serverIP:serverPort, like localhost:9999
    var result = Map(1 -> Max(2))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.map(line => {
      val numbers = line.split(" ")
      if (numbers.length == 2) {
        line.map(Map(numbers(0).toInt -> Max(numbers(1).toInt)))
        println(result)
      }
    }).reduce(_ + _)
    words.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
