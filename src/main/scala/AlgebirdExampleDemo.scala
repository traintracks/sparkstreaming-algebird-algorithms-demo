/**
 * Created by boson on 8/28/14.
 */
import org.apache.spark.api.java.function._
import org.apache.spark.streaming._
import org.apache.spark.streaming.api._
import org.apache.spark.streaming.StreamingContext._

object AlgebirdExampleDemo {
  def main(args: Array[String]): Unit = {

    val master = "local"
    // Create a StreamingContext with a local master
    val ssc = new StreamingContext(master, "AlgebirdDemo", Seconds(1))
    // Create a Dstream that will connect to serverIP:serverPort, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
