package io.traintracks.demo.spark.streaming.algebird.test

import org.scalatest.{FunSuite, FlatSpec}
import com.twitter.algebird.Operators._
import com.twitter.algebird.Max

/**
 * Created by boson on 8/29/14.
 */
class AlgebirdExampleDemoTest extends FunSuite {
  test("Map(1 -> Max(2)) + Map(1 -> Max(3)) should be Map(1 -> Max(3))") {
    assert(Map(1 -> Max(2)) + Map(1 -> Max(3)) == Map(1 -> Max(3)))
  }
}

