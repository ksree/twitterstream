package com.kxs.de

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import twitter4j.Status

class TwitterAppTest extends FunSuite with SharedSparkContext with BeforeAndAfter {
  // re-use the spark context
  override implicit def reuseContextIfPossible: Boolean = true

  test("Twitter top 10 hastag") {
    var ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    TwitterApp.setTwitterAuth

    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, Seq.empty[String])
    val seconds: Long = 10

    TwitterApp.topHashTags(stream, seconds)

    ssc.start()
    ssc.awaitTerminationOrTimeout(5000)
  }

}
