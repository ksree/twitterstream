package com.kxs.de

import com.kxs.de.utils.functions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status


object TwitterApp {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("TwitterStreams")
      .getOrCreate()

    setTwitterAuth

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, Seq(""))
    val seconds: Long = 10
    topHashTags(stream, seconds)

    ssc.start()
    ssc.awaitTermination()

  }

  def setTwitterAuth = {
    // Configure Twitter credentials
    val consumerKey = "ELoC3AYmLefiCnytIfSxXceTm"
    val consumerSecret = "199X0943GBpG9KxsNwq5VWJjW2SV9xZd8HDdIBkyQUaSkQ55UY"
    val accessToken = "945338272720523264-SOJA1ZXykLPfTvi0wH1zpqg0UxvzTOF"
    val accessTokenSecret = "wgqpiQK1nbssqA6TAqKN3dYNwFxrKMzFLuhyoTXLwrzjE"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    //functions.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
  }

  def topHashTags(stream: ReceiverInputDStream[Status], seconds: Long) = {
    val hashTags: DStream[String] = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(seconds))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last %s seconds (%s total):".format(seconds, rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
  }


}
