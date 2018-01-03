package com.kxs.de

import java.nio.charset.StandardCharsets.UTF_8

import com.kxs.de.utils.functions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterApp {

  def main(args: Array[String]) {

    val hconf: Configuration = HBaseConfiguration.create()
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    twitterAuth

    val stream = TwitterUtils.createStream(ssc, None)
    val seconds: Long = 120

    topHashTags(stream, seconds, hconf)
    writeToHBase(stream, nameSpace = "", tableName = "twitter")
    ssc.start()
    ssc.awaitTermination()

  }

  def twitterAuth: String = {
    // Add your Twitter credentials
    val consumerKey = "******"
    val consumerSecret = "******"
    val accessToken = "*******"
    val accessTokenSecret = "********"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
  }

  def topHashTags(stream: ReceiverInputDStream[Status], seconds: Long, hbaseConf: Configuration): Unit = {
    val hashTags: DStream[String] = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topCounts: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(seconds))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(ascending = false))

    //Print popular hashtags
    topCounts.foreachRDD((rdd, time) => {
      val connection = ConnectionFactory.createConnection(hbaseConf)
      val topList: Array[(Int, String)] = rdd.take(10)

      //println("\nPopular topics in last %s seconds (%s total):".format(seconds, rdd.count()))

      topList.foreach { case (count: Int, hashTag: String) =>
        val table = connection.getTable(TableName.valueOf(Bytes.toBytes("tophashtags")))
        var put = new Put(Bytes.toBytes(hashTag + "#" + time.toString()))
        put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("cnt"), Bytes.toBytes(count.toString))

        table.put(put)
        table.close()
      }
      connection.close()
    }
    )
  }

  def writeToHBase(stream: ReceiverInputDStream[Status], nameSpace: String, tableName: String): Unit = {
    val data: DStream[Tweet] = stream.map(status => Tweet(status))

    //Save twitter data to HBase
    data.foreachRDD { rdd =>
      functions.bulkPut(rdd, nameSpace, tableName, (tweet: Tweet) => {
        val idBytes = Bytes.toBytes(tweet.userName + "#" + tweet.createdAt)
        val hashtag: Array[Byte] = Bytes.toBytes(tweet.hastags)
        val text: Array[Byte] = Bytes.toBytes(tweet.text)
        val userName: Array[Byte] = Bytes.toBytes(tweet.userName)
        val lang: Array[Byte] = Bytes.toBytes(tweet.language)
        val country: Array[Byte] = Bytes.toBytes(tweet.country)
        val retweet: Array[Byte] = Bytes.toBytes(tweet.retweetCount)
        new Put(idBytes).addColumn("tw".getBytes(UTF_8), "ht".getBytes(UTF_8), hashtag)
          .addColumn("tw".getBytes(UTF_8), "txt".getBytes(UTF_8), text)
          .addColumn("tw".getBytes(UTF_8), "uname".getBytes(UTF_8), userName)
          .addColumn("tw".getBytes(UTF_8), "lng".getBytes(UTF_8), lang)
          .addColumn("tw".getBytes(UTF_8), "cty".getBytes(UTF_8), country)
          .addColumn("tw".getBytes(UTF_8), "rt".getBytes(UTF_8), retweet)
      })
    }

  }
}
