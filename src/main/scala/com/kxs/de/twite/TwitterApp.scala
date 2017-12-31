package com.kxs.de.twite

import java.nio.charset.StandardCharsets.UTF_8

import com.kxs.de.twite.utils.functions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterApp {

  def main(args: Array[String]) {

    val tablename: String = "twitter_streams"
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)

    val jobConfig: JobConf = new JobConf()
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    twitterAuth

    val stream = TwitterUtils.createStream(ssc, None)
    val seconds: Long = 120

    topHashTags(stream, seconds)
    writeToHBase(stream, hconf, nameSpace = "datascience", tableName = "twitter")
    ssc.start()
    ssc.awaitTermination()

  }

  def twitterAuth: String = {
    // Configure Twitter credentials
    val consumerKey = "ELoC3AYmLefiCnytIfSxXceTm"
    val consumerSecret = "199X0943GBpG9KxsNwq5VWJjW2SV9xZd8HDdIBkyQUaSkQ55UY"
    val accessToken = "945338272720523264-SOJA1ZXykLPfTvi0wH1zpqg0UxvzTOF"
    val accessTokenSecret = "wgqpiQK1nbssqA6TAqKN3dYNwFxrKMzFLuhyoTXLwrzjE"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
  }

  def topHashTags(stream: ReceiverInputDStream[Status], seconds: Long): Unit = {
    val hashTags: DStream[String] = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topCounts: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(seconds))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(ascending = false))

    //Print popular hashtags
    topCounts.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last %s seconds (%s total):".format(seconds, rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
  }

  def writeToHBase(stream: ReceiverInputDStream[Status], hconfig: Configuration, nameSpace: String, tableName: String): Unit = {
    val data: DStream[Tweet] = stream.map(status => Tweet(status))

    data.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords =>
        partitionOfRecords.foreach(tweet => {
          println(s"${tweet.userName},\n ${tweet.hastags}, \n ${tweet.text},\n ${tweet.retweetCount},\n ${tweet.language}, \n ${tweet.streetName},\n ${tweet.country} \n ${tweet.createdAt}")
        }
        )
      )
    })

    //Save twitter data to HBase
    data.foreachRDD { rdd =>
      functions.bulkPut(rdd, nameSpace, tableName, (tweet: Tweet) => {
        val idBytes = Bytes.toBytes(tweet.hastags + "#" + tweet.createdAt)
        val hashtag: Array[Byte] = Bytes.toBytes(tweet.hastags)
        val text: Array[Byte] = Bytes.toBytes(tweet.text)
        val userName: Array[Byte] = Bytes.toBytes(tweet.userName)
        val lang: Array[Byte] = Bytes.toBytes(tweet.language)
        val stName: Array[Byte] = Bytes.toBytes(tweet.streetName)
        val country: Array[Byte] = Bytes.toBytes(tweet.country)
        val retweet: Array[Byte] = Bytes.toBytes(tweet.retweetCount)
        new Put(idBytes).addColumn("TW".getBytes(UTF_8), "HT".getBytes(UTF_8), hashtag)
        new Put(idBytes).addColumn("TW".getBytes(UTF_8), "txt".getBytes(UTF_8), text)
        new Put(idBytes).addColumn("TW".getBytes(UTF_8), "UNAME".getBytes(UTF_8), userName)
        new Put(idBytes).addColumn("TW".getBytes(UTF_8), "lng".getBytes(UTF_8), lang)
        new Put(idBytes).addColumn("TW".getBytes(UTF_8), "stnm".getBytes(UTF_8), stName)
        new Put(idBytes).addColumn("TW".getBytes(UTF_8), "cty".getBytes(UTF_8), country)
        new Put(idBytes).addColumn("TW".getBytes(UTF_8), "rt".getBytes(UTF_8), retweet)
      })
    }

  }
}
