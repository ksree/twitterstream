package com.kxs.de

import java.nio.file.Paths
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import twitter4j.Status

class TwitterAppTest extends FunSuite with BeforeAndAfter {

  var utility: HBaseTestingUtility = new HBaseTestingUtility
  val ColumnFamily_TW: Array[Byte] = "TW".getBytes

  val sparkConf = new SparkConf()
  sparkConf.setAppName("TwitterAppTest")
  sparkConf.setMaster("local[*]")

  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))
  TwitterApp.twitterAuth
  val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, Seq.empty[String])

  ignore("Twitter top 10 hastag") {
    val seconds: Long = 10
    TwitterApp.topHashTags(stream, seconds)

  }

  test("Test Hbase twitter data inserts") {

    val config: Configuration = HBaseConfiguration.create

    val hbaseconfig: String = Paths.get("target/test-classes/hbase-site.local.xml").toAbsolutePath.toString
    utility.getConfiguration.addResource(hbaseconfig)
    utility.getConfiguration.reloadConfiguration()
    utility.startMiniCluster
    utility.createTable(Bytes.toBytes("twitter"), ColumnFamily_TW)

    TwitterApp.writeToHBase(stream, utility.getConfiguration, nameSpace = "", tableName = "twitter")

    val resultScanner: ResultScanner = utility.getConnection.getTable(TableName.valueOf("twitter")).getScanner(ColumnFamily_TW)
    var resultIt: util.Iterator[Result] = resultScanner.iterator()
    while (resultIt.hasNext) {
      val result = resultIt.next();
      print(result);
    }
  }

  after {
    ssc.start()
    ssc.awaitTermination

  }


}
