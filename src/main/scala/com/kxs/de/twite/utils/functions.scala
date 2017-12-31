package com.kxs.de.twite.utils

import java.io.{File, IOException}
import java.util.UUID

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap

object functions {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

  private val MAX_DIR_CREATION_ATTEMPTS = 10

  /** Configures the Oauth Credentials for accessing Twitter */
  def configureTwitterCredentials(apiKey: String, apiSecret: String, accessToken: String, accessTokenSecret: String) {
    val configs = new HashMap[String, String] ++= Seq(
      "apiKey" -> apiKey, "apiSecret" -> apiSecret, "accessToken" -> accessToken, "accessTokenSecret" -> accessTokenSecret)
    configs.foreach { case (key, value) =>
      if (value.trim.isEmpty) {
        throw new Exception("Error setting authentication - value for " + key + " not set")
      }
      val fullKey = "twitter4j.oauth." + key.replace("api", "consumer")
      System.setProperty(fullKey, value.trim)
    }
  }

  def bulkPut[T, U] (rdd: RDD[T],
                     namespace: String,
                     tableName: String,
                     convertToPut: (T) => Put) : Unit = {
    val hbaseConf = HBaseConfiguration.create
    //hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, s"$namespace:$tableName")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, s"$tableName")
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    val jobConfig = job.getConfiguration
    val tempDir = createDirectory()
    if(jobConfig.get("mapreduce.output.fileoutputformat.outputdir") == null) {
      jobConfig.set("mapreduce.output.fileoutputformat.outputdir", s"${tempDir.getPath}/outputDataset")
    }

    rdd.mapPartitions(
      it => it.map {
        r => (new ImmutableBytesWritable, convertToPut(r))
      }
    ).saveAsNewAPIHadoopDataset(jobConfig)
  }

  //use to create a dir
  def createDirectory(root: String = System.getProperty("java.io.tmpdir"), namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }

  def using[A <: {def close(): Unit }, B](param: A) (f: A => B) : B =
  try {
    f(param)
  } finally {
    param.close()
  }
}
