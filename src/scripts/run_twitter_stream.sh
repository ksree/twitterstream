#!/bin/bash

readonly SPARK_MAJOR_VERSION=2
readonly HBASE_CLASSPATH_ALL=`hbase classpath`
readonly REMOVE_CLASSPATH=""
readonly HBASE_CLASSPATH=${HBASE_CLASSPATH_ALL/$REMOVE_CLASSPATH/}

spark-submit --driver-class-path $HBASE_CLASSPATH \
             --class com.kxs.de.twite.TwitterApp \
             twitterstream-1.0-SNAPSHOT-jar-with-dependencies.jar

