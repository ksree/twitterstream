#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
APP_HOME=$(dirname "$SCRIPTPATH")

export SPARK_MAJOR_VERSION=2

readonly HBASE_CLASSPATH_ALL=`hbase classpath`
readonly REMOVE_CLASSPATH=""
readonly HBASE_CLASSPATH=${HBASE_CLASSPATH_ALL/$REMOVE_CLASSPATH/}
readonly APP_JARS=$( echo $HBASE_CLASSPATH | tr ':' ','),$(echo ${APP_HOME}/lib/* | tr ' ' ',')

spark-submit  --class com.kxs.de.TwitterApp \
              --jars $APP_JARS \
             /home/cloudbreak/twitter-stream/lib/twitterstream-1.0-SNAPSHOT.jar
