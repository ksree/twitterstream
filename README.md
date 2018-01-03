
# twitter-realtime

An spark streaming app that reads real time tweets and :     
    - Insert the top tweets in every 2mins into HBASE table 'tophashtags'   
    - insert all the tweets every 1 minute into HBASE table 'twitter'

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 
See deployment for notes on how to deploy the project on a live system.

### Prerequisites

Create a HDP cluster on AWS.

1. Follow the instructions provided in this link to launch cloud controller instance on your AWS, and create a HDP cluster(DEPLOYMENT OPTION #1: BASIC
) : http://hortonworks.github.io/hdp-aws/launch/index.html

2. Login to your Cloud Controller URL and create a Cluster

3. From the Horton Works Data Cloud homepage, open Ambari (Cluster UIs -> Ambari Web ). Add Service, and check HBase. Also make sure that Spark2 is checked. We dont need Spark 1.6 so uncheck it. Restart all the required services.

4. Setup twitter app credentials. Follow the instructions here
https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens

Get the bellow keys and set it in TwitterApp.scala
```
consumerKey 
consumerSecret 
accessToken 
accessTokenSecret
```

 
### Installing

Package the application
```
mvn clean package 
```

SFTP  twitterstream-1.0-SNAPSHOT-twitter-stream.tar.gz to the home directory of your master node.

SSH to your master node and untar the file copied above
```
ssh -i "*.pem" cloudbreak@**.***.***.***
tar -xvf twitterstream-1.0-SNAPSHOT-twitter-stream.tar.gz
```

Create HBase tables :
On one of your nodes, run the bellow commands to create two tables in HBASe:
```
hbase shell
create 'twitter', 'tw'
create 'tophashtags', 'c'

```
## Running the application

On the home directory of the master node, where you untar'd the file. Execute the following :

```
sudo chmod 775 -R twitter-stream
cd twitter-stream
./bin/run_twitter_stream.sh
```

This will start the spark streaming job, with a batch interval of 60 seconds.

To verify if the data is committed into hbase 
```
hbase shell
scan 'tophashtags'
scan 'twitter'
```

## TODO 

Index this data on Solr...