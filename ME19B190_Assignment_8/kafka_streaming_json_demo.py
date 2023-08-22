# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "me19b190-ass8-topic"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
#create a spark session
    spark = SparkSession\
        .builder\
        .appName("ME19B190-Ass8-consumer")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Create DataFrame from stream of input lines from the localhost 9092
    records = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers)\
        .option('subscribe', kafka_topic_name)\
        .option("startingOffsets",'latest')\
        .load()
#count the number of rows in the read input with the desired window size
    windowedCounts = records.groupby(
           window(records.timestamp,'10 seconds','5 seconds')).count() 
    query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .option('truncate', 'false')\
	    .format('console')\
        .start()

    query.awaitTermination()