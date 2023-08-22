# -*- coding: utf-8 -*-


from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import csv
from google.cloud import storage

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("pysparkdf").getOrCreate()   #create a spark session

KAFKA_TOPIC_NAME_CONS = "me19b190-ass8-topic"  #topic name
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092' #local host 

if __name__ == "__main__":
	storage_client = storage.Client()
	bucket = storage_client.get_bucket('me19b190-bucket-ass8')
	blob = bucket.blob('Customers.csv')
	records_str = blob.download_as_string().decode('utf-8').split('\n')
	records_str = list(csv.reader(records_str, delimiter=","))
	data = records_str[1:-1]               #construct the dataframe
	header = records_str[0]                #header of the dataframe
	df = spark.createDataFrame(data, header)
	df.show(10)
	num_records_terminate = 100     #termination number of records = 100
	print("Kafka Producer Application Started ... ")
	#create the producer object
	kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,value_serializer=lambda x: dumps(x).encode('utf-8')) 
	df = df.withColumn("CustomerID", df['CustomerID'].cast("Integer"))
	i = 11
	while i <= num_records_terminate+1:
		#collect batches of 10 records each and push them to topic
		print("Current records: ")
		records = (df.filter((df.CustomerID >= i - 10) & (df.CustomerID < i)))
		records.show()
		for row in records.rdd.collect():
			kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, row)
		#increment i
		i = i+10
		time.sleep(10) #sleep time of the producer, send data after a wait time of 10 seconds

	print("Kafka Producer Application Completed. ")
	kafka_producer_obj.close()