# -*- coding: utf-8 -*-
"""Untitled34.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/12lPcj-Mz1MnMNIOHk-XpXgCnxEs2IMyk
"""

import pyspark
import sys

def return_group(x):
    sen = x.split(",")         #Takes the entire line in the form of date,time,userid and splits using ','
    t = sen[1]			#extracts the value of time in the line
    hour = int(str(t).split(':')[0])    #Time is in HH:mm format so splitting the hours and minutes attributes
    minute = int(str(t).split(':')[1])
    hour = hour + (minute/60)       #Converting from HH:mm format to HH (hour) format only
    if hour>0 and hour<=6:
      return '0-6'
    if hour>6 and hour<=12:
      return '6-12'
    if hour>12 and hour<=18:		#returning the group based on condition 
      return '12-18'
    if (hour>18 and hour<=24) or hour ==0 :
      return '18-24'

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")       #Expection that raises when input arguments are not in the form 
									        	#__,input,output

inputURL=sys.argv[1]    #reading the input path
outputURL=sys.argv[2]   #reading the output path

sc = pyspark.SparkContext()  
df = sc.textFile(inputURL)   #reading the input file as text
header = df.first()         # getting the header of the input file
df  = df.filter(lambda line: line != header)  #it is important to delete the header as it contains labels but not data

groups = df.map(return_group)    #apply map operation
output = groups.map(lambda element: (element, 1)).reduceByKey(lambda c1, c2: c1 + c2)  #reduce operation

output.saveAsTextFile(outputURL)   #writing to an output file path