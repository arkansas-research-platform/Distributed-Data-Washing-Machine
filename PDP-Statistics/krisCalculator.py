#!/usr/bin/env python
# coding: utf-8

# Start Spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
spark = SparkSession.builder.appName('Kris PDP Statistics').getOrCreate()
sc = spark.sparkContext
accum = sc.accumulator(0)

# Logfile 
log = open('PDP_Collector.txt', 'a')

# Read Input Data
delimiter = "|"
inputRDD = spark.sparkContext.textFile("hdfs://snodemain:9000/user/nick/HadoopDWM/job1_Tokens-Freq")    # Reading input file from HDFS directory
#inputRDD = spark.sparkContext.textFile("krisIn.txt")		# Reading input file from local fs
inpSplitRDD = inputRDD.map(lambda word:word.strip().split(delimiter))
filterRDD = inpSplitRDD.filter(lambda x:len(x) == 2)
freqValRDD = filterRDD.map(lambda x: int(x[1].strip()))
frqMean = freqValRDD.mean()
frqStdDev = freqValRDD.stdev()

# Accumulator (like custom counters in MR)
totalFreq = freqValRDD.foreach(lambda x: accum.add(x))
totalFreqValShow = accum.value


print('Mean of Token Frequencies: ', round(frqMean, 4), file=log)
print('Standard Dev of Token Frequencies: ', round(frqStdDev, 4), file=log)
log.close()
# 3. View or Save Results
#filterRDD.coalesce(1).saveAsTextFile("hdfs://snodemain:9000/user/nick/HadoopDWM/Spark-PDP")
#sortRDD.saveAsTextFile("hdfs://snodemain:9000/user/nick/HadoopDWM/Spark-PDP")   # Output to HDFS
#freqValRDD.coalesce(1).saveAsTextFile("krisOut.txt")   # Output to local fs


#spark.stop()  # Stop Spark and UI