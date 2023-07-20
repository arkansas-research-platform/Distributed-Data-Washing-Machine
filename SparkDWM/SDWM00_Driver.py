#!/usr/bin/env python
# coding: utf-8

# Start Spark
import os
import sys
import re
import DWM10_Parms
import SDWM010_Tokenization
from pyspark import SparkConf
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# Initialize Spark
spark = SparkSession.builder.appName('Spark Data Washing Machine').getOrCreate()

###################### PHASE 1: TOKENIZATION PROCESS ######################
# Read Parms file
#parmfile = SparkFiles.get(('S1G-parms-copy.txt'))
DWM10_Parms.getParms('S8P-parms-copy.txt')

# Get Parameter values
hasHeader = DWM10_Parms.hasHeader
delimiter = DWM10_Parms.delimiter
tokenizerType = DWM10_Parms.tokenizerType
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens

# Read Input Data
inputRDD = spark.sparkContext.textFile("S8P-copy.txt")
#inputRDD = spark.sparkContext.textFile("hdfs://snodemain:9000/user/nick/SparkDWM/S8P-copy.txt")    # Reading input file from HDFS directory

# Tokenize input, remove duplicates/not
inpSplitRDD = inputRDD.map(SDWM010_Tokenization.TokenizationMapper)

# Token Counts
tokenFrequencies = inpSplitRDD.flatMap(lambda line: line) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

# Token Positions, RefID, token itself
rowDict = {}
pairs = inpSplitRDD.flatMap((lambda x: ((y, x[0]) for y in list(enumerate(x[1:],1)))))
pairStr = pairs.map(lambda x: (x[0][1], x[0][0], x[1])) # Rearrange in the form (token, position, refID)
kvPairs = pairStr.map(lambda x: (x[0], x)) # Rearrange in the form (token, position, refID)

# Join kvPairs and Frequencies
kvPairFreqJoin = kvPairs.join(tokenFrequencies)#.collect()


#{'refID':value, 'pos': n, 'tok': key}
#checkRDD = inpSplitRDD.flatMap(lambda xs: [(x, 1) for x in xs])

#checkRDD = inpSplitRDD.flatMap(SDWM010_Tokenization.tokenRefIDmapping)
#filterRDD = inpSplitRDD.filter(lambda x:len(x) == 2)
#freqValRDD = filterRDD.map(lambda x: int(x[1].strip()))

# 3. View or Save Results
kvPairFreqJoin.coalesce(1).saveAsTextFile("SDWM-Out")   # Output to local fs
#checkRDD.coalesce(1).saveAsTextFile("hdfs://snodemain:9000/user/nick/SparkDWM/SDWM-Out")   # Output to HDFS

#spark.stop()  # Stop Spark and UI