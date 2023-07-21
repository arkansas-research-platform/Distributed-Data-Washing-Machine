#!/usr/bin/env python
# coding: utf-8

# Start Spark
import os
import sys
import re
import time
import datetime
import DWM10_Parms
import SDWM010_Tokenization
import SDWM025_BlockTokenPair
from pyspark import SparkConf
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# Parameters
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens

# Creating LogFile
now = datetime.datetime.now()
tag = str(now.year)+(str(now.month)).zfill(2)+(str(now.day)).zfill(2)
tag = tag+'_'+(str(now.hour)).zfill(2)+'_'+(str(now.minute)).zfill(2)
logFile = open('SDWM_Log_'+tag+'.txt', 'w')

# Initialize Spark
spark = SparkSession.builder.appName('Spark Data Washing Machine').getOrCreate()
sc = spark.sparkContext
#accum = sc.accumulator(0)

print('SPARK DATA WASHING MACHINE', file=logFile) 

###################### PHASE 1: TOKENIZATION PROCESS ######################
print('\n>> Starting Tokenization Process', file=logFile)

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


# Tokenize input, remove duplicates/not (Serves as Reformed References)
inpSplitRDD_ReformedRefs = inputRDD.map(SDWM010_Tokenization.TokenizationMapper).map(lambda x: [x[0], x[1:]])

if removeDuplicateTokens:
    cleanRef = inpSplitRDD_ReformedRefs.map(lambda x: [x[0], list(dict.fromkeys(x[1]))])
else:
    cleanRef = inpSplitRDD_ReformedRefs

# Token Counts
tokenFrequencies = cleanRef.flatMap(lambda line: line[1]) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

    # Token Positions, RefID, token itself
pairs = cleanRef.flatMap((lambda x: ((y, x[0]) for y in list(enumerate(x[1],1)))))
    # Rearrange in the form (token, position, refID)
pairStr = pairs.map(lambda x: (x[0][1], x[0][0], x[1])) 
    # Rearrange in the form (token, position, refID)
kvPairs = pairStr.map(lambda x: (x[0], x)) 
    # Join kvPairs and Frequencies
kvPairFreqJoin = kvPairs.join(tokenFrequencies)#.collect()

# ------------------- Job Statistics -----------------
inputRefs = inputRDD.count()
tokens = inpSplitRDD_ReformedRefs.flatMap(lambda tok: tok[1]).count()
numTokens = cleanRef.flatMap(lambda tok: tok[1]).filter(lambda n: n.isdigit()).count()
remainToks = cleanRef.flatMap(lambda tok: tok[1]).count()
duplicateToks = int(tokens) - int(remainToks)
uniqueToks = tokenFrequencies.count()
print('    Total References Read:', inputRefs, file=logFile)
print('    Total Tokens Found:', tokens, file=logFile)
print('    Total Numeric Tokens:', numTokens, file=logFile)
print('    Remaining Tokens:', remainToks, file=logFile)
print('    Duplicate Tokens:', duplicateToks, file=logFile)
print('    Unique Tokens:', uniqueToks, file=logFile)

###################### PHASE 2: REFERENCE REFORMATION PROCESS ######################
#reformedRefs = cleanRef
repositionTokns = kvPairFreqJoin.map(lambda x: (x[1][0][2], (x[1][0][1], x[0], x[1][1])))
reformedRefs = repositionTokns.reduceByKey(lambda a, b: str(a)+'-'+str(b)) #.replace(',', '^', 1)
#finalDict = reformedRefs.map(lambda f: (f[0], {k:v for k,v in (x.split(',') for x in str(f[1]).split('-'))}))
tokDict = reformedRefs.map(lambda f: (f[0], {int(str(k).replace('(','')):v.strip().replace(')','').replace("'","") \
                                        for k,v in (x.split(',', maxsplit=1) for x in str(f[1]).split('-'))}))
sortedTokDict = tokDict.map(lambda d: (d[0], dict(sorted(d[1].items(), key=lambda item: item[0]))))

###################### PHASE 3: BLOCKING PROCESS ######################
#blckTokList = kvPairFreqJoin.map(SDWM025_BlockTokenPair.BlockTokenPairMap)







######################################################################
#{'refID':value, 'pos': n, 'tok': key}
#checkRDD = inpSplitRDD.flatMap(lambda xs: [(x, 1) for x in xs])

#checkRDD = inpSplitRDD.flatMap(SDWM010_Tokenization.tokenRefIDmapping)
#filterRDD = inpSplitRDD.filter(lambda x:len(x) == 2)
#freqValRDD = filterRDD.map(lambda x: int(x[1].strip()))

# View or Save Results
sortedTokDict.coalesce(1).saveAsTextFile("SDWM-Out")   # Output to local fs
#checkRDD.coalesce(1).saveAsTextFile("hdfs://snodemain:9000/user/nick/SparkDWM/SDWM-Out")   # Output to HDFS


logFile.close()
#spark.stop()  # Stop Spark and UI