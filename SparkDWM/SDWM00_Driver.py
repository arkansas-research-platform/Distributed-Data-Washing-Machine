#!/usr/bin/env python
# coding: utf-8

# Start Spark
import os
import sys
import re
import time
import datetime
from operator import add
import DWM10_Parms
import SDWM010_Tokenization
import SDWM025_Blocking
from pyspark import SparkConf
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# Initialize Spark
spark = SparkSession.builder.appName('Spark Data Washing Machine').getOrCreate()
sc = spark.sparkContext
#accum = sc.accumulator(0)

# Creating LogFile
now = datetime.datetime.now()
tag = str(now.year)+(str(now.month)).zfill(2)+(str(now.day)).zfill(2)
tag = tag+'_'+(str(now.hour)).zfill(2)+'_'+(str(now.minute)).zfill(2)
logFile = open('SDWM_Log_'+tag+'.txt', 'w')

#=================== STARTING JOB =================== 
# Parameters
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens
blockByPairs = DWM10_Parms.blockByPairs

print('SPARK DATA WASHING MACHINE', file=logFile) 

#====== Read Parms file ====== 
DWM10_Parms.getParms('parmStage.txt')
hasHeader = DWM10_Parms.hasHeader
delimiter = DWM10_Parms.delimiter
tokenizerType = DWM10_Parms.tokenizerType
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens

#=================== PHASE 1: TOKENIZATION PROCESS =================== 
print('\n>> Starting Tokenization Process', file=logFile)

# Read Input Data
inputRDD = spark.sparkContext.textFile("inputStage.txt")
#inputRDD = spark.sparkContext.textFile("hdfs://snodemain:9000/user/nick/SparkDWM/inputStage.txt")    # Reading input file from HDFS directory

# ======= Job 1: Tokenize Input & Remove duplicates/not
inpSplitRDD_ReformedRefs = inputRDD.map(SDWM010_Tokenization.TokenizationMapper).map(lambda x: [x[0], x[1:]])

if removeDuplicateTokens:
    cleanRef = inpSplitRDD_ReformedRefs.map(lambda x: [x[0], list(dict.fromkeys(x[1]))])
else:
    cleanRef = inpSplitRDD_ReformedRefs

# ======= Job 2: Frequency Generation
# Token Counts
tokenFrequencies = cleanRef.flatMap(lambda line: line[1]) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

# ======= Job 3: Form MetaData Tag and Join with Token Frequencies
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


#=================== PHASE 2: REFERENCE REFORMATION PROCESS =================== 
# ======= Job 4: Re-create original refs using tokens & token position in each refID
repositionTokns = kvPairFreqJoin.map(lambda x: (x[1][0][2], (x[1][0][1], x[0], x[1][1])))
reformedRefs = repositionTokns.reduceByKey(lambda a, b: str(a)+'-'+str(b)) 
tokDict = reformedRefs.map(lambda f: (f[0], {int(str(k).replace('(','')):v.strip().replace(')','').replace("'","").replace(',', '^') \
                                        for k,v in (x.split(',', maxsplit=1) for x in str(f[1]).split('-'))}))
sortedTokDict = tokDict.map(lambda d: (d[0], dict(sorted(d[1].items(), key=lambda item: item[0]))))


#=================== PHASE 3: BLOCKING PROCESS =================== 
# ======= Job 5a: Decide Blocking Tokens from each reference 
blckTokList = sortedTokDict.map(SDWM025_Blocking.BlockTokenPairMap)
blckTokListFilter = blckTokList.filter(lambda x:len(x[1]) > 0) #Exclude all empty lists 

# ======= Job 5b: Form Block Key pairs from the blocking tokens 
if blockByPairs:
    keyPairFilter = blckTokListFilter.filter(lambda x:len(x[1]) > 1)
    keyPairs = keyPairFilter.flatMap(SDWM025_Blocking.byPairs).sortBy(lambda x:x[0])
    #groupKeyPairs = keyPairs.groupByKey().mapValues(list)
else:
    keyPairs = blckTokListFilter.flatMap(SDWM025_Blocking.bySingles).sortBy(lambda x:x[0])
    #groupKeyPairs = keyPairs.groupByKey().mapValues(list)

# ======= Job 5c: Grouping values from each key sgroup 
IDpair = keyPairs.map(lambda x: (x.split(':')))  \
             .map(lambda word: (word[0], word[1])) \
             .reduceByKey(lambda a, b: a+','+b)

# ======= Job 5d: Creating refID pairs from elements in each key group 
# Option 1: Using SDWM025 module
#refIDPairs = IDpair.flatMap(SDWM025_Blocking.refIDPairing)
# Option 2
refIDFilt = IDpair.map(lambda x:x[1].split(',')).filter(lambda x:len(x)>1)
nestRefIDList = refIDFilt.map(lambda x: ([(str(x[a])+':'+str(x[b]), 1) \
                                        if x[a]<x[b] \
                                        else (str(x[b])+':'+str(x[a]), 1) \
                                        for a in range(0, len(x)-1) \
                                            for b in range(a+1, len(x))]))
refIDPairs = nestRefIDList.flatMap(lambda x:x)

# ======= Job 5d: Deduplicate refID pairs
unduplicateRefIDPair = refIDPairs.sortBy(lambda x:x[0:]).reduceByKey(add)

# ======= Job 5e: Join Block Pairs with Reformed Ref for Metadata Info
breakPairs = unduplicateRefIDPair.map(lambda x: (x[0].replace(':',''), x[0].split(':'))) \
                                .flatMap(lambda x: ((y, x[0]) for y in x[1])) # Breakdown each key ((x:y) --> <x, xy>, <y, xy>)
joinIDnMdata = breakPairs.join(sortedTokDict) # Join metadata with each refID
finalBlockPairs = joinIDnMdata.map(lambda x: (x[1][0], (str(x[0])+'-'+str(x[1][1])))) \
                            .reduceByKey(lambda x,y: x+' <> '+y) \
                            .map(lambda x:x[1])  #Note: .map - arranges join info, .reduceByKey - brings pairs back, .map - extracts only refID:{metadata Tag} 

# ------------------- Job Statistics -----------------
print('\n>> Starting Blocking Process', file=logFile)
print('    Total References Selected for Reprocessing:', blckTokListFilter.count(), file=logFile)
print('    Blocking Records Created:', keyPairs.count(), file=logFile)
print('    Total Pairs Generated by Blocks:', refIDPairs.count(), file=logFile)
print('    Total Unduplicated Blocks:', unduplicateRefIDPair.count(), file=logFile)


#=================== PHASE 4: SIMILARITY COMPARISON PROCESS =================== 



#=================== FINAL OUTPUT =================== 
refIDPairs.coalesce(1).saveAsTextFile("SDWM-Out")   # Output to local fs
#checkRDD.coalesce(1).saveAsTextFile("hdfs://snodemain:9000/user/nick/SparkDWM/SDWM-Out")   # Output to HDFS

logFile.close()