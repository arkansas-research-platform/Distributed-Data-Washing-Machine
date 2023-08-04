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
import SDWM050_SimilarityComparison
import SDWM055_TransitiveClosureCCMR
import StopWord
import DWM65_ScoringMatrixStd
import DWM66_ScoringMatrixKris
import textdistance
from pyspark import SparkConf
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# Initialize Spark
spark = SparkSession.builder.appName('Spark Data Washing Machine').getOrCreate()
sc = spark.sparkContext

###### TRANSITIVE CLOSURE FUNCTION #####
def ConnectedComponentsMR(inputData):
    global mergeStateAccum
    global locMaxStateAccum
    global clusterListAccum

    # Reset them to a value of '0' for each Transitive Closure Iteration
    mergeStateAccum += 0
    locMaxStateAccum += 0
    clusterListAccum += 0

    # START OF TRANSITIVE CLOSURE CONDITIONS
    groupKey = inputData[0]
    curr_KeySet = inputData[1][0]
    curr_valSet = inputData[1][1]
    firstCompKey = curr_KeySet[0]
    firstValue = curr_valSet[0].strip()
    lastValue = curr_valSet[-1].strip()
    groupSize = len(curr_valSet)
    #return(groupKey,lastValue)

    # Start with pairs in Descending order and has more than 1 value        
    if groupKey > firstValue:
        # Any time the mergeState is true/when we have to create chains, a counter is started
        # This counter is used by the Driver file to determine whether the TC process should 
        # be run for the next iteration or the programme should quit.

        #runNextIteration = "iterateNo"
        # << CONDITION 2: Check if the length is greater than 1 >>
        if groupSize > 1:
            sizeList = []
            # Create an additional Counter to be used for next iteration using stderr (this will be reported as 
            # part of MapReduce counters in the console)

            mergeStateAccum += 1
            locMaxStateAccum += 0

            # << Condition 2a: Generate chains from the values for that key group >>
            #return('****** Checking Group:', curr_KeySet, '***', curr_valSet, '******')  #Debug
            
            for i in range(len(curr_valSet)):
                x = firstValue
                y = curr_valSet[i]
                #return('     --Condition 2a fired with NEW PAIR **', '%s.%s,%s'%(x, y, y))
                #return('     --Condition 2a fired with NEW PAIR INVERSE **', '%s.%s,%s'%(y, x, x))
                newPair = '%s.%s,%s'%(x, y, y)    # New pair
                sizeList.append(newPair)
                newPairInverse = '%s.%s,%s'%(y, x, x)    # New pair inverse 
                sizeList.append(newPairInverse)
            #return sizeList
            # << CONDITION 3: Check if first key of this group is less than last value of the group >>
            if groupKey < lastValue:
                #return('     --Condition 3a fired FIRST PAIR in group CARRIED OVER **', '%s,%s'%(firstCompKey, firstValue))
                firstPair = '%s,%s'%(firstCompKey, firstValue)
                sizeList.append(firstPair)
            return sizeList
    else:
        mergeStateAccum += 0
        locMaxStateAccum += 1
        ascendingList = []
        # << CONDITION 1: check if key of key-group < firstValue of that group >>
        # << Condition 1a: Copy over key group(dont change anything) >>
        #print('****** Checking Group:', curr_KeySet, '***', curr_valSet, '******') #Debug
        for i in range(len(curr_KeySet)):
            #print('     --Condition 1a fired for this group with output **', curr_KeySet[i], curr_valSet[i])
            carryOver = '%s,%s'%(curr_KeySet[i], curr_valSet[i])
            ascendingList.append(carryOver)
            clusterListAccum += 1
            clusterListAccum += 0
        return ascendingList
    #return (mergeStateAccum, locMaxStateAccum)
################ END OF TRANSITIVE CLOSURE FUNCTIONS ################


# Creating LogFile
now = datetime.datetime.now()
tag = str(now.year)+(str(now.month)).zfill(2)+(str(now.day)).zfill(2)
tag = tag+'_'+(str(now.hour)).zfill(2)+'_'+(str(now.minute)).zfill(2)
logFile = open('SDWM_Log_'+tag+'.txt', 'w')

#=================== STARTING JOB =================== 
print('SPARK DATA WASHING MACHINE', file=logFile) 

#====== Read Parms file ====== 
DWM10_Parms.getParms('parmStage.txt')
hasHeader = DWM10_Parms.hasHeader
delimiter = DWM10_Parms.delimiter
tokenizerType = DWM10_Parms.tokenizerType 
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens
blockByPairs = DWM10_Parms.blockByPairs

#=================== PHASE 1: TOKENIZATION PROCESS =================== 
print('\n>> Starting Tokenization Process', file=logFile)

# Read Input Data
inputRDD = spark.sparkContext.textFile("inputStage.txt")
#inputRDD = spark.sparkContext.textFile("hdfs://snodemain:9000/user/nick/SparkDWM/inputStage.txt")    # Reading input file from HDFS directory

# ======= Job 1: Tokenize Input & Remove duplicates/not
tokenizedRef = inputRDD.map(SDWM010_Tokenization.TokenizationMapper).map(lambda x: [x[0], x[1:]])

if removeDuplicateTokens:
    cleanRefTokens = tokenizedRef.map(lambda x: [x[0], list(dict.fromkeys(x[1]))])
else:
    cleanRefTokens = tokenizedRef

# ======= Job 2: Frequency Generation
# Token Counts
tokenFrequencies = cleanRefTokens.flatMap(lambda line: line[1]) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

# ======= Job 3: Form MetaData Tag and Join with Token Frequencies
# Enumerate Tokens to preserve its positions
enumerateTokens = cleanRefTokens.flatMap((lambda x: ((y, x[0]) for y in list(enumerate(x[1],1)))))
# Build token metadata structure in the form (token, position, refID)
tokenMdataStructure = enumerateTokens.map(lambda x: (x[0][1], x[0][0], x[1])).map(lambda x: (x[0], x)) 
# Update metadata structure by joining structure & frequency info
joinMdataFreq = tokenMdataStructure.join(tokenFrequencies)

# ------------------- Job Statistics -----------------
inputRefsCnt = inputRDD.count()
tokenCnt = tokenizedRef.flatMap(lambda tok: tok[1]).count()
numTokensCnt = cleanRefTokens.flatMap(lambda tok: tok[1]).filter(lambda n: n.isdigit()).count()
remainToksCnt = cleanRefTokens.flatMap(lambda tok: tok[1]).count()
duplicateToksCnt = int(tokenCnt) - int(remainToksCnt)
uniqueToksCnt = tokenFrequencies.count()
print('  ----- Job Statistics ----- ', file=logFile)
print('    Total References Read:', inputRefsCnt, file=logFile)
print('    Total Tokens Found:', tokenCnt, file=logFile)
print('    Total Numeric Tokens:', numTokensCnt, file=logFile)
print('    Remaining Tokens:', remainToksCnt, file=logFile)
print('    Duplicate Tokens:', duplicateToksCnt, file=logFile)
print('    Unique Tokens:', uniqueToksCnt, file=logFile)
#==============================================================================

#=================== PHASE 2: REFERENCE REFORMATION PROCESS =================== 
# ======= Job 4: Re-create original refs using tokens & token position in each refID
regroupTokens = joinMdataFreq.map(lambda x: (x[1][0][2], (x[1][0][1], x[0], x[1][1]))) \
                                .reduceByKey(lambda a, b: str(a)+'-'+str(b)) 
reformedCleanRefs = regroupTokens.map(lambda f: (f[0], {int(str(k).replace('(','')):v.strip().replace(')','').replace("'","").replace(',', '^') \
                                        for k,v in (x.split(',', maxsplit=1) \
                                        for x in str(f[1]).split('-'))})) \
                                .map(lambda d: (d[0], dict(sorted(d[1].items(), key=lambda item: item[0]))))
#==============================================================================

#=================== PHASE 3: BLOCKING PROCESS =================== 
print('\n>> Starting Blocking Process', file=logFile)

# ======= Job 5a: Decide Blocking Tokens from each reference 
qualifiedTokens = reformedCleanRefs.map(SDWM025_Blocking.BlockTokenPairMap)
refTokensLeft = qualifiedTokens.filter(lambda x:len(x[1]) > 0) #Exclude all empty lists 

# ======= Job 5b: Form Block Key pairs from the blocking tokens 
if blockByPairs:
    cleanToks4BlckKeys = refTokensLeft.filter(lambda x:len(x[1]) > 1)
    #buildBlockingKeys = cleanToks4BlckKeys.flatMap(SDWM025_Blocking.byPairs).sortBy(lambda x:x[0])
    buildBlockingKeys = cleanToks4BlckKeys.map(lambda x: ([(str(x[1][a])+str(x[1][b]), x[0]) \
                                        if x[1][a]<x[1][b] \
                                        else (str(x[1][b])+str(x[1][a]), x[0]) \
                                        for a in range(0, len(x[1])-1) \
                                            for b in range(a+1, len(x[1]))])) \
                                    .flatMap(lambda x:x)
else:
    #buildBlockingKeys = refTokensLeft.flatMap(SDWM025_Blocking.bySingles).sortBy(lambda x:x[0])
    buildBlockingKeys = refTokensLeft.map(lambda x: ([(x[1][a], x[0])
                                        for a in range(0, len(x[1]))])) \
                                    .flatMap(lambda x:x)

# ======= Job 5c: Grouping values from each key group 
#blockKeyGroup = buildBlockingKeys.map(lambda x: (x.split(':')))  \
#             .map(lambda word: (word[0], word[1])) \
#             .reduceByKey(lambda a, b: a+','+b)
blockKeyGroup = buildBlockingKeys.reduceByKey(lambda a, b: a+','+b)

# ======= Job 5d: Creating refID pairs from elements in each key group 
# Option 1: Using SDWM025 module
#dupBlockPairList = blockKeyGroup.flatMap(SDWM025_Blocking.refIDPairing)
# Option 2
# Due to Pairwise Comparison, we need all refIDList with len > 1
refIDsFilter = blockKeyGroup.map(lambda x:x[1].split(',')).filter(lambda x:len(x)>1) 
dupBlockPairList = refIDsFilter.map(lambda x: ([(str(x[a])+':'+str(x[b]), 1) \
                                        if x[a]<x[b] \
                                        else (str(x[b])+':'+str(x[a]), 1) \
                                        for a in range(0, len(x)-1) \
                                            for b in range(a+1, len(x))])) \
                                .flatMap(lambda x:x)

# ======= Job 5d: Deduplicate refID pairs
uniqueBlockPairList = dupBlockPairList.sortBy(lambda x:x[0:]).reduceByKey(add)

# ======= Job 5e: Join Block Pairs with Reformed Ref for Metadata Info
breakPairs = uniqueBlockPairList.map(lambda x: (x[0].replace(':',''), x[0].split(':'))) \
                                .flatMap(lambda x: ((y, x[0]) for y in x[1])) # Breakdown each key ((x:y) --> <x, xy>, <y, xy>)
joinPairsMdata = breakPairs.join(reformedCleanRefs) # Join metadata with each refID
pairsWithMetadata = joinPairsMdata.map(lambda x: (x[1][0], (str(x[0])+'-'+str(x[1][1])))).sortByKey() \
                            .reduceByKey(lambda x,y: (x+'<>'+y) if x<y else (y+'<>'+x)) \
                            .map(lambda x:x[1])  #Note: .map - arranges join info, .reduceByKey - brings pairs back, .map - extracts only refID:{metadata Tag} 

# ------------------- Job Statistics -----------------
print('  ----- Job Statistics ----- ', file=logFile)
print('    Total References Selected for Reprocessing:', qualifiedTokens.count(), file=logFile)
print('    Total Records Excluded:', (qualifiedTokens.count() - refTokensLeft.count()), file=logFile)
print('    Total Records Left for Blocks Creation:', refTokensLeft.count(), file=logFile)
print('    Blocking Records Created:', buildBlockingKeys.count(), file=logFile)
print('    Total Pairs Generated by Blocks:', dupBlockPairList.count(), file=logFile)
print('    Total Unduplicated Blocks:', uniqueBlockPairList.count(), file=logFile)
#==============================================================================

#=================== PHASE 4: SIMILARITY COMPARISON PROCESS =================== 
print('\n>> Starting Similarity Comparison Process', file=logFile)
#prepRefPair = pairsWithMetadata.map(lambda x: x.split('<>'))
    #linkedPairs = '%s.%s,%s' % (refID1,refID2,refID2) # Original Linked Pairs 
    #inversedLinkedPairs = '%s.%s,%s' % (refID2, refID1,refID1) # Inverted Linked Pairs
    #pairSelf = '%s.%s,%s' % (refID1,refID1,refID1) # PairSelf
linkPairs = pairsWithMetadata.map(SDWM050_SimilarityComparison.similarPairs) \
                                    .filter(lambda x: x != None)
links = linkPairs.map(lambda x: (x.split(',')[0].strip() + '.'+ x.split(',')[1].strip(), x.split(',')[1].strip()))
inversedLinkedPairs = linkPairs.map(lambda x: (x.split(',')[1].strip() + '.'+ x.split(',')[0].strip(), x.split(',')[0].strip()))
pairSelf = linkPairs.map(lambda x: (x.split(',')[0].strip() + '.'+ x.split(',')[0].strip(), x.split(',')[0].strip()))
unionPairsForTC = links.union(inversedLinkedPairs).union(pairSelf)
#==============================================================================
# ------------------- Job Statistics -----------------
print('  ----- Job Statistics ----- ', file=logFile)
print('    Number of Pairs Linked:', linkPairs.count(), file=logFile)
#==============================================================================

#=================== PHASE 5: TRANSITIVE CLOSURE PROCESS =================== 
print('\n>> Starting Transitive Closure Process', file=logFile)
print('  ----- Iteration Statistics ----- ', file=logFile)

transitiveClosureIn = unionPairsForTC
transitiveClosureIn.persist()     # Caching will prevent the Transitive Closure RDD from starting from the entire program in each iteration. Can also use .cache()
transClosureCounter = 9999      # Default value to always start the first TC iteration
transitiveClosIterationCnt = 0  # Counter for Transitive Closure iterations

while transClosureCounter > 0:
    mergeStateAccum = sc.accumulator(0)
    locMaxStateAccum = sc.accumulator(0)
    clusterListAccum = sc.accumulator(0)

    print('  == Computing Transitive Closure at Counter ===> ', transClosureCounter, file=logFile)
    print('     PairIn Count:', transitiveClosureIn.count(), file=logFile)

    keySet = transitiveClosureIn.map(lambda x: (x[0].split('.')[0], x[0])) \
                            .reduceByKey(lambda x,y:(x+','+y)) \
                            .map(lambda x: (x[0], sorted(list(set([v.strip() for v in x[1].split(',')])))))
    valueSet = transitiveClosureIn.map(lambda x: (x[0].split('.')[0], x[1])) \
                            .reduceByKey(lambda x,y:(x+','+y)) \
                            .map(lambda x: (x[0], sorted(list(set([v.strip() for v in x[1].split(',')])))))
    keyValueSet = keySet.union(valueSet).reduceByKey(lambda x,y: [x, y]).sortByKey()

    ##transitiveClosure = keyValueSet.map(SDWM055_TransitiveClosureCCMR.ConnectedComponentsMR) 
    ##counterLines = transitiveClosure.filter(lambda x: x[1] == 'MergeStateCnt')# \
    ###                                .flatMap(lambda x: (y for y in x))
    ##transitiveClosurePairs = transitiveClosure.filter(lambda x: x != None) \
    ##                                        .flatMap(lambda x: (y for y in x))

    transitiveClosureOut = keyValueSet.map(ConnectedComponentsMR) \
                                    .filter(lambda x: x != None) \
                                    .flatMap(lambda x: tuple(y for y in x)) \
                                    .map(lambda x:tuple(x.split(',')))

    transitiveClosureIn.unpersist()      # unpersist transitiveClosureIn since it will be overwritten for next iteration
    transitiveClosureOut.persist()

    print('     PairOut Count:', transitiveClosureOut.count(), file=logFile)
    print('     MergeState Counter:', mergeStateAccum.value, file=logFile)
    print('     Local Max State Counter:', locMaxStateAccum.value, file=logFile)
    print('     Cluster List Counter:', clusterListAccum.value, file=logFile)
    transClosureCounter = mergeStateAccum.value

    transitiveClosureIn = transitiveClosureOut      # Update transitiveClosureIn for the next iteration

    transitiveClosIterationCnt +=1

finalTransitiveClosure = transitiveClosureOut

#==============================================================================
# ------------------- Job Statistics -----------------
print('\n  ----- Job Statistics ----- ', file=logFile)
print('    Total Transitive Closure Iterations:', transitiveClosIterationCnt, file=logFile)
print('    Size of Cluster List Formed from TC:', finalTransitiveClosure.count(), file=logFile)
#==============================================================================


#=================== FINAL OUTPUT =================== 
unionPairsForTC.coalesce(1).saveAsTextFile("SDWM-Out")   # Output to local fs
#checkRDD.coalesce(1).saveAsTextFile("hdfs://snodemain:9000/user/nick/SparkDWM/SDWM-Out")   # Output to HDFS

logFile.close()