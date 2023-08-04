#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys 
import subprocess
import os
from operator import itemgetter
# sortedcontainers, source: https://grantjenks.com/docs/sortedcontainers/

##############################################################
#                     DEVELOPER's NOTES
#  Transitive Closure Reducer takes as input, a set of vertices
#  and edges the goal is to produce a star-like subgraphs by
#  iteratively assigning each vertex (v) with its snallest
#  neighbor.
##############################################################
groupKey = None                          
key = None 
groupValue = None
groupKeyList = None
runNextIteration=False
count = 0
proLoopCnt = 0
###### TRANSITIVE CLOSURE FUNCTION #####
def ConnectedComponentsMR(inputData):
    # START OF TRANSITIVE CLOSURE CONDITIONS
    groupKey = inputData[0]
    curr_KeySet = inputData[1][0]
    curr_valSet = inputData[1][1]
    firstCompKey = curr_KeySet[0]
    firstValue = curr_valSet[0].strip()
    lastValue = curr_valSet[-1].strip()
    groupSize = len(curr_valSet)
    #return(groupKey,lastValue)

    mStateCnt = 0
    lmStateCnt = 0
    clusListCnt = 0

    # Start with pairs in Descending order and has more than 1 value        
    if groupKey > firstValue:
        # Any time the mergeState is true/when we have to create chains, a counter is started
        # This counter is used by the Driver file to determine whether the TC process should 
        # be run for the next iteration or the programme should quit.

        # << CONDITION 2: Check if the length is greater than 1 >>
        if groupSize > 1:
            sizeList = []
            global runNextIteration
            global count
            global proLoopCnt
            # Create an additional Counter to be used for next iteration using stderr (this will be reported as 
            # part of MapReduce counters in the console)
            runNextIteration=True
            count+=1

            mStateCnt += 1
            lmStateCnt += 0

            # << Condition 2a: Generate chains from the values for that key group >>
            #return('****** Checking Group:', curr_KeySet, '***', curr_valSet, '******')  #Debug
            
            for i in range(len(curr_valSet)):
                x = firstValue
                y = curr_valSet[i]
                #return('     --Condition 2a fired with NEW PAIR **', '%s.%s,%s'%(x, y, y))
                #return('     --Condition 2a fired with NEW PAIR INVERSE **', '%s.%s,%s'%(y, x, x))
                newPair = '%s.%s,%s'%(x, y, y)    # New pair
                sizeList.append(newPair)
                proLoopCnt +=1
                newPairInverse = '%s.%s,%s'%(y, x, x)    # New pair inverse 
                sizeList.append(newPairInverse)
                proLoopCnt +=1  
            #return sizeList
            # << CONDITION 3: Check if first key of this group is less than last value of the group >>
            if groupKey < lastValue:
                #return('     --Condition 3a fired FIRST PAIR in group CARRIED OVER **', '%s,%s'%(firstCompKey, firstValue))
                firstPair = '%s,%s'%(firstCompKey, firstValue)
                sizeList.append(firstPair)
                proLoopCnt +=1
            return sizeList
    else:
        mStateCnt += 0
        lmStateCnt += 1
        #TransitiveClosureAccum += 1
        ascendingList = []
        # << CONDITION 1: check if key of key-group < firstValue of that group >>
        # << Condition 1a: Copy over key group(dont change anything) >>
        #print('****** Checking Group:', curr_KeySet, '***', curr_valSet, '******') #Debug
        for i in range(len(curr_KeySet)):
            #print('     --Condition 1a fired for this group with output **', curr_KeySet[i], curr_valSet[i])
            carryOver = '%s,%s'%(curr_KeySet[i], curr_valSet[i])
            ascendingList.append(carryOver)
            proLoopCnt +=1
            clusListCnt+= 1
            clusListCnt += 0
        return ascendingList
    return ('MergeStateCnt', mStateCnt, 'LocMaxStateCnt', lmStateCnt, 'ClusterListCnt', clusListCnt)
################ END OF FUNCTIONS ################