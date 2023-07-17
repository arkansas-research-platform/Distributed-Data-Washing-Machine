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
def ConnectedComponentsMR(curr_KeySet, curr_valSet):
    # START OF TRANSITIVE CLOSURE CONDITIONS
    firstCompKey = curr_KeySet[0]
    firstValue = curr_valSet[0].strip()
    lastValue = curr_valSet[-1].strip()
    groupSize = len(curr_valSet)
    #print(groupKey,lastValue)

    # Start with pairs in Descending order and has more than 1 value        
    if groupKey > firstValue:
        # Any time the mergeState is true/when we have to create chains, a counter is started
        # This counter is used by the Driver file to determine whether the TC process should 
        # be run for the next iteration or the programme should quit.

        #runNextIteration = "iterateNo"
        # << CONDITION 2: Check if the length is greater than 1 >>
        if groupSize > 1:
            global runNextIteration
            global count
            global proLoopCnt
            # Create an additional Counter to be used for next iteration using stderr (this will be reported as 
            # part of MapReduce counters in the console)
            runNextIteration=True
            count+=1
            # Reporting to MapReduce Counter
            sys.stderr.write("reporter:counter:Transitive Closure Counters,Merge State,1\n")
            sys.stderr.write("reporter:counter:Transitive Closure Counters,Local Max State,0\n")

            # << Condition 2a: Generate chains from the values for that key group >>
            #print('****** Checking Group:', curr_KeySet, '***', curr_valSet, '******')  #Debug
            
            for i in range(len(curr_valSet)):# - 1):
                x = firstValue
                y = curr_valSet[i]
                #print('     --Condition 2a fired with NEW PAIR **', '%s.%s,%s'%(x, y, y))
                #print('     --Condition 2a fired with NEW PAIR INVERSE **', '%s.%s,%s'%(y, x, x))
                print('%s.%s,%s'%(x, y, y))     # New pair
                proLoopCnt +=1
                print('%s.%s,%s'%(y, x, x))     # New pair inverse 
                proLoopCnt +=1  
            
            # << CONDITION 3: Check if first key of this group is less than last value of the group >>
            if groupKey < lastValue:
                #print('     --Condition 3a fired FIRST PAIR in group CARRIED OVER **', '%s,%s'%(firstCompKey, firstValue))
                print('%s,%s'%(firstCompKey, firstValue))
                proLoopCnt +=1
    else:
        # Reporting to MapReduce Counter
        sys.stderr.write("reporter:counter:Transitive Closure Counters,Merge State,0\n")
        sys.stderr.write("reporter:counter:Transitive Closure Counters,Local Max State,1\n")
        # << CONDITION 1: check if key of key-group < firstValue of that group >>
        # << Condition 1a: Copy over key group(dont change anything) >>
        #print('****** Checking Group:', curr_KeySet, '***', curr_valSet, '******') #Debug
        for i in range(len(curr_KeySet)):
            #print('     --Condition 1a fired for this group with output **', curr_KeySet[i], curr_valSet[i])
            print('%s,%s'%(curr_KeySet[i], curr_valSet[i]))
            # Reporting to MapReduce Counter
            sys.stderr.write("reporter:counter:Transitive Closure Counters,Cluster List,1\n")
            sys.stderr.write("reporter:counter:Transitive Closure Counters,Cluster List,0\n")
            proLoopCnt +=1
    return
################ END OF FUNCTIONS ################

def TransitiveClosure():
    global groupKey
    global key
    global groupValue
    global groupKeyList

    ###### Input Prepping ######
    isLinkedIndex = False
    isUsedRef = False
    for file in sys.stdin:
        # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
        file1 = file.strip().replace(',','.').replace('\t','.')
        # Check if the ref has already been used
        if '*used*' in file1:
            isUsedRef = True
            print(file1.strip()) 
            continue
        if 'GoodCluster' in file1:
            isLinkedIndex = True
            print(file1.strip())
            continue
        kv = file.strip().split(',')
        compKey = kv[0]
        key = compKey.split('.')[0]
        value = kv[1]
        #print(compositeKey,value)

        # Creating a data structure in the form, "key {val1, val2, val3,...valn}",
        # for each key group. This data structure is what will be used for the 
        # start of the iteration to form connected components
        if groupKey == key:
            groupKeyList = groupKeyList + ',' + compKey
            groupValue = groupValue + ',' + value
        else:
            if groupKey:
                #print(groupKey,groupValue)
                # Forming Group k-v pairs which will be fed into the iterator
                # 'set' function ensures there are no duplicate values
                curr_valSet = list(set([v.strip() for v in groupValue.split(',')]))
                curr_KeySet = list(set([k.strip() for k in groupKeyList.split(',')]))
                curr_valSet.sort()  
                curr_KeySet.sort()

                # Calling the TC function for current key group before moving to next group
                ConnectedComponentsMR(curr_KeySet,curr_valSet)

            groupKeyList = compKey
            groupKey = key    
            groupValue = value

    # Calling TC function for last key group
    if groupKey == key:
        curr_valSet = list(set([v.strip() for v in groupValue.split(',')]))
        curr_KeySet = list(set([k.strip() for k in groupKeyList.split(',')]))
        curr_valSet.sort()  
        curr_KeySet.sort()

        ConnectedComponentsMR(curr_KeySet,curr_valSet)

if __name__ == '__main__':
    TransitiveClosure()  
############################################################
#               END OF PROGRAM      
############################################################