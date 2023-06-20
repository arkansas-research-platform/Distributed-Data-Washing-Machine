#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys 
import subprocess
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
locMaxState = False
mergeState = False
recordToSkip = False
runNextIteration=False
#runNextIteration = "iterateNo"

###### TRANSITIVE CLOSURE FUNCTION #####
def trasitiveClosure(curr_valSet):
    # START OF TRANSITIVE CLOSURE CONDITIONS
    firstValue = curr_valSet[0].strip()
    lastValue = curr_valSet[-1].strip()
    groupSize = len(curr_valSet)
    #print(groupKey,lastValue)

    # << CONDITION 1: check if key of key-group < firstValue of that group >>
    if groupKey < firstValue:
        # << Condition 1a: Copy over key group(dont change anything) >>
        #print('****** Checking Group:', groupStructure, '******') #Debug
        for v in curr_valSet:
            #print('     --Condition 1a fired for this group with output **', groupKey, v)
            print('%s:%s'%(groupKey.strip(), v.strip()))
            
    else:
        # Any time the mergeState is true/when we have to create chains, a counter is started
        # This counter is used by the Driver file to determine whether the TC process should 
        # be run for the next iteration or the programme should quit.
        global runNextIteration
        runNextIteration=True
        #runNextIteration = "iterateNo"
        # << CONDITION 2: Check if the length is greater than 1 >>
                # NB: Added "'A922259' : 'A992529'" to the input file, out_LinkedPairs.txt,
                #     to test test this section, condition 2, i.e. if there are more than 2 values,
                # how will the chain llok like. Just checking if the chaining works as expected.
                # After successful debugging, remove "'A922259' : 'A992529'" from the input file, line 65
        if groupSize > 1:
            # << Condition 2a: Generate chains from the values for that key group >>
            #print('****** Checking Group:', groupStructure, '******')  #Debug
            for i in range(len(curr_valSet) - 1):
                x = curr_valSet[i]
                y = curr_valSet[i + 1]
                #print('     --Condition 2a fired for this group with output **',  x, y)
                print('%s:%s'%(x, y))

            # << CONDITION 3: Check if first key of this group is less than last value of the group >>
            if groupKey < lastValue:
                #print('     --Condition 3a fired for this group with output **', groupKey, firstValue)
                print('%s:%s'%(groupKey.strip(), firstValue))
         
        else:
            # << Condition 2b: Go To next Group >>
            #print('****** Checking Group:', groupStructure, '******')  #Debug
                    
            # << Condition 2b: Go To next Group if groupKey > 1st value but len(values) = 1 >>
            if groupKey > firstValue and groupSize == 1:
                recordToSkip = True
                #print('     --Condition 2b fired for this group with the pair **', groupStructure, '> skipped.')
    return

###### Input Prepping ######
for file in sys.stdin:
    kv = file.strip().split(':')
    key = kv[0]
    value = kv[1]

    # Creating a data structure in the form, "key {val1, val2, val3,...valn}",
    # for each key group. This data structure is what will be used for the 
    # start of the iteration to form connected components
    if groupKey == key:
        groupValue = groupValue + ',' + value
    else:
        if groupKey:
            # Forming Group k-v pairs which will be fed into the iterator
            # 'set' function ensures there are no duplicate values
            curr_valSet = list(set([vals.strip() for vals in groupValue.split(',')]))
            curr_valSet.sort()  
            groupStructure = '%s - %s'%(groupKey, curr_valSet)
            #print(groupStructure)

            # START OF TRANSITIVE CLOSURE CONDITIONS
            #firstValue = curr_valSet[0]
            #lastValue = curr_valSet[-1]
            #groupSize = len(curr_valSet)
            #print(groupKey,lastValue)
          
            # Calling the TC function for current key group before moving to next group
            trasitiveClosure(curr_valSet)

        groupKey = key    
        groupValue = value
            
# Calling TC function for last key group
if groupKey == key:
    curr_valSet = list(set([vals.strip() for vals in groupValue.split(',')]))
    curr_valSet.sort()
    # Forming Group k-v pairs of last group which will be fed into the iterator
    groupStructure = '%s - %s'%(groupKey, curr_valSet)
    #print("Last Record", groupStructure)
    
    trasitiveClosure(curr_valSet)

# Check how many times we had mergeState cases. This number is used by the driver file
# to determine whether or not to run the next iteration. The iterations stops when 
# this value is less than (<) 1
#print(bashCheckCounter)
f1 = open("./HDWM/check.txt", "w")
f1.write(str(runNextIteration))
f1.close()

#echoIterationStatus = 'echo "$runNextIteration"'
#subprocess.call(echoIterationStatus, shell=True) 
############################################################
#               END OF PROGRAM      
############################################################
