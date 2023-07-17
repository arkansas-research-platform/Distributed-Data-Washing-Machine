#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import itertools
import sys
import os
from operator import itemgetter
 #########################################################
 #        TAGGING CLUSTERED REFERENCES Reducer
 # Tag refs that has been processed already in previous 
 # iterations. If not tagged, they will be processed again    
 #########################################################
def TagRefsReduce():
    currentRefID = None 
    # Current word value (refID)                        
    refID = None  
    current_count = 0 
    currRestInfo = None
    tag = '*usedRef*'
    lineToKeep = True

    # Read the data from STDIN (the output from mapper)
    for items in sys.stdin:
        file = items.strip().split('|') 
        #print(file)
        refID = file[0]
        restInfo = file[1]
        #print(restInfo)

        #oldTag = file[2]
        #mdata = CID+'-'+oldTag
        #print(CID)
        if currentRefID == refID:
            current_count += 1
            currRestInfo = currRestInfo + '|' + restInfo 
        else:
            if currentRefID:
                #print (currentRefID, current_count, currRestInfo)
                if current_count >1:
                    for x in currRestInfo.split('|'):
                        #print('%s-%s%s'% (currentRefID,x,tag))
                        ref = '%s%s'% (x,tag)
                        #print(ref)
                        # Skip all copies
                        if '*copy*' in ref:
                            lineToKeep = False
                        else: 
                            lineToKeep = True
                            print(ref)
                else:
                    #ref = '%s,%s'% (currentRefID,currRestInfo)
                    ref = currRestInfo
                    # If its single but has already been clustered, tag as used
                    if 'GoodCluster' in ref:
                        lineToKeep = False
                        print(ref,tag)
                    else:
                        print(ref)
            current_count = 1
            currentRefID = refID
            currRestInfo = restInfo
    ##      
    ### Output the last word
    if currentRefID == refID:
        #print (currentRefID, current_count, currRestInfo)
        if current_count >1:
            for x in currRestInfo.split('|'):
                #print('%s-%s%s'% (currentRefID,x,tag))
                #ref = '%s%s'% (currentRefID,x,tag)
                ref = '%s%s'% (x,tag)
                #print(ref)
                if '*copy*' in ref:
                    lineToKeep = False
                else:
                    lineToKeep = True
                    print(ref)
        else:
            #print('%s-%s'% (currentRefID,currRestInfo))
            ref = currRestInfo
            #print(ref)
            if 'GoodCluster' in ref:
                lineToKeep = False
                print(ref,tag)
            else:
                print(ref)

if __name__ == '__main__':
    TagRefsReduce()  
############################################################
#               END OF MAPPER       
############################################################