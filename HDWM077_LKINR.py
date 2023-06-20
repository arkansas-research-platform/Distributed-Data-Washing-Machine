#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import itertools
import sys
import os
 #########################################################
 #        TAGGING CLUSTERED REFERENCES Reducer
 # Tag refs that has been processed already in previous 
 # iterations. If not tagged, they will be processed again    
 #########################################################
#Loading the Log_File from the bash driver
logfile = open(os.environ["Log_File"],'a')

# Give Header to Linked Index File
print('RefID * ClusterID')
# This is the current word key
currentRefID = None 
# Current word value (refID)                        
refID = None  
current_count = 0 
currRestInfo = None
tag = '*usedRef*'
lineToKeep = True
countLinkIndexRefs = 0

# Read the data from STDIN (the output from mapper)
for items in sys.stdin:
    file = items.strip().split('|') 
    #print(file)
    refID = file[0].replace("'","").replace('"','').replace(' ','')
    restInfo = file[1].replace("'","").replace('"','').replace(' ','')
    #print(restInfo)

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
                    elif 'one*usedRef*' in ref:
                        lineToKeep = False
                    else: 
                        lineToKeep = True
                        #print(ref)
                        cleanRID = ref.split('-')[0]
                        cleanCID = ref.split('-')[1]
                        countLinkIndexRefs +=1
                        print('%s * %s'%(cleanRID,cleanCID))
            else:
                #ref = '%s,%s'% (currentRefID,currRestInfo)
                ref = currRestInfo
                # If its single but has already been clustered, tag as used
                if 'GoodCluster' in ref:
                    lineToKeep = False
                    tagGood = '%s,%s'%(ref,tag)
                    #print(tagGood)
                    cleanRID = tagGood.split('-')[0].strip()
                    cleanCID = cleanRID.strip()
                    countLinkIndexRefs +=1
                    print('%s * %s'%(cleanRID,cleanCID))
                else:
                    cleanRID = ref.split('-')[0].strip()
                    cleanCID = cleanRID.strip()
                    countLinkIndexRefs +=1
                    print('%s * %s'%(cleanRID,cleanCID))
                    #print(ref)
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
            elif 'one*usedRef*' in ref:
                lineToKeep = False
            else:
                lineToKeep = True
                cleanRID = ref.split('-')[0].strip()
                cleanCID = ref.split('-')[1].strip()
                countLinkIndexRefs +=1
                print('%s * %s'%(cleanRID,cleanCID))
    else:
        #print('%s-%s'% (currentRefID,currRestInfo))
        ref = currRestInfo
        #print(ref)
        if 'GoodCluster' in ref:
            lineToKeep = False
            tagGood = '%s,%s'%(ref,tag)
            #print(tagGood)
            cleanRID = tagGood.split('-')[0].strip()
            cleanCID = cleanRID.strip()
            countLinkIndexRefs +=1
            print('%s * %s'%(cleanRID,cleanCID))
        else:
            cleanRID = ref.split('-')[0].strip()
            cleanCID = cleanRID.strip()
            countLinkIndexRefs +=1
            print('%s * %s'%(cleanRID,cleanCID))
            #print(ref)
        
# Reporting to logfile
print('\n>> Starting Write-To-LinkIndex Process', file=logfile)
print('   Total Records Written to Linked Index File: ', countLinkIndexRefs, file=logfile)
############################################################
#               END OF MAPPER       
############################################################