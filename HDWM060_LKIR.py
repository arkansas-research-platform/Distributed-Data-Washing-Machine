#!/usr/bin/env python
import sys
import string

 #########################################################
 #                  DEVELOPER's NOTES
 #              --Linked Index Reducer --               
 # Get the difference records between the TC output and 
 # Original reference, and also shows the out put from 
 # the TC output. The final output is (refIDs, clusterIDs)
 # from both the TC output and the Difference
 # Note: the cluster ID for each of the difference records
 # is the record itself. 
 #########################################################

# maps words to their counts
refID = "-1"
clusterID = "-1"
value = "-1"
tokenList = ""
currentRefID = None
currentRefID2 = None
isTokenMappingLine = True
lineToKeep = False
lineToDelete = False
currClusterID = clusterID
currClusterID2 = None
currTokenList = None
currTokenList2 = None
currRefIDCnt = 0
refIDcnt = 0
tag = 'unprocessedRef'  # This tag is used as the clusterID for all unprocessed Refs. 
                        # When the output from this process is sorted, the tag will appear last 
isLinkedIndex = False
isUsedRef = False
for line in sys.stdin:
    # Check if the ref has already been used
    #if '*used*' in line:
    #    isUsedRef = True
    #    print(line.strip().replace('|','-').replace(',','')) 
    #    continue
    # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
    if 'GoodCluster' in line:
        isLinkedIndex = True
        goodSplit=line.strip().replace('-1','').split('|')#replace('|','<>')
        rID = goodSplit[0]
        cID = goodSplit[1].replace('.','<>').replace('*usedRef*','')
        #print(goodPart1)
        print('%s<>%s'%(rID,cID))
        #continue
    lineSplit = line.strip().split('.')
    #print(lineSplit)
    refID_value = lineSplit[0].split('|')
    clusterID_mdata = lineSplit[1].split('|')
    #print(refID_Metadata)
    refID = refID_value[0].strip()
    refID2 = refID_value[0].strip()
    value = refID_value[1].strip()
    clusterID = clusterID_mdata[0].strip()
    clusterID2 = clusterID_mdata[0].strip()
    tokenList = clusterID_mdata[1].strip()
    tokenList2 = clusterID_mdata[1].strip()
    #print(refID2)
    
#-----> Phase 1: Update the clustered references with their corresponding tokens
    # First line should be a mapping line that contains 
    # the tokens information for that key group
    if value == '-1': #That is a line with tokens info that will be used to map
        currTokenList = tokenList
        currentRefID = refID
        isTokenMappingLine = True
    else:
        isTokenMappingLine = False
    #print('--Debug',refID,value,clusterID,currTokenList)
    #print(refIDcnt)

#-----> Phase 2: Get all the clustered refs with their updated tokens first
    if 'ClusterLine' in line:
        lineToKeep = True
        clusterID = clusterID.replace(' ClusterLine','')
        #print('-- Checking clustered Ref --> ', 'CID- ', clusterID, 'RefID- ', refID, 'Tokens-', currTokenList)
        print('%s-%s-%s'%(clusterID, refID, currTokenList))   # Send this to stdout

##-----> Phase 3: Get all the single refs that were not clustered and are from the original ref
    #print('---Debug',refID,clusterID,tokenList2)
    if currentRefID2 == refID2:
        currRefIDCnt = currRefIDCnt+1
        currTokenList2 = currTokenList2+'@'+tokenList2
        currClusterID2 = currClusterID2+'@'+clusterID2
    else:
        if currentRefID2:
            if currRefIDCnt == 1:
                #print('---Debug',currentRefID2,currClusterID2,currRefIDCnt,currTokenList2)
                print('%s-%s-%s'%(tag,currentRefID2,currTokenList2)) # Send this to stdout
        currentRefID2 = refID2
        currRefIDCnt = 1
        currTokenList2 = tokenList2
        currClusterID2 = clusterID2
if currentRefID2 == refID2:
    if currRefIDCnt == 1:
        #print('---Debug',currentRefID2,currClusterID2,currRefIDCnt,currTokenList2)
        print('%s-%s-%s'%(tag,currentRefID2,currTokenList2))  # Send this to stdout
#############################################################
#               END OF REDUCER       
############################################################