#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys 
import re
import os
import math
from operator import itemgetter
 ##############################################################
 #                     DEVELOPER's NOTES
 #  Cluster Entropy Evaluation Calculator (CECR) reducer. 
 #  Takes as input, the output from LKIM-LKIR output and 
 #  calculated the entropy (organization/disorganization) of 
 #  each cluster. If the cluster is good, it is kept as LinkedIndex
 #  if bad cluster, they are sent back to blocking for reprocessing
 ##############################################################
####### READ PARAMETER FILE #######
# Getting Epsilon value
with open('epsilonReport.txt', 'r') as openMuFile:
    epsilonVal = str(openMuFile.readline()).strip()
epsilon = float(epsilonVal)

########### Entropy Calculator Function ###############
# Note: The function takes (cluster group list, cluster size, total tokens in each group)
def entropyCalculator(ClusterGroupList):
    cluster = ClusterGroupList
    #print('--', cluster)
    baseProb = 1/float(sizeOfClusterGrp)  # Worse case 
    denominator = -totalTokensInClusterGrp*(baseProb*math.log(baseProb,2))
    entropy = 0.0
    # Iterate each cluster group
    for x in range(0, sizeOfClusterGrp-1):
        xList = cluster[x]
        #print('x=',x,'xList=', xList)
        for token in xList:
            cnt = 1
            #print('token=', token, ',', 'cnt=', cnt)
            for y in range(x+1,sizeOfClusterGrp):
                #print('y=',y)
                if token in cluster[y]:
                    cnt +=1
                    cluster[y].remove(token)
                    #print(curClusterID, token, 'Token found in: ', y, cluster[y])
            # Get probability of each token in cluster
            probOfToken = cnt/sizeOfClusterGrp
            #print('--ClusterID:', curClusterID, 'Token:', token, 'has prob of -->', probOfToken)
            # Get token entropy
            tokenEntropy = -probOfToken*math.log(probOfToken,2)
            # Get total cluster entropy
            entropy += tokenEntropy
            # Normalized the entropy score
            qualityScore = 1.0 - entropy/denominator
            #print('--ClusterID:', curClusterID, ', Normalized etropy -->', qualityScore)
            # Compare Quality score with Entropy
            if qualityScore < epsilon:
                #print('quit early top row, entropy=', entropy, ' quality=',qualityScore)
                return qualityScore
            cnt = 0
    # Get the remaining tokens that are left in last ref of each cluster
    for token in cluster[sizeOfClusterGrp-1]:
        # Get probability of each token in cluster
        probOfToken = 1.0/sizeOfClusterGrp
        #print('--ClusterID:', curClusterID, 'Token:', token, 'has prob of -->', probOfToken)
        # Get token entropy
        tokenEntropy = -probOfToken*math.log(probOfToken,2)
        # Get total cluster entropy
        entropy += tokenEntropy
        # Normalized the entropy score
        qualityScore = 1.0 - entropy/denominator
        #print('--ClusterID:', curClusterID, ', Normalized etropy -->', qualityScore)
        if qualityScore < epsilon:
            #print('quit early top row, entropy=', entropy, ' quality=',qualityScore)
            return qualityScore
        cnt = 0
    #print('--Entire cluster scanned, entropy=', entropy, ' normalized=',qualityScore)    
    qualityScore = 1.0 - entropy/denominator
    #print(qualityScore)
    return qualityScore
########### End of Remove StopWords Function ###############

####################################################
################### Main Program ###################
####################################################
curClusterID  = None 
currTokenList = None
currRefIDList = None  
clusterID = None 
isClusterLine = False 
tag1 = 'GoodCluster'    # For good cluster refs (refID,CID)
tag2 = 'BadCluster'     # For bad cluster refs (refID,tokens)
tag3 = 'UnprocessedRef' # For unprocessed refs (refID,tokens)
isLinkedIndex = False
isUsedRef = False
# Counters
clusterSize = 0
cluSizeGreaterThanOne = 0
totalClustersProcessed = 0
totalRefsInClusters = 0
goodClusterProcessed = 0
totalRefsInGoodClusters = 0

tokensInAllClusters = 0

for line in sys.stdin:
    line = line.strip()
    #print(line)
    # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
    if 'GoodCluster' in line:
        isLinkedIndex = True
        print(line.strip())#.replace('-1-',''))
        continue
    
    line = line.strip().split('-', maxsplit=1)
    clusterID = line[0].strip()
    refIDbody = line[1].split('-',maxsplit=1)
    refID = refIDbody[0].strip()
    tokens = refIDbody[1].strip().replace(',','.').replace("'",'')

    # Separate Clustered Refs from Unprocessed Refs
#-----> Phase 1: Reserving all unprocessed references
    if clusterID == 'unprocessedRef':
        isClusterLine = False
        tokens = tokens.replace('.',',')
        print('%s-%s-%s'% (refID, tokens, tag3))
        continue
    #print(clusterID,refID)
    #print(tokens)

#-----> Phase 2: Process Clusters for Good and Bad Clusters
    # Evaluate only formed clusters
    clusterID2 = line[0].strip()
    refID2 = refIDbody[0].strip()
    tokens2 = refIDbody[1].strip().replace(',','.').replace("'",'')
    # Extract only tokens, ignoring position and frequency info
    #tokens2 = tokens2.replace('{','').replace('}','').split('.')
    #print(tokens2)
    if curClusterID == clusterID2:
        currTokenList = currTokenList + '@' + tokens2
        currRefIDList = currRefIDList + '@' + refID2
        clusterSize +=1
    else:
        if curClusterID:
            # Reporting to MapReduce Counter
            sys.stderr.write("reporter:counter:Cluster Evaluation Counters,Total Clusters Processed,1\n")
            totalClustersProcessed +=1
            # Process each cluster group
            newClusterGroup = []    #Count total tokens in each cluster group
            ClusterGroupList = []   #Create list of refs in each cluster group
            clusterGroup = currTokenList.split('@')
            refIDgroup = currRefIDList.split('@')
            #print('---Debug', 'CID',curClusterID,'RefID List ',currRefIDList)
            for refs in clusterGroup:
                # Reporting to MapReduce Counter
                sys.stderr.write("reporter:counter:Cluster Evaluation Counters,Total References in Clusters,1\n")
                totalRefsInClusters +=1
                #print(refs)
                ###refs =  refs.replace('[','').replace(']','').replace("'","").replace(' ','').split('.')
                tokInRef = [str(x.split(':')[1]).split('^')[0] for x in refs.split('.')]
                #print('--- Reference', curClusterID, tokInRef)
                # Get total tokens in each cluster group
                newClusterGroup.extend(tokInRef)
                # Get list of each cluster list (list comprehension)
                ClusterGroupList.append(tokInRef)
            #print('--- Coount of Refs in cluster group:', curClusterID, '--->', clusterGroup,clusterSize)
            #print('--- Total Tokens in cluster:', curClusterID, '--->', newClusterGroup, len(newClusterGroup))
            #print('--- List of refs in cluster:', curClusterID, '--->', ClusterGroupList)

            # Checking for cluster Quality using Entropy 
            sizeOfClusterGrp = len(ClusterGroupList)
            totalTokensInClusterGrp = len(newClusterGroup)
            if sizeOfClusterGrp > 1:
                # Reporting to MapReduce Counter
                sys.stderr.write("reporter:counter:Cluster Evaluation Counters,Cluster Size Greater than 1,1\n")
                cluSizeGreaterThanOne +=1
                #print('-- calculate entropy of cluster',clusterID, 'Size-', sizeOfClusterGrp)
                qualityScore = entropyCalculator(ClusterGroupList)
                ##print('-- CID-',curClusterID, 'Size-', sizeOfClusterGrp, 'Qscore-', qualityScore)
            else:
                qualityScore = 1.0
                ##print('-- CID-',curClusterID, 'Size-', sizeOfClusterGrp, 'Qscore-', qualityScore)  
            
            # Get Good vs Bad Cluster (Output both - when all clusters are good, the iteration will exit)
                # Good Clusters
            if qualityScore >= epsilon:
                # Reporting to MapReduce Counter
                sys.stderr.write("reporter:counter:Cluster Evaluation Counters,Total Good Clusters,1\n")
                goodClusterProcessed +=1
                for rID in refIDgroup:
                    # Reporting to MapReduce Counter
                    sys.stderr.write("reporter:counter:Cluster Evaluation Counters,References in Good Clusters,1\n")
                    totalRefsInGoodClusters +=1
                    rID = rID.strip()
                    #print('-- CID-', curClusterID, '-- RefID-',rID, 'Good Cluster')
                    print('%s-%s-%s'%(rID, curClusterID,tag1))             
              # Bad Clusters
            else:
                for rID,tkns in zip(refIDgroup,clusterGroup):
                    tkns = tkns.replace('.', ',')
                    #print('-- Bad Cluster ', '-- RefID-',rID, '-- Tokens', tkns) 
                    print('%s-%s-%s'% (rID, tkns, tag2))
        curClusterID = clusterID2
        currRefIDList = refID2
        currTokenList = tokens2
        clusterSize = 1
# Process last record in file
if curClusterID:
    # Reporting to MapReduce Counter
    sys.stderr.write("reporter:counter:Cluster Evaluation Counters,Total Clusters Processed,1\n")
    totalClustersProcessed +=1
    newClusterGroup = []    #Count total tokens in each cluster group
    ClusterGroupList = []   #Create list of refs in each cluster group
    clusterGroup = currTokenList.split('@')
    refIDgroup = currRefIDList.split('@')
    #print('---Debug', 'CID',curClusterID,'RefID List ',currRefIDList)
    for refs in clusterGroup:
        # Reporting to MapReduce Counter
        sys.stderr.write("reporter:counter:Cluster Evaluation Counters,Total References in Clusters,1\n")
        totalRefsInClusters +=1
        ###refs =  refs.replace('[','').replace(']','').replace(' ','').split('.')
        tokInRef = [str(x.split(':')[1]).split('^')[0] for x in refs.split('.')]
        #print('--- Reference', tokInRef)
        # Get total tokens in each cluster group
        newClusterGroup.extend(tokInRef)
        # Get list of each cluster list (list comprehension)
        ClusterGroupList.append(tokInRef)
    #print('--- Coount of Refs in cluster group:', curClusterID, '--->', clusterGroup,clusterSize)
    #print('--- Total Tokens in cluster:', curClusterID, '--->', newClusterGroup, len(newClusterGroup))
    #print('--- Total Refs in cluster:', curClusterID, '--->',ClusterGroupList)

    # Checking for cluster Quality using Entropy 
    sizeOfClusterGrp = len(ClusterGroupList)
    totalTokensInClusterGrp = len(newClusterGroup)

    if sizeOfClusterGrp > 1:
        # Reporting to MapReduce Counter
        sys.stderr.write("reporter:counter:Cluster Evaluation Counters,Cluster Size Greater than 1,1\n")
        cluSizeGreaterThanOne +=1
        #print('-- calculate entropy of cluster',curClusterID, 'Size-', sizeOfClusterGrp)
        qualityScore = entropyCalculator(ClusterGroupList)
        ##print('-- CID-',curClusterID, 'Size-', sizeOfClusterGrp, 'Qscore-', qualityScore)
    else:
        qualityScore = 1.0
        ##print('-- CID-',curClusterID, 'Size-', sizeOfClusterGrp, 'Qscore-', qualityScore)   

    # Get Good vs Bad Cluster (Output both - when all clusters are good, the iteration will exit)
       # Good Clusters
    if qualityScore >= epsilon:
        # Reporting to MapReduce Counter
        sys.stderr.write("reporter:counter:Cluster Evaluation Counters,Total Good Clusters,1\n")
        goodClusterProcessed +=1
        for rID in refIDgroup:
            # Reporting to MapReduce Counter
            sys.stderr.write("reporter:counter:Cluster Evaluation Counters,References in Good Clusters,1\n")
            totalRefsInGoodClusters +=1
            rID = rID.strip()
            #print('-- CID-', curClusterID, '-- RefID-',rID, 'Good Cluster') 
            print('%s-%s-%s'%(rID, curClusterID, tag1)) # Good cluster
      # Bad Clusters
    else:
        for rID,tkns in zip(refIDgroup,clusterGroup):
            tkns = tkns.replace('.', ',')
            #print('-- Bad Cluster ', '-- RefID-',rID, '-- Tokens', tkns) 
            print('%s-%s-%s'% (rID, tkns, tag2))
#############################################################
##               END OF REDUCER      
############################################################
