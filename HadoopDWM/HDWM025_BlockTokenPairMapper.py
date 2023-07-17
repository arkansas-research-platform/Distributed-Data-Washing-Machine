#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys
import re
import os
from pathlib import Path

# Making DWM modules available for MapReduce
sys.path.append('DWM-Modules.zip')
import DWM10_Parms 

# Read PArms file
DWM10_Parms.getParms('parms')
 #########################################################

 #                     DEVELOPER's NOTES
 #                 BLOCKING TOKENS PAIRS Mapper 
 #  Takes the tokens that satisfied the Beta threshold and 
 #  that formed list from the TLR step and looped through
 #  each list to create pairs with the pairs as key and the 
 #  refID as value. This process considers whether 
 #  numeric tokens should be included or excluded
 #########################################################

 # Get Parameter values
beta = DWM10_Parms.beta
excludeNumericBlocks = DWM10_Parms.excludeNumericBlocks
minBlkTokenLen = DWM10_Parms.minBlkTokenLen
blockByPairs = DWM10_Parms.blockByPairs
#############################
####### MAIN PROGRAM #######
############################
def BlockTokenPairMap():
    isLinkedIndex  = False 
    isUsedRef = False
    selectedRefCnt = 0  
    blkRefsCnt = 0
    numericCnt = 0
    remainRefs = 0
    excludedRefCnt = 0

    for record in sys.stdin:
        file = record.strip()
        #print(file)
        file_split = file.split('-')
        #print(f_split)
        # Check if the ref has already been used
        if '*used*' in file:
            isUsedRef = True
            print(file.replace('-',':')) 
            continue
        # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
        if 'GoodCluster' in file:
            isLinkedIndex = True
            print(file.replace('-',':')) 
            continue
        # Reporting to MapReduce Counter
        sys.stderr.write("reporter:counter:Blocking Counters,Refs for Reprocessing,1\n")
        selectedRefCnt +=1
        # Get refID
        refID = file_split[0].strip()
        metaData = file_split[1].strip()
        #print(metaData)

        # Get Tokens and Frequency from the metadata
        metaData = metaData.replace("{",'').replace("}",'').split(',')

        blkTokenList = []
        tokenFreq = []
        for item in metaData:
            item = item.split(':')
            tokenFreq.append(item[1].strip().replace("'",""))
        #print(tokenFreq)
        for clean in tokenFreq:
            isBlkToken = True 
            clean = clean.split('^')
            token = clean[0].strip().replace('"','').replace("'","")
            frequency = int(clean[1])
            #print(token, frequency)

    # ---- PHASE 1: TLR - Deciding on Blocking Tokens in each Ref ------
            # Count numeric tokens
            if token.isdigit():
                numericCnt += 1
            # If the frequency of the refID tokens is 1, skip it
            if frequency < 2:
                #print('-- Freq < 2 Rule: ', refID,token,frequency)
                isBlkToken = False
            # If the frequency of the refID tokens is greater than beta, skip it
            if frequency > beta:
                #print('-- Freq > Beta Rule: ', refID,token,frequency)
                isBlkToken = False
            # Exclude or include numeric tokens
            if excludeNumericBlocks and token.isdigit():
                #print('-- Num Token Rule: ', refID,token)
                isBlkToken = False
            if len(token) < minBlkTokenLen:
                #print('-- Min Blocking Token Length Rule: ', refID,token, 'len', len(token))
                isBlkToken = False
            # Remainder Blocking Tokens
            if isBlkToken:
                blkTokenList.append(token)
        #print('-- Blocking Tokens for this Ref: ',refID, blkTokenList)

    # ---- PHASE 2: BTPM - Forming Blocking Keys using the tokens in BlknTokenList ------
        # If there are no tokens in a list, nothing to do, so delete such list
        if len(blkTokenList) < 1:
            # Reporting to MapReduce Counter
            sys.stderr.write("reporter:counter:Blocking Counters,Excluded References,1\n")
            excludedRefCnt +=1
            continue
        # Reporting to MapReduce Counter
        sys.stderr.write("reporter:counter:Blocking Counters,Remaining References,1\n")
        remainRefs += 1
        #print(refID, blkTokenList)
    ##---------------------------------------------
        ### Blocking-by-Pairs ###
        # Exclude single tokens in a list if "Block-by-Pairs" is True 
        if blockByPairs:
            if len(blkTokenList) < 2:
                continue
            #print(blkTokenList)
            # Create a nested for loop to build pairs of refIDs to be compared
            for x in range(0, len(blkTokenList)-1):
                Xtoken = blkTokenList[x].strip()
                #print(Xtoken)
                for y in range(x+1, len(blkTokenList)):
                    Ytoken = blkTokenList[y].strip()
                    #print(Ytoken)
                    # Keeping Pairs in Ascending Order
                    if Xtoken < Ytoken:
                        #pair = (Xtoken + "," + Ytoken)
                        pair = (Xtoken+Ytoken)
                        print ('%s:%s' % (pair, refID))
                        # Reporting to MapReduce Counter
                        sys.stderr.write("reporter:counter:Blocking Counters,Blocking References Created,1\n")
                        blkRefsCnt += 1
                    else:
                        #pair = (Ytoken + "," + Xtoken)
                        pair = (Ytoken+Xtoken)
                        print ('%s:%s' % (pair, refID))
                        # Reporting to MapReduce Counter
                        sys.stderr.write("reporter:counter:Blocking Counters,Blocking References Created,1\n")
                        blkRefsCnt += 1
    ##---------------------------------------------
        # If BlockByPairs was set to False, that means blockBySingles
        else:
            for x in range(0, len(blkTokenList)):
                Xtoken = blkTokenList[x]
                print ('%s:%s' % (Xtoken, refID))
                # Reporting to MapReduce Counter
                sys.stderr.write("reporter:counter:Blocking Counters,Blocking References Created,1\n")
                blkRefsCnt += 1

if __name__ == '__main__':
    BlockTokenPairMap()  
############################################################
#               END OF MAPPER       
############################################################