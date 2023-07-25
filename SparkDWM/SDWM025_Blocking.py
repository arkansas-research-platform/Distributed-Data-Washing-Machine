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

#########################################################
#                     DEVELOPER's NOTES
#
#########################################################

#====== Read Parms file ====== 
DWM10_Parms.getParms('parmStage.txt')
beta = DWM10_Parms.beta
excludeNumericBlocks = DWM10_Parms.excludeNumericBlocks
minBlkTokenLen = DWM10_Parms.minBlkTokenLen
blockByPairs = DWM10_Parms.blockByPairs

#===================================================================================
def BlockTokenPairMap(data):
    blkTokenList = []
    refID = data[0]
    metaDataDict = data[1]
    mdataVals = list(metaDataDict.values())
    # Get Tokens and Frequency from the metadata
    for item in mdataVals:
        isBlkToken = True 
        item = item.split('^')
        token = item[0].strip().strip().replace('"','').replace("'","")
        frequency = int(item[1].strip())
        #blkTokenList.append(token)
    
# ---- PHASE 1: TLR - Deciding on Blocking Tokens in each Ref ------
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
    return (refID, blkTokenList)

#===================================================================================
def byPairs(keyList):
    # ---- PHASE 2: BTPM - Forming Blocking Keys using the tokens in BlknTokenList ------
        refID = keyList[0]
        toks = keyList[1]

        pairList = []
        for x in range(0, len(toks)-1):
            Xtoken = toks[x].strip()
            #print(Xtoken)
            for y in range(x+1, len(toks)):
                Ytoken = toks[y].strip()
                #print(Ytoken)
                # Keeping Pairs in Ascending Order
                if Xtoken < Ytoken:
                    pair = (Xtoken+Ytoken)
                    pair = ('%s:%s' % (pair, refID))
                    #return pair
                    pairList.append(pair)
                else:
                    pair = (Ytoken+Xtoken)
                    pair = ('%s:%s' % (pair, refID))
                    #return pair
                    pairList.append(pair)
        return pairList

#===================================================================================          
def bySingles(keyList):
    refID = keyList[0]
    toks = keyList[1]
    pairList = []

    for x in range(0, len(toks)):
        Xtoken = toks[x]
        Xtoken = ('%s:%s' % (Xtoken, refID))
        #return Xtoken
        pairList.append(Xtoken)
    return pairList

#=================================================================================== 
def refIDPairing(refIDList):
    keyPairList = []
    tokKey = refIDList[0]
    refIDs = refIDList[1]
    tokKeySplit = refIDs.split(',')

    for x in range(0, len(tokKeySplit)-1):
        for y in range(x+1, len(tokKeySplit)):
            Xtoken = tokKeySplit[x]
            Ytoken = tokKeySplit[y]
            if Xtoken < Ytoken:
                pair = (Xtoken+','+Ytoken)
                keyPair = ('%s:%s' % (pair, 'one'))
                keyPairList.append(keyPair)
            else:
                pair = (Ytoken+','+Xtoken )
                keyPair = ('%s:%s' % (pair, 'one'))
                keyPairList.append(keyPair)
    return keyPairList
#===================================================================================

############################################################
#               END OF MAPPER       
############################################################