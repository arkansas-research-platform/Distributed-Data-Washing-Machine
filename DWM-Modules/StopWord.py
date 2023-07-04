#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys
import re

# Making DWM modules available for MapReduce
sys.path.append('DWM-Modules.zip')
import DWM10_Parms 
# Read Parms file
DWM10_Parms.getParms('parms')

 # Get Parameter values
sigma = DWM10_Parms.sigma
removeExcludedBlkTokens = DWM10_Parms.removeExcludedBlkTokens
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens
minBlkTokenLen = DWM10_Parms.minBlkTokenLen
excludeNumericBlocks = DWM10_Parms.excludeNumericBlocks

########### Remove StopWords Function ###############
def removestopwords(RefTokenList):
    cleanList = []
    for x in RefTokenList:
        carryToken = True
        tokenRef = x.split("^")[0].strip().replace('"','').replace("'","")
        #print(tokenRef)
        freqRef = x.split("^")[1].strip().replace('"','').replace("'","")
        #print(freqRef)
        # Remove all tokens with freq higher or equal to sigma
        if int(freqRef) >= sigma:
            carryToken = False
        # Check for other rules (Duplicate tokens, Excluded Block Tokens)
        if removeDuplicateTokens and (tokenRef in cleanList):
            carryToken = False  
        if removeExcludedBlkTokens:
            if len(tokenRef) < minBlkTokenLen:
                carryToken = False
                #print('tokenRef, len(tokenRef))
            if excludeNumericBlocks and tokenRef.isdigit():
                carryToken = False
                #print('tokenRef)     
        if carryToken:
            cleanList.append(tokenRef)
    return cleanList
########### End of Remove StopWords Function ###############