#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys
import re
import json
from collections import Counter
import os
import time
# Making DWM modules available for MapReduce
sys.path.append('DWM-Modules.zip')
import DWM10_Parms 

 #########################################################
 #              DEVELOPER's NOTES
 #
 #########################################################

#====== Read Parms file ====== 
DWM10_Parms.getParms('parmStage.txt')
hasHeader = DWM10_Parms.hasHeader
delimiter = DWM10_Parms.delimiter
tokenizerType = DWM10_Parms.tokenizerType
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens

def TokenizationMapper(data):
    #***********Inner Function*******************************
    #Replace delimiter with blanks, then compress token by replacing non-word characters with null
    def tokenizerCompress(line):
        string = line.upper()
        string = string.replace(delimiter,' ')
        tokenList = re.split('[\s]+',string)
        newList = []
        for token in tokenList:
            newToken = re.sub('[\W]+','',token)
            if len(newToken)>0:
                newList.append(newToken)
        return newList
    #***********Inner Function*******************************
    #Replace all non-words characters with blanks, then split on blanks
    def tokenizerSplitter(line):
        string = line.upper()
        string = re.sub('[\W]+',' ',string)
        tokenList = re.split('[\s]+',string)
        newList = []
        for token in tokenList:
            if len(token)>0:
                newList.append(token)
        return newList

    goodType = False
    if tokenizerType=='Splitter':
        tokenizerFunction = tokenizerSplitter
        goodType = True
    if tokenizerType=='Compress':
        tokenizerFunction = tokenizerCompress
        goodType = True
    if goodType == False:
        print('**Error: Invalid Parameter value for tokenizerType ',tokenizerType)
        sys.exit()
    #*********** End of Inner Functions *******************************

    unclean_file = data.strip()

    firstDelimiter = unclean_file.find(delimiter)
    value = unclean_file[0:firstDelimiter]
    keyTokens = unclean_file[firstDelimiter+1:]
    cleanLine = tokenizerFunction(keyTokens)
    valCleanline = [value] + cleanLine
    return valCleanline
############################################################
#               END OF MAPPER       
############################################################