#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys
import re
import json
from collections import Counter
import os

# Making DWM modules available for MapReduce
sys.path.append('DWM-Modules.zip')
import DWM10_Parms 

# Read Parms file
DWM10_Parms.getParms('parms')
 #########################################################
 #              DEVELOPER's NOTES
 #     --TOKENIZATION Mapper (Metadata Creation)--
 #  Input comes from standard input STDIN (original data)
 #  Takes each row of record and breaks its down into tokens
 #  and then forms a json structure from it with each of 
 #  tokens as the key and the refID, token position, and 
 #  the token itself as values in a json structure. 
 #########################################################

# Get Parameter values
hasHeader = DWM10_Parms.hasHeader
delimiter = DWM10_Parms.delimiter
tokenizerType = DWM10_Parms.tokenizerType
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens

def TokenizationMapper():
    refCnt = 0
    tokCnt = 0
    tokensLeft = 0
    numericTokCnt = 0
    #***********Inner Function*******************************
    #Replace delimiter with blanks, then compress token by replacing non-word characters with null
    def tokenizerCompress(line):
        string = line.replace(delimiter,' ')
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
        string = re.sub('[\W]+',' ',line)
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
##########################################################
##                # START OF MAPPER PROGRAM #
########################################################
# Remove(skip) the header from the record if hasHeader is True

# -- Note: This parameter is discontinued in Distributed Environment
#if hasHeader:
#    next(sys.stdin)

    # Read and tokenize each line
    for line in sys.stdin:
        # Convert dataset into uppercase
        uc_file = line.upper()
        refCnt += 1
        #print(uppercase_file)    
        # Remove leading and trailing whitespaces
        unclean_file = uc_file.strip()
        #print(unclean_file)
        firstDelimiter = unclean_file.find(delimiter)
        value = unclean_file[0:firstDelimiter]
        keyTokens = unclean_file[firstDelimiter+1:]
        cleanLine = tokenizerFunction(keyTokens)
        #print(value)
    
        # Counting tokens
        tokCnt = tokCnt + len(cleanLine)
        for t in cleanLine:
            # Reporting to MapReduce Counter
            sys.stderr.write("reporter:counter:Tokenization Counters,Tokens Found,1\n")
    
        # Remove Duplicate tokens or not
        if removeDuplicateTokens:
            cleanLine = list(dict.fromkeys(cleanLine))
        tokensLeft = tokensLeft + len(cleanLine)
        # Reporting to MapReduce Counter
        for x in cleanLine:
            sys.stderr.write("reporter:counter:Tokenization Counters,Remaining Tokens,1\n")
    
        # Counting numeric tokens
        for tok in cleanLine:
            tok = tok.strip().replace('"','').replace("'","")
            if tok.isdigit():
                # Reporting to MapReduce Counter
                sys.stderr.write("reporter:counter:Tokenization Counters,Numeric Tokens,1\n")
                numericTokCnt +=1
        
        for n,key in enumerate(cleanLine,1): #n is a num/position using enumerate and starts from 1
            # Generate json-like output with metadata information
            mypair = {'refID':value, 
                    'pos': n, 
                    'tok': key}
            mypair = str(mypair).replace('(', '').replace(')', '')
            print('%s | %s | %s'% (key, mypair, 'one')) 

if __name__ == '__main__':
    TokenizationMapper()
############################################################
#               END OF MAPPER       
############################################################