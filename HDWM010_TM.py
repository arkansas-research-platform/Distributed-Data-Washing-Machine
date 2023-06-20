#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys
import re
import json
from collections import Counter
from itertools import count
import os
 #########################################################
 #              DEVELOPER's NOTES
 #     --TOKENIZATION Mapper (Metadata Creation)--
 #  Input comes from standard input STDIN (original data)
 #  Takes each row of record and breaks its down into tokens
 #  and then forms a json structure from it with each of 
 #  tokens as the key and the refID, token position, and 
 #  the token itself as values in a json structure. 
 #########################################################
def convertToBoolean(value):
    if value=='True':
        return True
    if value=='False':
        return False

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

#Replace all non-words characters with blanks, then split on blanks
def tokenizerSplitter(line):
    string = re.sub('[\W]+',' ',line)
    tokenList = re.split('[\s]+',string)
    newList = []
    for token in tokenList:
        if len(token)>0:
            newList.append(token)
    return newList

# Loading the Log_File from the bash driver
#logfile = open(os.environ["Log_File"],'a')
logfile = open('/usr/local/jobTmp/HDWM_log.txt', 'a')
#print(file)
print('\n>> Starting Tokenization Process', file=logfile)

####### READ PARAMETER FILE #######
#parameterFile = open('S1G-parms-copy.txt', 'r') 
#parameterFile = open('parmStage.txt') 
parameterFile = open('parms') 
#parameterFile = "hdfs://snodemain:9000/user/nick/HadoopDWM/parmStage.txt"
while True:
    pline = (parameterFile.readline()).strip()
    if pline == '':
        break
    if pline.startswith('#'):
        continue
    if pline.find('=')<0:
        continue
    part = pline.split('=')
    parmName = part[0].strip()
    parmValue = part[1].strip()
    if parmName == 'hasHeader':
        hasHeader = convertToBoolean(parmValue)
        continue
    if parmName=='delimiter':
        if ',;:|\t'.find(parmValue)>=0:
            delimiter = parmValue
            continue
    if parmName=='tokenizerType':
        tokenizerType = parmValue
        if tokenizerType=='Splitter':
            tokenizerFunction = tokenizerSplitter
        if tokenizerType=='Compress':
            tokenizerFunction = tokenizerCompress
        continue
    if parmName=='removeDuplicateTokens':
        removeDuplicateTokens = convertToBoolean(parmValue)
        continue
########################################################
                # START OF MAPPER PROGRAM #
########################################################
# Remove(skip) the header from the record if hasHeader is True
if hasHeader:
    next(sys.stdin)

refCnt = 0
tokCnt = 0
tokensLeft = 0
numericTokCnt = 0
# Read and tokenize each line
for line in sys.stdin:
    cnt = 0
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

    # Counting total tokens
    for tok in cleanLine:
        tok = tok.strip().replace('"','').replace("'","")
        if tok.isdigit():
            numericTokCnt +=1
        cnt += 1
    tokCnt += cnt
    # Remove Duplicate tokens or not
    if removeDuplicateTokens:
        cleanLine = list(dict.fromkeys(cleanLine))
    tokensLeft = tokensLeft + len(cleanLine)
    
    for n,key in enumerate(cleanLine,1): #n is a num/position using enumerate and starts from 1
        # Generate json-like output with metadata information
        mypair = {'refID':value, 
                'pos': n, 
                'tok': key}
        mypair = str(mypair).replace('(', '').replace(')', '')
        print('%s | %s | %s'% (key, mypair, 'one')) 

removedTokens = tokCnt - tokensLeft
# Reporting to logfile
print('   Total References Read: ', refCnt, file=logfile)
print('   Total Tokens Found: ', tokCnt, file=logfile)
print('   Duplicate Tokens Found: ', removedTokens, file=logfile)
print('   Tokens Left after Duplicates Removal: ', tokensLeft, file=logfile)
print('   Total Numeric Tokens Found: ', numericTokCnt, file=logfile)
############################################################
#               END OF MAPPER       
############################################################
