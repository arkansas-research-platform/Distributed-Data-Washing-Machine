#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import re
import json
from collections import Counter
from itertools import count

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
    '''
    #Replace delimiter with blanks, then compress token by replacing non-word characters with null
    def tokenizerCompress(string):
        string = string.upper()
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
    def tokenizerSplitter(string):
        string = string.upper()
        string = re.sub('[\W]+',' ',string)
        tokenList = re.split('[\s]+',string)
        newList = []
        for token in tokenList:
            if len(token)>0:
                newList.append(token)
        return newList
    '''     
parameterFile = open('HDWM/parmStage.txt', 'r')
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
        #global delimiter
        if ',;:|\t'.find(parmValue)>=0:
            delimiter = parmValue
            continue

##############
# MAIN PROGRAM
##############
# Remove(skip) the header from the record if hasHeader is True
if hasHeader:
    next (sys.stdin)

for line in sys.stdin:
    # Convert dataset into uppercase
    uc_file = line.upper()
    #print(uppercase_file)    
    # Remove leading and trailing whitespaces
    unclean_file = uc_file.strip()
    #print(unclean_file)   
    # Remove unwanted characters
    file = re.sub('[\W]+', ' ', unclean_file)
    #print(file)    
    file_split = file.split()
    #print(file_split)

    value = file_split[0]
    for key in file_split[1:]:
        # Generate json-like output with metadata information (JSON style)
        mypair = {'refID':value, 
                                    'pos': file_split.index(key), 
                                    #'char':  character(key), 
                                    'tok': key}
        mypair = str(mypair).replace('(', '').replace(')', '')
        print('%s | %s | %s'% (key, mypair, 'one'))
        
############################################################
#               END OF MAPPER       
############################################################
