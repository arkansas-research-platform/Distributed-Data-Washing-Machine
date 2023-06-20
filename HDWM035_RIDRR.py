#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys 
import re
from operator import itemgetter
import os
 ##############################################################
 #                     DEVELOPER's NOTES
 #  Re-Grouping to get pairs back
 ##############################################################
# This is the current word key
currentIdentifier  = None 
# Current word value (identifier)                        
currentFullRef= None   
identifier = None   
tokenCnt = 0
isUsedRef = False
isLinkedIndex = False

 # Loading the Log_File from the bash driver
logfile = open(os.environ["Log_File"],'a')

# Read the data from STDIN (the output from mapper)
for items in sys.stdin:
    line = items.strip()
    #print(line)
    # Check if the ref has already been used
    if '*used*' in line:
        isUsedRef = True
        print(line.strip().replace(',','<>')) 
        continue
    # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
    if 'GoodCluster' in line:
        isLinkedIndex = True
        print((line.strip().replace(',','<>')))
        continue
    lineSplit = line.split("-")
    identifier = lineSplit[0].strip()
    FullRef = lineSplit[1].strip()
    #line.split(":", maxsplit=1)

    if currentIdentifier == identifier:
        currentFullRef = currentFullRef + "<>" + FullRef        
    else:
        if currentIdentifier:
            tokenCnt += 1
            print (currentFullRef)
        currentIdentifier = identifier
        currentFullRef= FullRef

# Output the last word
if currentIdentifier == identifier:
    tokenCnt += 1
    print (currentFullRef)

# Reporting to logfile
print('   Total Unduplicated Blocks: ', tokenCnt, file=logfile)
############################################################
#               END OF MAPPER       
############################################################