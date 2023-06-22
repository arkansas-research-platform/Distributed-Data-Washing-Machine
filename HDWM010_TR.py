#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import itertools
import sys
import os
from pathlib import Path
from operator import itemgetter
 #########################################################
 #        TOKENIZATION Reducer (Frequency Computation)
 # Takes input comes from the output of Tokenization mapper
 # Computes the frequencies of each of the tokens keys.     
 #########################################################
# Loading the Log_File from the bash driver
#logfile = open(os.environ["Log_File"],'a')
#logfile = open('/usr/local/jobTmp/HDWM_log.txt', 'a')
with open('path.txt', 'r') as p:
    localLogLocation = str(p.readline()).strip()
logfile = open(localLogLocation, "a")

# This is the current word key
current_tok_key = None 
# Current word value (refID)                        
current_metadata = None   
tok_key = None  
current_count = 0 
uniqueTokCnt = 0
# Read the data from STDIN (the output from mapper)
for items in sys.stdin:
    line = items.strip()

    # First phase, print all the input that came in (this part replaced Job 1 in the old code)
    print(line)

    # Second phase: calculate frequency
    file = line.replace('(', '').replace(')', '').split("|" , 1) #max split is set to 1 to split on only the first comma
    #print(file)
    tok_key = file[0]
    #print(tok_key)
    metadata = file[1]

    if current_tok_key == tok_key:
        #print ('%s , %s' % (tok, current_count))
        current_count += 1
    else:
        if current_tok_key:
	  # Write result to STDOUT
            print ('%s | %s' % (current_tok_key, current_count))
            uniqueTokCnt +=1
        #current_metadata = metadata
        current_count = 1
        current_tok_key = tok_key
      
# Output the last word
if current_tok_key == tok_key:
    print ('%s | %s' % (current_tok_key, current_count)) 
    uniqueTokCnt +=1

# Reporting to logfile
print('   Unique Tokens Found: ', uniqueTokCnt, file=logfile)
############################################################
#               END OF MAPPER       
############################################################
