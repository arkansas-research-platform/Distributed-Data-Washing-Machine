#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import re

 #########################################################
 #                  DEVELOPER's NOTES
 #            --JOIN Mapper (MapSide Join)-- 
 #  Input comes from a combination of the file with the 
 #  tokens and their embedded metadata (job 1 output), and
 #  the file with the frequency information (job 2 output).  
 #  The program then identifies tokens that have metadata 
 #  info and those with the frequency info to have uniform
 #  info (key, mdata, freq) for the reducer to do the updates.                      
 #########################################################

for line in sys.stdin:
    # Setting some defaults
    key = -1    #default sorted as first
    mdata = -1  #default sorted as first
    freq = -1   #default sorted as first

    line = line.strip()
    splits = line.split("|")

    if len(splits) == 3: # tokens with metadata info
        key = splits[0]      #token key
        mdata = splits[1]    #metadata
    else:                # tokens with Frequency info
        key = splits[0]      #token key
        freq = splits[1]     #counts info   

    print ('%s|%s|%s' % (key, mdata, freq))
        
############################################################
#               END OF MAPPER       
############################################################
