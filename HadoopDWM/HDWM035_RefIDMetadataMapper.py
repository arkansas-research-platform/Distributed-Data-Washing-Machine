#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys
import re

 #########################################################
 #           REF_ID & REFERENCE MERGE MAPPER 
 # Takes as input, the pairs of refIDs after blocking and
 # the reformed refs then merges them
 # 
 #########################################################
isLinkedIndex = False
isUsedRef = False
for line in sys.stdin:
    # Setting some defaults
    key = -1    #default sorted as first
    identifier = -1   #default sorted as first
    constant = -1   #default sorted as first
    metadata = -1  #default sorted as first

    line = line.strip().replace('"','').replace("'","")
    #print(line)
    # Check if the ref has already been used
    if '*used*' in line:
        isUsedRef = True
        print('%s > %s'%('-1',line.strip().replace('-','>')) )
        continue
    # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
    if 'GoodCluster' in line:
        isLinkedIndex = True
        print('%s > %s'%('-1',line.strip().replace('-','>'))) 
        continue
    splits = line.split("-")
    #print(splits)
    
    if len (splits) == 1:
        continue
    elif len(splits) == 2:  # The refID pairs
        is_two = splits
        key = is_two[0]
        identifier = is_two[1]
        #print(is_two)
    else:
        is_three = splits   # The refID and 
        key = is_three[0]
        metadata = is_three[1]
        constant = is_three[2]
    print('%s > %s > %s' % (key, metadata, identifier))
############################################################
#               END OF MAPPER       
############################################################