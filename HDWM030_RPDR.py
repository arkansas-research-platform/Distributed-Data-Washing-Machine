#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import re
from itertools import groupby
 #########################################################
 #           RECORD PAIRS DEDUPLICATION REDUCER 
 # Takes the pairs of refIDs and deduplicates it to obtain
 # unique pairs of refIDs which will be sent to the 
 # Similarity comparison stage
 #########################################################
# Read the data from STDIN and use the lambda function to
# spit out the pair_key from every group   
for key, group in groupby(sys.stdin, key = lambda x: x[0:]):
    #print(key) 
    ref = (key.strip()).split(':')
    #print(ref)
    ref_pair = ref[0]
    refPairSplit = ref_pair.split(',')
    identifier = ((refPairSplit[0].strip()+refPairSplit[1].strip()).replace("'",''))
    #print(identifier)
    refPairWithIdentifier = '%s - %s' % (ref_pair, identifier)
    #print(ref_pair)
    #print(refPairWithIdentifier)
    for refID in refPairSplit:
        refIDwithIdentifier = '%s - %s' % (refID.strip(), identifier)
        print(refIDwithIdentifier)
############################################################
#               END OF REDUCER    
############################################################