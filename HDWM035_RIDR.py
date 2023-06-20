#!/usr/bin/env python
import sys
import os
 #########################################################
 #                  DEVELOPER's NOTES
 #   --JOIN Reducer (Update metadata with Frequency)--               
 # Takes the output from the Join Mapper and updates the 
 # metadata information by adding frequency info to rows.
 # A one-to-many join operation happens to add frequency 
 # information 
 #########################################################

# maps words to their counts
currentKey = -1    #default sorted as first
identifier = -1   #default sorted as first
constant = -1   #default sorted as first
currentMetadata = -1  #default sorted as first
currentIdentifier = ''
currentReference = None
isMetadataMappingLine = False
isUsedRef = False
for line in sys.stdin:
    # Check if the ref has already been used
    if '*used*' in line:
        isUsedRef = True
        print(line.strip().replace('>',',')) 
        continue
    # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
    if 'GoodCluster' in line:
        isLinkedIndex = True
        print((line.strip().replace('>',',')))
        continue
    line = line.strip().split(">")
    key = line[0].strip()
    metadata = line[1].strip()
    identifier= line[2].strip()
    #print(metadata)

    # First line should be a mapping line that contains 
    # the frequency information for that key group
    if identifier == "-1":  #That is a line with mdata to be used to map
        currentMetadata = metadata
        currentKey = key
        isMetadataMappingLine = True
        #print(key2,currentMdata1)
    else:
        isMetadataMappingLine = False

    if identifier == "-1":  #Remove all previous frequency info line because they have been used to map
        continue
    #print (identifier, key, currentMetadata)
    #join = '%s : %s : %s' % (identifier, key, currentMetadata)
    #print(join)
    print('%s - %s , %s' % (identifier, key, currentMetadata))
############################################################
#               END OF REDUCER       
############################################################