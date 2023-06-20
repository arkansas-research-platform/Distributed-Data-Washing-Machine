#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import re

 #########################################################
 #                  DEVELOPER's NOTES
 #            --Linked Index Mapper (MapSide Join)-- 
 #  Input comes from a combination of the file with the 
 #  final Transitive Closure output  (job 10 output) and 
 #  the output from the Full Records Reform (job 4 output).
 #  Goal: Get the remainder of the refIDs that did not end 
 # up in the Transitive Closure. This is the difference btn
 # the Full total references & TC outputs.
 # eg. If total ref = 1000 & TC output = 817(cluster size>1),
 # the remainder 183 (clusters of size = 1) will be added to
 # the 817. This will now show all clusters for the entire file                     
 #########################################################
isLinkedIndex = False
isUsedRef = False
for line in sys.stdin:
    # Setting some defaults
    refID = -1    #default sorted as first
    clusterID = -1  #default sorted as first
    value = -1   #default sorted as first
    mdata = -1
    tokenList = -1
    # Check if the ref has already been used
    if '*used*' in line:
        isUsedRef = True
        print(line.strip().replace('.','|').replace(' ','')) 
        continue
    # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
    if 'GoodCluster' in line:
        isLinkedIndex = True
        print((line.strip().replace('.','|').replace(' ','')))
        continue
    line = line.strip().replace('-',',').replace("'","")
    splits = line.split(',')
    #print(len(splits))

    if len(splits) == 2: # TC output
        compKey = splits[0].split('.')
        refID = compKey[1].strip()      
        clusterID = compKey[0].strip() + ' ClusterLine'
        value = splits[1]
    else:                # Original Data
        refID = splits[0].strip()      
        clusterID = refID.strip()  
        #mdata = str(splits[1:-1]).replace(' ','').replace("['{",'').replace("}']",'')#.split(',')
        #mdata = str(splits[1:-1]).replace(' ','').replace("[",'').replace("]",'')
        mdata = str(splits[1:-1]).replace(' ','').replace("[",'').replace("]",'')
        #print(mdata)
    print ('%s|%s.%s|%s' % (refID, value, clusterID, mdata))
############################################################
#               END OF MAPPER       
############################################################
