#!/usr/bin/env python
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
def ClusterRemainRef():
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
    ##        print((line.strip().replace('.','|').replace(' ','')))
            copyRef = line.strip().replace('.','|').replace(' ','').split('<>')
            refID = copyRef[1].strip()
            clusterID = copyRef[2].strip()
            oldTag = copyRef[3].strip()
            main = '%s|%s.%s|%s' % (refID,clusterID,oldTag,'-1')
            copy = '%s|%s.%s%s|%s' % (clusterID,refID,oldTag.replace('GoodCluster','DeleteLine'),'copy','-1')
            print(main)
            print(copy)
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

if __name__ == '__main__':
    ClusterRemainRef()  
############################################################
#               END OF MAPPER       
############################################################
