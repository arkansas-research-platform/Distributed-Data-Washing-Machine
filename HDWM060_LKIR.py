#!/usr/bin/env python
import sys
import string

 #########################################################
 #                  DEVELOPER's NOTES
 #              --Linked Index Reducer --               
 # Get the difference records between the TC output and 
 # Original reference, and also shows the out put from 
 # the TC output. The final output is (refIDs, clusterIDs)
 # from both the TC output and the Difference
 # Note: the cluster ID for each of the difference records
 # is the record itself. 
 #########################################################

# maps words to their counts
refID = "-1"
clusterID = "-1"
value = "-1"
currentRefID = None
lineToSkip = False
currClusterID = clusterID
currCount = 0

# Give a Header to the LinkedIndex file
print('RefID,ClusterID,constant')
for line in sys.stdin:
    line = line.strip()
    lineSplit = line.split(",")
    compKey = lineSplit[0].strip().split('.')
    value = lineSplit[1].strip()
    refID = compKey[0]
    clusterID = compKey[1]

    # Get the records that were not linked to any record (singleton clusters)
    if currentRefID == refID:
        currCount +=1
        currClusterID = clusterID
    else:
        if currentRefID:
            if currCount > 1:
                lineToSkip = True
            else: 
                print('%s,%s,%s'%(currentRefID,currClusterID,'x'))
        currentRefID = refID
        currClusterID = clusterID
        currCount = 1
    
    # Get the clusters that came out of Transitive closure
    if value != "-1":
        print('%s,%s,%s'%(refID,clusterID,'x'))

# Get Last Record
if currentRefID:
    if currCount > 1:
        lineToSkip = True
    else:
        print('%s,%s,%s'%(currentRefID,currClusterID,'x'))
############################################################
#               END OF REDUCER       
############################################################