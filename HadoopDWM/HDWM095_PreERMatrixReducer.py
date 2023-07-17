#!/usr/bin/env python
import sys
import string
from operator import itemgetter
 #########################################################
 #                  DEVELOPER's NOTES
 # ERMR - takes output from ERMM and 
 #########################################################

def countPairs(pair):
    totalPairs = 0
    for cnt in pair:
        pairs = cnt*(cnt-1)/2
        totalPairs +=pairs
    return totalPairs

def PreMatrixReduce():
    # maps words to their counts
    currentRefID = "-1"
    currentTruthID = "-1"
    isTruthIDMappingLine = False
    currTIDcount=0
    tID = None
    currTID = None

    for line in sys.stdin:
        line = line.strip()
        refID,clusterID,truthID = line.split(',')
        #print(truthID)

        # First line should be a mapping line that contains 
        # the truthID information for that key group
        if clusterID == "-1":  #That is a line with truthID info that will be used to map
            currentTruthID = truthID
            currentRefID = refID
            isTruthIDMappingLine = True

        else:
            isTruthIDMappingLine = False        
    #    print ('%s|%s|%s' % (linkID,refID,currentTruthID))
        if clusterID == "-1":  #Remove all previous frequency info line because they have been used to map
            continue
        #print ('%s,%s,%s' % (refID,clusterID,currentTruthID))
        # Only interested in linkedID and truthID
        print('%s,%s,%s'%(currentTruthID, '0', '0'))            # 1st outpt used to calculate Equivalent Pairs
        print ('%s,%s'%(clusterID,currentTruthID)) # 2nd output for L-pairs, TP-pairs

if __name__ == '__main__':
    PreMatrixReduce()  
############################################################
#               END OF REDUCER       
############################################################