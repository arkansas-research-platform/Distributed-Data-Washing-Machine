#!/usr/bin/env python
import sys
import string
 #########################################################
 #                  DEVELOPER's NOTES
 # EEPR - takes output from ERMM & EEMR and and calculate
 # Total Equivalent Pairs
 #########################################################
def countPairs(cnt):
    #totalPairs = 0
    pairs = cnt*(cnt-1)/2
        #totalPairs +=pairs
    return pairs

# maps words to their counts
totalEquivalentPairs = 0
currTIDcount=0
tID = None
currTID = None

for line in sys.stdin:
    line = line.strip()
    linkID,truthID = line.split(',')
    #print(truthID)

    if currTID == truthID:
        #print ('%s , %s' % (tok, current_count))
        currTIDcount += 1
    else:
        if currTID:
            #print (currTID,currTIDcount)
            pairCount = countPairs(int(currTIDcount))
            #print(currTID,pairCount)
            totalEquivalentPairs += pairCount
        currTIDcount = 1
        currTID = truthID
      
# Output the last word
if currTID == truthID:
    #print (currTID,currTIDcount)
    pairCount = countPairs(int(currTIDcount))
    #print(currTID,pairCount)
    totalEquivalentPairs += pairCount

print('%s,%s,%s' % ('0',totalEquivalentPairs, 'equivalentPairs'))
############################################################
#               END OF REDUCER       
############################################################