#!/usr/bin/env python
import sys
import string
import os
from operator import itemgetter
 #########################################################
 #                  DEVELOPER's NOTES
 # EEPR - takes output from ERMM & EEMR and and calculate
 # Total Equivalent Pairs
 #########################################################
def countPairs(cnt):
    pairs = cnt*(cnt-1)/2
    return pairs

# Loading the Log_File from the bash driver
#logfile = open(os.environ["Log_File"],'a')
#logfile = open('/usr/local/jobTmp/HDWM_log.txt', 'a')
with open('path.txt', 'r') as p:
    localLogLocation = str(p.readline()).strip()
logfile = open(localLogLocation, "a")
#print(file)
print('\n>> Starting ER Matrix Process', file=logfile)

# maps words to their counts
onlyTruthIDs = True
truthIDandClusterID = True
totalEquivalentPairs = 0                                # First count needed (E-pairs)
cTIDcount=0
tID = None
cTID = None

totalERlinkPairs = 0                                     # Second count needed (L-pairs)
totalTruePositives = 0                                   # Third count needed (TP-pairs)
currLinkIDcount=0
currlinkID = None
tIDgroup = None
truthID = None

for line in sys.stdin:
    line = line.strip().split(',')

    # ---- Phase 1: Calculate E-pairs ----
    if len(line) == 3:  # This is the line containing only TruthID
        onlyTruthIDs
        tID = line[0]
        if cTID == tID:
            #print ('%s , %s' % (tok, current_count))
            cTIDcount += 1
        else:
            if cTID:
                #print (cTID,cTIDcount)
                cnt = int(cTIDcount)
                pairCount = countPairs(cnt)
                #print(cTID,pairCount)
                totalEquivalentPairs += pairCount
            cTIDcount = 1
            cTID = tID

    # ---- Phase 2: Calculate L-pairs, TP-pairs ----
    else: # This is the line containing both ClusterID and TruthID
        truthIDandClusterID
        erLinkID = line[0].strip()
        truthID = line[1].strip()
        #print(lineSplit)
        # Counting ER linked pairs
        if currlinkID == erLinkID:
            currLinkIDcount += 1
            tIDgroup = tIDgroup+','+truthID
        else:
            if currlinkID:
                #print (currlinkID,currLinkIDcount)
                #  Count total linked records
                pairCount = countPairs(int(currLinkIDcount))
                #print(currlinkID,pairCount)
                totalERlinkPairs += pairCount

                # Count all truthIDs in each linked group
                #print(currlinkID,tIDgroup)
                tIDlist = [x for x in tIDgroup.split(',')]
                #print(tIDlist)
                countTidGroup = {a:tIDlist.count(a) for a in tIDlist}
                #print(currlinkID,countTidGroup)
                # Get the pairs in each group
                for i in list(countTidGroup.values()):
                    #print(i)
                    TPpairs = countPairs(i) 
                    #print(tIDlist, i, TPpairs)
                    #print(TPpairs)
                    # Third Count needed: Get Total True Positive Pairs
                    totalTruePositives += TPpairs
            currLinkIDcount = 1
            currlinkID = erLinkID
            tIDgroup = truthID

# Output the last record in the E-pairs
if cTID == tID:
    cnt = int(cTIDcount)
    pairCount = countPairs(cnt)
    #print(cTID,pairCount)
    totalEquivalentPairs += pairCount

## Output the last record in the L,TP pairs
if currlinkID == erLinkID:
    pairCount = countPairs(int(currLinkIDcount))
    totalERlinkPairs += pairCount
    tIDlist = [x for x in tIDgroup.split(',')]
    countTidGroup = {a:tIDlist.count(a) for a in tIDlist}
    for i in list(countTidGroup.values()):
        TPpairs = countPairs(i) 
        totalTruePositives += TPpairs
        
print('Linked Pairs =',totalERlinkPairs)  #total Linked Pairs (L)
print('   Linked Pairs =',totalERlinkPairs, file=logfile)
print('True Positive Pairs = ', totalTruePositives)  #total True Positive Pairs (TP)
print('   True Positive Pairs = ', totalTruePositives,file=logfile)
print('Equivalent Pairs = ', totalEquivalentPairs)  #total Equivalent Pairs (TP)
print('   Equivalent Pairs = ', totalEquivalentPairs,file=logfile)

# ---- Phase 3: Calculate Precision, Recall, F-score ----
precision = round(float(totalTruePositives)/float(totalERlinkPairs),4)
recall = recall = round(float(totalTruePositives)/float(totalEquivalentPairs),4)
fmeas = round((2*precision*recall)/(precision+recall),4)
print('Precision =', precision)
print('   Precision =', precision,file=logfile)
print('Recall =', recall)
print('   Recall =', recall,file=logfile)
print('F-score =', fmeas)
print('   F-score =', fmeas,file=logfile)
############################################################
#               END OF REDUCER       
############################################################