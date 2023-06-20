#!/usr/bin/env python
import sys
from collections import Counter

 #########################################################
 #                  DEVELOPER's NOTES
 # EFCR - takes output from EEPR and calculate
 # LP, TP, P, R, F-score
 #########################################################
def countPairs(cnt):
    #totalPairs = 0
    pairs = cnt*(cnt-1)/2
        #totalPairs +=pairs
    return pairs

totalEquivalentPairs = 0
totalERlinkPairs = 0                                     # Secong count needed (L)
totalTruePositives = 0                                   # Third count needed (TP)
currLinkIDcount=0
linkID = None
currlinkID = None
currTIDcount=0
tID = None
tIDgroup = None
currTID = None
overLap = False
group = False
truthID = None

for line in sys.stdin:
    line = line.strip()
    #print(line)
    keepLine = True
    if 'equivalent' in line:
        equivalentPairs = float(line.split(',')[1])
        #print(equivalentPairs)
        keepLine = False
    if 'ID' in line:
        keepLine = False
    #print(line)

    if keepLine:
        lineSplit = line.split(',')
        erLinkID = lineSplit[0].strip()
        truthID = lineSplit[1].strip()
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

# Output the last record
if currlinkID == erLinkID:
    #print (currlinkID,currLinkIDcount)
    pairCount = countPairs(int(currLinkIDcount))
    #print(currlinkID,pairCount)
    totalERlinkPairs += pairCount
    #print(currlinkID,tIDgroup)
    #print(tIDlist)
    tIDlist = [x for x in tIDgroup.split(',')]
    countTidGroup = {a:tIDlist.count(a) for a in tIDlist}
    for i in list(countTidGroup.values()):
        TPpairs = countPairs(i) 
        #print(tIDlist, i, TPpairs)
        totalTruePositives += TPpairs
#print('totalLinkedPairs =',totalERlinkPairs)  #total Linked Pairs (L)
#print('totalTruePositivePairs = ', totalTruePositives)  #total True Positive Pairs (TP)

# Output Final ER Matrix
print('Equivalent Pairs =', equivalentPairs)
print('Linked Pairs =',totalERlinkPairs)  
print('True Posiive Pairs =', totalTruePositives)
precision = round(float(totalTruePositives)/float(totalERlinkPairs),4)
print('Precision =', precision)
recall = recall = round(float(totalTruePositives)/float(equivalentPairs),4)
print('Recall =', recall)
fmeas = round((2*precision*recall)/(precision+recall),4)
print('F-score =', fmeas)
############################################################
#               END OF REDUCER       
############################################################