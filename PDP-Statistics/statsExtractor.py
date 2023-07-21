#!/usr/bin/env python

import math

pdpDict = {}
pdpBDict = {}

# Values that will occure multiple times in program loop
#muList = []
#pairLinklist = []
#clusEvalList = []
muCnt = 0
pairLinkCnt = 0
clusEvalCnt = 0

logger = open('PDP-Dictionary.txt', 'a')

with open('PDP_Collector.txt', 'r') as pdp:
    line = pdp.readlines()

for items in line:
    item = items.strip().replace(' ','').replace('=',':')
    itemVal = item.split(':')

    if len(itemVal) == 2:
        k = itemVal[0].strip()
        v = itemVal[1].strip()

        # Stats for pdpBDict (string, string)
        if k == 'TokensFound':
            pdpBDict['tokenCnt'] = str(v)
        if k == 'UniqueTokens':
            pdpBDict['uniqueTokenCnt'] = str(v)
        if k == 'NumericTokens':
            pdpBDict['numTokenCnt'] = str(v)
        if 'Max' in k:
            pdpBDict['maxTokenFreq'] = str(v)
        if 'Mean' in k:
            pdpBDict['avgFreq'] = str(v)
        if 'Standard' in k:
            pdpBDict['stdFreq'] = str(v)
        if 'Precision' in k:
            pdpDict['precision'] = float(v)
            pdpBDict['precision'] = str(v)
        if 'Recall' in k:
            pdpDict['recall'] = float(v)
            pdpBDict['recall'] = str(v)
        if 'F-score' in k:
            pdpDict['fMeasure'] = float(v)
            pdpBDict['fMeasure'] = str(v)

        # Stats for pdpDict (string, Integer) 
        if 'References' in k:
            pdpDict['refCnt'] = int(v.replace(',', ''))
        if 'TotalMuCnts' in k:
            muCnt += int(v.replace(',', ''))
            #muList.append(int(v.replace(',', '')))
            pdpDict['totalMuCounts'] = muCnt
        if 'NumberofPairsLinked' in k:
            pairLinkCnt += int(v.replace(',', ''))
            #pairLinklist.append(int(v.replace(',', '')))
            pdpDict['greaterThanMu'] = pairLinkCnt
        if 'GreaterThanEpsilon' in k:
            clusEvalCnt += int(v.replace(',', ''))
            #clusEvalList.append(int(v.replace(',', '')))
            pdpDict['greaterEpsilon'] = clusEvalCnt
        if 'LinkedPairs' in k:
            pdpDict['linkedPairs'] = int(float(v))
            pdpBDict['linkedPairs'] = int(v.replace(',', '').replace('.0', ''))
        if 'TruePositivePairs' in k:
            pdpDict['truePairs'] = int(float(v))
            pdpBDict['truePairs'] = int(v.replace(',', '').replace('.0', ''))
        if 'EquivalentPairs' in k:
            pdpDict['expectedPairs'] = int(float(v))
            pdpBDict['expectedPairs'] = int(v.replace(',', '').replace('.0', ''))


# Additional Stats for pdpBDict (string, string)
uniqueTokenRatio = round(int(pdpBDict.get('uniqueTokenCnt').replace(',', ''))/int(pdpBDict.get('tokenCnt').replace(',', '')), 4)
pdpBDict['uniqueTokenRatio'] = str(uniqueTokenRatio)
numTokenRatio = round(int(pdpBDict.get('numTokenCnt').replace(',', ''))/int(pdpBDict.get('tokenCnt').replace(',', '')), 4)
pdpBDict['numTokenRatio'] = str(numTokenRatio)

print('pdpDict = ', pdpDict, file=logger)
print('pdpBDict = ', pdpBDict, file=logger)
#print(muList, file=logger)
logger.close()