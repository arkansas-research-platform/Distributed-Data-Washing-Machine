#!/usr/bin/env python

import math

pdpDict = {}
pdpBDict = {}

logger = open('PDP-Stats.txt', 'a')

with open('PDP_Collector.txt', 'r') as pdp:
    line = pdp.readlines()

for items in line:
    item = items.strip().replace(' ','').replace('=',':')
    itemVal = item.split(':')

    if len(itemVal) == 2:
        k = itemVal[0].strip()
        v = itemVal[1].strip()

        if k == 'TokensFound':
            pdpBDict['tokenCnt'] = v
        if k == 'UniqueTokens':
            pdpBDict['uniqueTokenCnt'] = v
        if k == 'NumericTokens':
            pdpBDict['numTokenCnt'] = v
        if k == 'ReferencesRead':
            pdpDict['refCnt'] = v
        if 'TotalMuCnts' in k:
            pdpDict['totalMuCounts'] = int(v.replace(',', ''))
        if 'NumberofPairsLinked' in k:
            pdpDict['greaterThanMu'] = int(v.replace(',', ''))
        if 'GreaterThanEpsilon' in k:
            pdpDict['greaterEpsilon'] = int(v.replace(',', ''))
        if 'LinkedPairs' in k:
            pdpDict['linkedPairs'] = int(float(v))
            pdpBDict['linkedPairs'] = v
        if 'TruePositivePairs' in k:
            pdpDict['truePairs'] = int(float(v))
            pdpBDict['truePairs'] = v
        if 'EquivalentPairs' in k:
            pdpDict['expectedPairs'] = int(float(v))
            pdpBDict['expectedPairs'] = v
        if 'Precision' in k:
            pdpDict['precision'] = float(v)
            pdpBDict['precision'] = v
        if 'Recall' in k:
            pdpDict['recall'] = float(v)
            pdpBDict['recall'] = v
        if 'F-score' in k:
            pdpDict['fMeasure'] = float(v)
            pdpBDict['fMeasure'] = v
        if 'Mean' in k:
            pdpBDict['avgFreq'] = v
        if 'Standard' in k:
            pdpBDict['stdFreq'] = v

uniqueTokenRatio = round(int(pdpBDict.get('uniqueTokenCnt').replace(',', ''))/int(pdpBDict.get('tokenCnt').replace(',', '')), 4)
pdpBDict['uniqueTokenRatio'] = str(uniqueTokenRatio)
numTokenRatio = round(int(pdpBDict.get('numTokenCnt').replace(',', ''))/int(pdpBDict.get('tokenCnt').replace(',', '')), 4)
pdpBDict['numTokenRatio'] = str(numTokenRatio)

print('pdpDict = ', pdpDict, file=logger)
print('pdpBDict = ', pdpBDict, file=logger)
logger.close()