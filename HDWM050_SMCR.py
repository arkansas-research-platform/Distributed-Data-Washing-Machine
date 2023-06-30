#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys 
import re
import os
from operator import itemgetter
from pathlib import Path

#Note: The line below is important in order for Hadoop to recognize the 'textdistance' library
#       Ignoring this line will cause the job to fail ( 4 days researching to discover this )
#'/home/nick/.local/lib/python3.10/site-packages'
textdistanceDir = '/usr/local/lib/python*/dist-packages'
sys.path.append(textdistanceDir)

from textdistance import DamerauLevenshtein
from textdistance import Cosine
from textdistance import MongeElkan
 ##############################################################
 #                     DEVELOPER's NOTES
 #  Similarity computation using Matrix Comparator after 
 #  removing stopwords, Linking Equivalent pair of references 
 #  by comparing similarity score with Mu Threshold. 
 #  Output is a list(rows) of linked pairs
 #  Reducer Job only
 ##############################################################
############################

def convertToBoolean(value):
    if value=='True':
        return True
    if value=='False':
        return False


# Loading muReport file from Distributed Cache
with open('muReport.txt', 'r') as openMuFile:
    muVal = str(openMuFile.readline()).strip()
mu = float(muVal)

####### READ PARAMETER FILE #######
#parameterFile = open('S9P-parms-copy.txt', 'r')  #Delete this line. Only used in Terminal
#parameterFile = open('parmStage.txt', 'r') #Add back this line. Used by HDFS    
parameterFile = open('parms', 'r') 

while True:
    pline = (parameterFile.readline()).strip()
    if pline == '':
        break
    if pline.startswith('#'):
        continue
    if pline.find('=')<0:
        continue
    part = pline.split('=')
    parmName = part[0].strip()
    parmValue = part[1].strip()
    #if parmName == 'mu':
    #    mu = float(parmValue)
    #    continue
    if parmName == 'sigma':
        sigma = int(parmValue)
        continue    
    if parmName == 'matrixNumTokenRule':
        matrixNumTokenRule = convertToBoolean(parmValue)
        continue 
    if parmName == 'matrixInitialRule':
        matrixInitialRule = convertToBoolean(parmValue)
        continue 
    if parmName=='removeExcludedBlkTokens':
        removeExcludedBlkTokens = convertToBoolean(parmValue)
        continue 
    if parmName=='removeDuplicateTokens':
        removeDuplicateTokens = convertToBoolean(parmValue)
        continue 
    if parmName=='minBlkTokenLen':
        minBlkTokenLen = int(parmValue)
        continue
    if parmName=='excludeNumericBlocks':
        excludeNumericBlocks = convertToBoolean(parmValue)
        continue         
    if parmName=='comparator':
        comparator = parmValue
        continue 
############################

########### Remove StopWords Function ###############
def removestopwords(RefTokenList):
    cleanList = []
    for x in RefTokenList:
        carryToken = True
        tokenRef = x.split("^")[0].strip().replace('"','').replace("'","")
        #print(tokenRef)
        freqRef = x.split("^")[1].strip().replace('"','').replace("'","")
        #print(freqRef)
        # Remove all tokens with freq higher or equal to sigma
        if int(freqRef) >= sigma:
            carryToken = False
        # Check for other rules (Duplicate tokens, Excluded Block Tokens)
        if removeDuplicateTokens and (tokenRef in cleanList):
            carryToken = False  
        if removeExcludedBlkTokens:
            if len(tokenRef) < minBlkTokenLen:
                carryToken = False
                #print('tokenRef, len(tokenRef))
            if excludeNumericBlocks and tokenRef.isdigit():
                carryToken = False
                #print('tokenRef)     
        if carryToken:
            cleanList.append(tokenRef)
    return cleanList
########### End of Remove StopWords Function ###############

##### ScoringMatrixStd Function #####
def scoringMatrix_Std(refList1, refList2):
    #print(refList1,'***',refList2)
    Class = DamerauLevenshtein()
    score = 0.0  
    m = len(refList1)
    n = len(refList2)
    #print(m, '***', n)
    if m==0 or n==0:
        return score
    #generate m x n matrix
    matrix = [[0.0 for j in range(n)] for i in range(m)]
    #print(matrix)
    maxVal = -1.0
    #populate matrix with similarities between tokens
    for j in range(0,m):
        token1 = refList1[j].strip().replace('"','').replace("'","")
        for k in range(0,n):
            token2 = refList2[k].strip().replace('"','').replace("'","")
            simVal = 0.0
            #print('-Comparing',token1, token2)
            # Numeric Token Rule, if both tokens numeric, only exact match
            if matrixNumTokenRule:      
                if token1.isdigit() and token2.isdigit():
                    if token1==token2:
                        simVal = 1.0
                    else:
                        simVal = 0.0
                    #print('*Fired Rule 1', j, k, simVal)
                    matrix[j][k] = simVal                  
                    continue
            # Initial Rule, if either token length 1, only exact match
            if matrixInitialRule:            
                if len(token1)==1 or len(token2)==1:
                    if token1==token2:
                        simVal = 1.0
                    else:
                        simVal = 0.0
                    #print('*Fired Rule 2',j, k, simVal)
                    matrix[j][k] = simVal             
                    continue
            # Default Rule, otherwise use Damerau-levesthein distance
            simVal = Class.normalized_similarity(token1,token2)
            #print(simVal)
            #print('*Fired Rule 3', j, k, simVal)
            matrix[j][k] = simVal
    #end of matrix population       
    loops = 0 
    total = 0.0
    while True:
        maxVal = -1.0
        # search for maximum value in matrix
        for j in range(m):
            for k in range(n):
                if matrix[j][k]>maxVal:
                    maxVal = matrix[j][k]
                    saveJ = j
                    saveK = k
        #print('-*Max Value ', maxVal, ' found at ', saveJ, saveK)
        if maxVal < 0:
            #print('-Normal Ending no more postive values, loops =', loops, score)
            return score
        total = total + maxVal
        loops +=1
        score = total/loops
        if score < mu:
            #print('-Ending because score below mu =',loops, score)
            return score
        # set column saveK values to -1.0
        for j in range(m):
            matrix[j][saveK] = -1.0
        # set row saveJ values to -1.0
        for k in range(n):
            matrix[saveJ][k] = -1.0  
    #end of while loop
    #print('-Should never see this message',loops, score)
    return score
########### End of ScoringMatrixStd Function ###############

##### ScoringMatrixKris Function #####
def scoringMatrix_Kris(refList1, refList2):
    Class = DamerauLevenshtein()
    #print('--Starting DWM66')
    # First make ref1 the shorter of the two lists.
    m = len(refList1)
    n = len(refList2)
    score = 0.0  
    if m==0 or n==0:
        return score
    if m <= n:
        ref1 = refList1
        ref2 = refList2
    else:
        ref1 = refList2
        ref2 = refList1
    # reset lengths m & n
    m = len(ref1)
    n = len(ref2)
    #print(ref1,'***',ref2)
    # set base for weight function to length of short (first) list
    base = float(m*(m+1)/2)    
    #print('base=',base)
    #generate m x n matrix
    matrix = [[0.0 for j in range(n)] for i in range(m)]
    maxVal = -1.0
    #populate matrix with similarities between tokens
    for j in range(0,m):
        token1 = ref1[j].strip().replace('"','').replace("'","")
        for k in range(0,n):
            token2 = ref2[k].strip().replace('"','').replace("'","")
            simVal = 0.0
            # Numeric Token Rule, if both tokens numeric, only exact match
            if matrixNumTokenRule:
                if token1.isdigit() and token2.isdigit():
                    if token1==token2:
                        simVal = 1.0
                    else:
                        simVal = 0.0
                    #print('*Fired Rule 1', j, k, simVal)
                    matrix[j][k] = simVal                  
                    continue
            # Initial Rule, if either token length 1, only exact match
            if matrixInitialRule:            
                if len(token1)==1 or len(token2)==1:
                    if token1==token2:
                        simVal = 1.0
                    else:
                        simVal = 0.0
                    #print('*Fired Rule 2',j, k, simVal)
                    matrix[j][k] = simVal             
                    continue
            #simVal = lev.ratio(token1.lower(),token2.lower())
            #simVal = damerauLevenshtein(token1.lower(),token2.lower())
            simVal = Class.normalized_similarity(token1,token2)
            #print('*Fired Rule 3', j, k, token1, token2, simVal)
            matrix[j][k] = simVal                
    #end of matrix population       
    loops = 0 
    total = 0.0
    while True:
        maxVal = -1.0
        # search for maximum value in matrix
        for j in range(m):
            for k in range(n):
                if matrix[j][k]>maxVal:
                    maxVal = matrix[j][k]
                    saveJ = j
                    saveK = k
        #print('-*Max Value ', maxVal, ' found at ', saveJ, saveK)
        if maxVal < 0:
            #print('check: loops=',loops,' m =', m)
            #print('-Normal Ending no more postive values, loops =', loops, score)
            return score
        numerator = m - saveJ
        wgtSim = maxVal*float(numerator)/base  
        score = score + wgtSim
        #print('saveJ=',saveJ,'numer=',numerator,'wgtSim=',wgtSim,'score=',score)
        loops +=1
        #global mu
        #if score < mu:
            #print('-Ending because score below mu =',loops, score)
            #return score
        # set column saveK values to -1.0
        for j in range(m):
            matrix[j][saveK] = -1.0
        # set row saveJ values to -1.0
        for k in range(n):
            matrix[saveJ][k] = -1.0  
    #end of while loop
    #print('-Should never see this message',loops, score)
    return score
########### End of ScoringMatrixStd Function ###############

############################################################
#                     MAIN PROGRAM     
############################################################
###### Input Prepping ######
isLinkedIndex = False
isUsedRef = False
links = 0

for pairList in sys.stdin:
    stripPairs = pairList.strip().replace('\t','.')
    # Check if the ref has already been used
    if '*used*' in stripPairs:
        isUsedRef = True
        print(stripPairs.strip()) 
        continue
    # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
    if 'GoodCluster' in stripPairs:
        isLinkedIndex = True
        print(stripPairs.strip())#.replace('<>',',')))
        continue
    splitPairList = stripPairs.split('<>')
                # Right-side (Ref1) Prep
    reference1 = splitPairList[0].split(",", maxsplit=1)
    refID1 = reference1[0].strip().replace("'","")
    ref1 = reference1[1].strip()
    full_ref1 = ref1.replace("{", "").replace("}", "").split(",")  #ref with position info
    #List of token & Freq before stopword removal, to get the position info,use tokFreq.split(":")[0]
    Ref1TokenFreq = [tokFreq.split(":")[1].strip().replace("'" , "") for tokFreq in full_ref1]
                # Left-side (Ref2) Prep
    reference2 = splitPairList[1].split(",", maxsplit=1)
    refID2 = reference2[0].strip().replace("'","")
    ref2 = reference2[1].strip()
    full_ref2 = ref2.replace("{", "").replace("}", "").split(",") #ref with position info
    #List of token & Freq before stopword removal, to get the position info,use tokFreq.split(":")[0]
    Ref2TokenFreq = [tokFreq.split(":")[1].strip().replace("'" , "") for tokFreq in full_ref2] 

    # Debugging Lines
    #print(reference1,'**',reference2)
    #print(refID1,'**',refID2)
    #print(ref1,'**',ref2)
    #print(full_ref1,'**',full_ref2)
    #print(Ref1TokenFreq, '**', Ref2TokenFreq)
    #print('Count of Tokens before Stopwords Removal: ',len(Ref1TokenFreq), '**', len(Ref2TokenFreq)) 

###### Remove all Stopword using frequency information ###### 
    refList1 = removestopwords(Ref1TokenFreq)
    refList2 = removestopwords(Ref2TokenFreq)
    #print(refID1, '-', refList1, '**', refID2, '-', refList2)
    #print('Count of Tokens after Stopwords Removal: ', refID1, '-', len(refList1), '**', refID2, '-', len(refList2))

###### Get Similarity comparison Score using Scoring Matrix ###### 
    if comparator == 'MongeElkan':
        similarity_comparison = MongeElkan(refList1, refList2)
    if comparator == 'Cosine':
        similarity_comparison = Cosine(refList1, refList2)
    if comparator == 'ScoringMatrixStd':
        similarity_comparison = scoringMatrix_Std(refList1, refList2)
    if comparator == 'ScoringMatrixKris':
        similarity_comparison = scoringMatrix_Kris(refList1, refList2)
    #print('Similarity Between:', refID1, '**', refID2, 'is', similarity_comparison)  

###### Compare Sim_Score with Mu to determnine Match/No match ###### 
    if similarity_comparison >= mu:  #Link or no-Link decision
        #print('Linked Pair:', refID1, '**', refID2, 'has a similarity of:', similarity_comparison)
        # Outpout the original linked pairs and their inverse, which will be the input 
        # for the Transitive Closure algorithm in the next reducer
        linkedPairs = '%s.%s,%s' % (refID1,refID2,refID2) # Original Linked Pairs 
        # Reporting to MapReduce Counter
        sys.stderr.write("reporter:counter:Linking Counters,Linked Pairs,1\n")
        links += 1 
        inversedLinkedPairs = '%s.%s,%s' % (refID2, refID1,refID1) # Inverted Linked Pairs
        pairSelf = '%s.%s,%s' % (refID1,refID1,refID1) # PairSelf
        print(linkedPairs)   
        print(inversedLinkedPairs) 
        print(pairSelf) 
    else:
        # Reporting to MapReduce Counter
        sys.stderr.write("reporter:counter:Linking Counters,Linked Pairs,0\n")
############################################################
#               END OF REDUCER      
############################################################
