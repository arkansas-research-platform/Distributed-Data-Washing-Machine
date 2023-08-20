#!/usr/bin/env /usr/bin/python3.10
# coding: utf-8

# Importing libraries
import sys 
import re
import os
from operator import itemgetter

# Read Parms file
DWM10_Parms.getParms('parmStage.txt')

# Making DWM modules available for MapReduce
#Note: The line below is important in order for Hadoop to recognize the 'textdistance' library
#       Ignoring this line will cause the job to fail ( 4 days researching to discover this )
#'/home/nick/.local/lib/python3.10/site-packages'
textdistanceDir='/usr/local/lib/python3.10/dist-packages'
sys.path.append(textdistanceDir)
sys.path.append('DWM-Modules.zip')
#sys.path.append('textdistance.zip')

import DWM10_Parms 
import StopWord
import DWM65_ScoringMatrixStd
import DWM66_ScoringMatrixKris
import textdistance

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

# Loading muReport file from Distributed Cache
with open('muReport.txt', 'r') as openMuFile:
    muVal = str(openMuFile.readline()).strip()
mu = float(muVal)

 # Get Parameter values
sigma = DWM10_Parms.sigma
removeExcludedBlkTokens = DWM10_Parms.removeExcludedBlkTokens
removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens
minBlkTokenLen = DWM10_Parms.minBlkTokenLen
excludeNumericBlocks = DWM10_Parms.excludeNumericBlocks
comparator = DWM10_Parms.comparator
matrixNumTokenRule = DWM10_Parms.matrixNumTokenRule
matrixInitialRule = DWM10_Parms.matrixInitialRule
############################################################
#                     MAIN PROGRAM     
############################################################
def SimilarityComparison():
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
        refList1 = StopWord.removestopwords(Ref1TokenFreq)
        refList2 = StopWord.removestopwords(Ref2TokenFreq)
        #print(refID1, '-', refList1, '**', refID2, '-', refList2)
        #print('Count of Tokens after Stopwords Removal: ', refID1, '-', len(refList1), '**', refID2, '-', len(refList2))

    ###### Get Similarity comparison Score using Scoring Matrix ###### 
        if comparator == 'MongeElkan':
            similarity_comparison = MongeElkan.normalized_similarity(refList1, refList2)
        if comparator == 'Cosine':
            similarity_comparison = Cosine.normalized_similarity(refList1, refList2)
        if comparator == 'ScoringMatrixStd':
            similarity_comparison = DWM65_ScoringMatrixStd.normalized_similarity(refList1, refList2)
        if comparator == 'ScoringMatrixKris':
            similarity_comparison = DWM66_ScoringMatrixKris.normalized_similarity(refList1, refList2)
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

if __name__ == '__main__':
    SimilarityComparison()  
############################################################
#               END OF REDUCER      
############################################################
