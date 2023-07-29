#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys 
import re
import os
from operator import itemgetter
from pathlib import Path

# Making DWM modules available for MapReduce
sys.path.append('textdistance.zip')
sys.path.append('DWM-Modules.zip')
import DWM10_Parms 
import StopWord
import DWM65_ScoringMatrixStd
import DWM66_ScoringMatrixKris
import textdistance

#Note: The line below is important in order for Hadoop to recognize the 'textdistance' library
#       Ignoring this line will cause the job to fail ( 4 days researching to discover this )
#'/home/nick/.local/lib/python3.10/site-packages'
#textdistanceDir = '/usr/local/lib/python*/dist-packages'
#sys.path.append(textdistanceDir)

from textdistance import DamerauLevenshtein
from textdistance import Cosine
from textdistance import MongeElkan

 ##############################################################
 #                     DEVELOPER's NOTES
 #  Similarity computation using Matrix Comparator after 
 #  removing stopwords, Linking Equivalent pair of references 
 #  by comparing similarity score with Mu Threshold. 
 #  Output is a list(rows) of linked pairs
 ##############################################################

# Loading muReport file from Distributed Cache
#with open('muReport.txt', 'r') as openMuFile:
#    muVal = str(openMuFile.readline()).strip()
#mu = float(muVal)

 # Get Parameter values
# Read Parms file
#DWM10_Parms.getParms('parmStage.txt')
mu = DWM10_Parms.mu
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
###### Input Prepping ######
def similarPairs(refPair):
    isLinkedIndex = False
    isUsedRef = False
    links = 0

    splitPairList = refPair.split('<>')
    # Right-side (Ref1) Prep
    reference1 = splitPairList[0]#.split(",", maxsplit=1)
    ref1Split = reference1.split('-')
    refID1 = ref1Split[0]
    ref1Metadata = ref1Split[1].strip()
    full_ref1 = ref1Metadata.replace("{", "").replace("}", "").split(",")  #ref with position info
    #List of token & Freq before stopword removal, to get the position info,use tokFreq.split(":")[0]
    Ref1TokenFreq = [tokFreq.split(":")[1].strip().replace("'" , "") for tokFreq in full_ref1]
 
    # Left-side (Ref2) Prep
    reference2 = splitPairList[1]#.split(",", maxsplit=1)
    ref2Split = reference2.split('-')
    refID2 = ref2Split[0]#.strip().replace("'","")
    ref2Metadata = ref2Split[1].strip()
    full_ref2 = ref2Metadata.replace("{", "").replace("}", "").split(",") #ref with position info
    #List of token & Freq before stopword removal, to get the position info,use tokFreq.split(":")[0]
    Ref2TokenFreq = [tokFreq.split(":")[1].strip().replace("'" , "") for tokFreq in full_ref2] 

    # Debugging Lines
    #return(reference1,'**',reference2)
    #return(refID1,'**',refID2)
    #return(ref1Metadata,'**',ref2Metadata)
    #return(full_ref1,'**',full_ref2)
    #return(Ref1TokenFreq, '**', Ref2TokenFreq)
    #return('Count of Tokens before Stopwords Removal: ',len(Ref1TokenFreq), '**', len(Ref2TokenFreq)) 

    ###### Remove all Stopword using frequency information ###### 
    refList1 = StopWord.removestopwords(Ref1TokenFreq)
    refList2 = StopWord.removestopwords(Ref2TokenFreq)
    #return(refID1, '-', refList1, '**', refID2, '-', refList2)
    #return('Count of Tokens after Stopwords Removal: ', refID1, '-', len(refList1), '**', refID2, '-', len(refList2))

    ###### Get Similarity comparison Score using Scoring Matrix ###### 
    if comparator == 'MongeElkan':
        similarity_comparison = MongeElkan.normalized_similarity(refList1, refList2)
    if comparator == 'Cosine':
        similarity_comparison = Cosine.normalized_similarity(refList1, refList2)
    if comparator == 'ScoringMatrixStd':
        similarity_comparison = DWM65_ScoringMatrixStd.normalized_similarity(refList1, refList2)
    if comparator == 'ScoringMatrixKris':
        similarity_comparison = DWM66_ScoringMatrixKris.normalized_similarity(refList1, refList2)
    #return('Similarity Between:', refID1, '**', refID2, 'is', similarity_comparison)  

    ###### Compare Sim_Score with Mu to determnine Match/No match ###### 
    if similarity_comparison >= mu:  #Link or no-Link decision
        #return('Linked Pair:', refID1, '**', refID2, 'has a similarity of:', similarity_comparison)
        # Outpout the original linked pairs and their inverse, which will be the input 
        # for the Transitive Closure algorithm in the next reducer
        linkedPairs = '%s,%s,%s' % (refID1,refID2,refID2) # Original Linked Pairs 
        #inversedLinkedPairs = '%s.%s,%s' % (refID2, refID1,refID1) # Inverted Linked Pairs
        #pairSelf = '%s.%s,%s' % (refID1,refID1,refID1) # PairSelf
        return(linkedPairs)   
        #return(inversedLinkedPairs) 
        #return(pairSelf) 
    ############################################################
    #               END OF REDUCER      
    ############################################################
