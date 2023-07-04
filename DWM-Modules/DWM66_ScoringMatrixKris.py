#!/usr/bin/env python
# coding: utf-8

# In[1]:


from textdistance import DamerauLevenshtein
#import Levenshtein as lev
#from fastDamerauLevenshtein import damerauLevenshtein 
import re
import DWM10_Parms
# Read Parms file
DWM10_Parms.getParms('parms')

matrixNumTokenRule = DWM10_Parms.matrixNumTokenRule
matrixInitialRule = DWM10_Parms.matrixInitialRule

# Loading muReport file from Distributed Cache
with open('muReport.txt', 'r') as openMuFile:
    muVal = str(openMuFile.readline()).strip()
mu = float(muVal)

##### ScoringMatrixKris Function #####
def normalized_similarity(refList1, refList2):
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