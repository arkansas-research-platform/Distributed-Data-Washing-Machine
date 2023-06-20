#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import re
import os
 #########################################################
 #                     DEVELOPER's NOTES
 #                 BLOCKING TOKENS PAIRS Mapper 
 #  Takes the tokens that satisfied the Beta threshold and 
 #  that formed list from the TLR step and looped through
 #  each list to create pairs with the pairs as key and the 
 #  refID as value. This process considers whether 
 #  numeric tokens should be included or excluded
 #########################################################
#--------------------------------------------------------------------
############################
# PARAMETER FOR DISCTRIBUTED CACHE
# Blocking parameter

def convertToBoolean(value):
    if value=='True':
        return True
    if value=='False':
        return False

####### READ PARAMETER FILE #######
#parameterFile = open('S1G-parms-copy.txt', 'r')  #Delete this line. Only used in Terminal
parameterFile = open('parmStage.txt', 'r') #Add back this line. Used by HDFS
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
    if parmName == 'beta':
        beta = int(parmValue)
        continue
    if parmName=='excludeNumericBlocks':
        excludeNumericBlocks = convertToBoolean(parmValue)
        continue 
    if parmName=='minBlkTokenLen':
        minBlkTokenLen = int(parmValue)
        continue 
    if parmName=='blockByPairs':
        blockByPairs = convertToBoolean(parmValue)

# Loading the Log_File from the bash driver
logfile = open(os.environ["Log_File"],'a')
############################
####### MAIN PROGRAM #######
############################
isLinkedIndex  = False 
isUsedRef = False
selectedRefCnt = 0  
blkRefsCnt = 0
numericCnt = 0
remainRefs = 0
excludedRefCnt = 0


for record in sys.stdin:
    file = record.strip()
    #print(file)
    file_split = file.split('-')
    #print(f_split)
    # Check if the ref has already been used
    if '*used*' in file:
        isUsedRef = True
        print(file.replace('-',':')) 
        continue
    # Decide which references to reprocess (NB: LinkedIndex are skipped - this is for program iteration)
    if 'GoodCluster' in file:
        isLinkedIndex = True
        print(file.replace('-',':')) 
        continue
    selectedRefCnt +=1
    # Get refID
    refID = file_split[0].strip()
    metaData = file_split[1].strip()
    #print(metaData)

    # Get Tokens and Frequency from the metadata
    metaData = metaData.replace("{",'').replace("}",'').split(',')
    
    blkTokenList = []
    tokenFreq = []
    for item in metaData:
        item = item.split(':')
        tokenFreq.append(item[1].strip().replace("'",""))
    #print(tokenFreq)
    for clean in tokenFreq:
        isBlkToken = True 
        clean = clean.split('^')
        token = clean[0].strip().replace('"','').replace("'","")
        frequency = int(clean[1])
        #print(token, frequency)

# ---- PHASE 1: TLR - Deciding on Blocking Tokens in each Ref ------
        # Count numeric tokens
        if token.isdigit():
            numericCnt += 1
        # If the frequency of the refID tokens is 1, skip it
        if frequency < 2:
            #print('-- Freq < 2 Rule: ', refID,token,frequency)
            isBlkToken = False
        # If the frequency of the refID tokens is greater than beta, skip it
        if frequency > beta:
            #print('-- Freq > Beta Rule: ', refID,token,frequency)
            isBlkToken = False
        # Exclude or include numeric tokens
        if excludeNumericBlocks and token.isdigit():
            #print('-- Num Token Rule: ', refID,token)
            isBlkToken = False
        if len(token)<minBlkTokenLen:
            #print('-- Min Blocking Token Length Rule: ', refID,token, 'len', len(token))
            isBlkToken = False
        # Remainder Blocking Tokens
        if isBlkToken:
            blkTokenList.append(token)
    #print('-- Blocking Tokens for this Ref: ',refID, blkTokenList)

# ---- PHASE 2: BTPM - Forming Blocking Keys using the tokens in BlknTokenList ------
    # If there are no tokens in a list, nothing to do, so delete such list
    if len(blkTokenList) < 1:
        excludedRefCnt +=1
        continue
    remainRefs += 1
    #print(refID, blkTokenList)
##---------------------------------------------
    ### Blocking-by-Pairs ###
    # Exclude single tokens in a list if "Block-by-Pairs" is True 
    if blockByPairs:
        if len(blkTokenList) < 2:
            continue
        #print(blkTokenList)
        # Create a nested for loop to build pairs of refIDs to be compared
        for x in range(0, len(blkTokenList)-1):
            Xtoken = blkTokenList[x].strip()
            #print(Xtoken)
            for y in range(x+1, len(blkTokenList)):
                Ytoken = blkTokenList[y].strip()
                #print(Ytoken)
                # Keeping Pairs in Ascending Order
                if Xtoken < Ytoken:
                    #pair = (Xtoken + "," + Ytoken)
                    pair = (Xtoken+Ytoken)
                    print ('%s:%s' % (pair, refID))
                    blkRefsCnt += 1
                else:
                    #pair = (Ytoken + "," + Xtoken)
                    pair = (Ytoken+Xtoken)
                    print ('%s:%s' % (pair, refID))
                    blkRefsCnt += 1
##---------------------------------------------
    # If BlockByPairs was set to False, that means blockBySingles
    else:
        for x in range(0, len(blkTokenList)):
            Xtoken = blkTokenList[x]
            print ('%s:%s' % (Xtoken, refID))
            blkRefsCnt += 1

# Reporting to logfile
print('\n>> Starting Blocking Process', file=logfile)
print('   Total Numeric Tokens Found: ', numericCnt, file=logfile)
print('   Total References Selected for Reprocessing: ', selectedRefCnt, file=logfile)
print('   Total Record Excluded: ', excludedRefCnt, file=logfile)
print('   Total Record Left for Blocks Creation: ', remainRefs, file=logfile)
print('   Total Blocking Records Created: ', blkRefsCnt, file=logfile)

# Debugging Lines
#print('   Total References Selected for Reprocessing: ', selectedRefCnt)
#print('   Total Record Excluded: ', excludedRefCnt)
#print('   Total Record Left for Blocks Creation: ', remainRefs)
#print('   Total Blocking Records Created: ', blkRefsCnt)
############################################################
#               END OF MAPPER       
############################################################