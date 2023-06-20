#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import re
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

parameterFile = open('HDWM/parmStage.txt', 'r')
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
    if parmName == 'excludeNumericBlocks':
        excludeNumericTokens = convertToBoolean(parmValue)
        continue

#import HDWM006_Parms
#excludeNumericTokens = HDWM006_Parms.excludeNumericTokens
############################
for record in sys.stdin:
    file = record.strip()
    file = file.replace('"','').replace("[", "").replace("]", "")
    #print(file)
    file_split = file.split(":")
    f_split = file_split[1].replace("'", '')
    #print(f_split)

    # Get refID
    refID = file_split[0]
    #print(refID)
    # Split blocking tokens to form a list
    tok_list = f_split.split(',')
    #print(tok_list)
# ------------------------------
#   REMOVE NUMERIC TOKES FROM LIST
    if excludeNumericTokens:
        remain_TokList = [x for x in tok_list if not (x.isdigit() 
                                         or x[0] == '-' and x[1:].isdigit())]
    #print(remain_TokList)
    else: #excludeNumericTokens is False or includeNumTokens
        remain_TokList = tok_list
#---------------------------------------------
# Exclude single tokens, "Block-by-Pairs" only
        # If the frequency of the refID tokens is 1, skip it
    if len(remain_TokList) == 1:
        continue
    #print(tok_split)
#---------------------------------------------
# Creating pairs of tokens from the remaining Blocking Tokens
        # Create a nested for loop to build pairs of refIDs to be compared
    for x in range(0, len(remain_TokList)-1):
        for y in range(x+1, len(remain_TokList)):
            Xtoken = remain_TokList[x]
            Ytoken = remain_TokList[y]
            #print(Xtoken)
            #print(Ytoken)
            # Make sure the smallest value is printed first
            if Xtoken < Ytoken:
                pair = (Xtoken + "," + Ytoken)
                print ('%s : %s' % (pair, refID))
                #pair_dict[pair] = refID
            else:
                pair2 = (Ytoken + "," + Xtoken)
                print ('%s : %s' % (pair2, refID))
                #pair_dict[pair2] = refID 
############################################################
#               END OF MAPPER       
############################################################