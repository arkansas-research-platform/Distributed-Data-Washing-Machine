#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys 
import re
 ##############################################################
 #                     DEVELOPER's NOTES
 #  -- LIST OF TOKENS THAT MEET BETA Threashold <REDUCER only>-- 
 #  Input comes from output of Pre-Block Mapper. 
 #  The value is a list of all the tokens that meets the Beta 
 #  Threshold for a particular refID. Format will look like 
 #  R1: [123,Oak,ST], where  123,Oak,ST are all the tokens
 #  in R1 that meets the Beta threshold. 
 ##############################################################

# This is the current word key
current_refID  = None 
# Current word value (refID)                        
current_token= None   
refID = None   

############################
# PARAMETER FOR DISCTRIBUTED CACHE
# Type: BLOCKING PARAMETER #
# Task: Setting BETA Threshold

def convertToBoolean(value):
    if value=='True':
        return True
    if value=='False':
        return False

####### READ PARAMETER FILE #######
#parameterFile = open('S8P-parms-copy.txt', 'r')  #Delete this line. Only used in Terminal
parameterFile = open('parmStage.txt') #Add back this line. Used by HDFS
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
############################
# Read the data from STDIN (the output from mapper)
for items in sys.stdin:
    line = items.strip()
    #print(line)
    line = line.split('|')
    refID = line[0].strip()
    mdata = line[1].strip()
    #refID, mdata = line.split("|")
    #print(refID)
#---------------------------------------------   
    # Getting the refID description and value from embedded metadata
    split_mdata = str(mdata).replace("{",'').replace("}",'').split(',')
    #print(split_mdata)
#-------------------------------------------   
    isBlkToken = True 
    # Getting the token
    token = (split_mdata[2]).split(':')[1].strip().replace('"','').replace("'","")
    #print(token, len(token))
    # Getting the frequency
    frequency = int(split_mdata[3].split(':')[1].strip().replace('"','').replace("'",""))
    #print(frequency)
#--------------------------------------------
# Checking for all Bloking Tokens
#--------------------------------------------
# Checking for blocking tokens and stopwords using frequency info
        # If the frequency of the refID tokens is 1, skip it
    if frequency < 2:
        isBlkToken = False
        # If the frequency of the refID tokens is greater than beta, skip it
    if frequency > beta:
        isBlkToken = False
        # Exclude or include numeric tokens
    if excludeNumericBlocks and token.isdigit():
        isBlkToken = False
    if len(token)<minBlkTokenLen:
        isBlkToken = False
#--------------------------------------------
    if isBlkToken:
        #print(token,frequency)
        # Blocking Tokens for a refID saved in a list 
        if current_refID == refID:
            current_token = current_token + "," + token        
        else:
            if current_refID:
			    # Write result to STDOUT
                curr_TokenSet = list([vals.strip() for vals in current_token.split(',')])
                print ('%s : %s' % (current_refID,curr_TokenSet))
            current_refID = refID
            current_token= token

# Output the last word
#if isBlkToken:
    #if current_refID == refID:
curr_TokenSet = list([vals.strip() for vals in current_token.split(',')])
print ('%s : %s' % (current_refID,curr_TokenSet))
############################################################
#               END OF MAPPER       
############################################################