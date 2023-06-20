#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys 
import re
from operator import itemgetter
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
    if parmName == 'beta':
        #beta = convertToInteger(lineNbr, parmValue)
        beta = int(parmValue)
        continue
############################
# Read the data from STDIN (the output from mapper)
for items in sys.stdin:
    line = items.strip()
    #print(line)
#    refID, token = line.split("|")
    refID, mdata = line.split("|")
    #print(refID)
    #print(mdata)
#---------------------------------------------   
    # Getting the refID description and value from embedded metadata
    split_mdata = str(mdata).replace("{",'').replace("}",'')
    split_mdata1 = split_mdata.split(',')
    #print(split_mdata1)
#-------------------------------------------    
    # Getting the token
    token = split_mdata1[2].split(':')[1]
    #print(token)
    # Getting the frequency
    frequency = int(split_mdata1[3].split(':')[1])
    #print(frequency)
#--------------------------------------------
# Checking for blocking tokens 
        # If the frequency of the refID tokens is 1, skip it
    if frequency == 1:
        continue
        # If the frequency of the refID tokens is greater than beta, skip it
    if frequency > beta:
        continue
# ------------------------------------------
    #num_pattern = r'[0-9]'
    #tokensLeft = re.sub(num_pattern, '', token)
    #print(tokensLeft)
    #if token.isdigit:
    #    continue
# ------------------------------------------
#--------------------------------------------
    # Blocking Tokens for a refID saved in a list 
    if current_refID == refID:
        current_token = current_token + "," + token        
    else:
        if current_refID:
			# Write result to STDOUT
            curr_TokenSet = list(set([vals.strip() for vals in current_token.split(',')]))
            print ('%s : %s' % (current_refID,curr_TokenSet))
            #print ('%s : %s' % (current_refID,current_token))
            # Discard Numeric Tokens
            #tokensLeft = ''.join(filter(lambda x: not x.isdigit(), current_token))
            #print(tokensLeft) 
        current_refID = refID
        current_token= token

# Output the last word
if current_refID == refID:
    # Discard Numeric Tokens
    #tokensLeft = ''.join(filter(lambda x: not x.isdigit(), current_token))
    #print(tokensLeft)
    curr_TokenSet = list(set([vals.strip() for vals in current_token.split(',')]))
    print ('%s : %s' % (current_refID,curr_TokenSet))
    #print ('%s : %s' % (current_refID,current_token))
############################################################
#               END OF MAPPER       
############################################################