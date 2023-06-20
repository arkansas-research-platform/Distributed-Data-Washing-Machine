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

####### READ PARAMETER FILE #######
#parameterFile = open('S8P-parms-copy.txt', 'r')  #Delete this line. Only used in Terminal
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
    if parmName=='blockByPairs':
        blockByPairs = convertToBoolean(parmValue)

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
#---------------------------------------------
#### Deciding Blocking Tokens ####
    # If there are no tokens in a list, nothing to do
    #if len(tok_list) == 1:
    #    continue
    ### Blocking-by-Pairs ###
    # Exclude single tokens freq of refID tokens is 1, skip it and "Block-by-Pairs" 
    if blockByPairs:
        if len(tok_list) < 2:
            continue
        #print(tok_list)
        # Create a nested for loop to build pairs of refIDs to be compared
        for x in range(0, len(tok_list)-1):
            Xtoken = tok_list[x].strip()
            #print(Xtoken)
            for y in range(x+1, len(tok_list)):
                Ytoken = tok_list[y].strip()
                #print(Ytoken)
                # Keeping Pairs in Ascending Order
                if Xtoken < Ytoken:
                    #pair = (Xtoken + "," + Ytoken)
                    pair = (Xtoken+Ytoken)
                    print ('%s : %s' % (pair, refID))
                else:
                    #pair = (Ytoken + "," + Xtoken)
                    pair = (Ytoken+Xtoken)
                    print ('%s : %s' % (pair, refID))
#---------------------------------------------
    # If BlockByPairs was set to False, that means blockBySingles
    else:
        for x in range(0, len(tok_list)):
            Xtoken = tok_list[x]
            print ('%s : %s' % (Xtoken, refID))
############################################################
#               END OF MAPPER       
############################################################