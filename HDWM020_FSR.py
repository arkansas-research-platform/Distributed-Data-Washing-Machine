#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys 
import re
from operator import itemgetter
 ##############################################################
 #                     DEVELOPER's NOTES
 #     --FULL STRING OF REFERENCE (FSR) <REDUCER only>-- 
 #  Input comes from output of Pre-Block Mapper. 
 #  The value is a full string of reference with refID as key, 
 #  and a tuple of positional index, token, and freq  as value
 #  in the form (AA,BB,CC)
 ##############################################################
# This is the current word key
current_refID  = None 
# Current word value (refID)                        
current_token_mdata = None   
refID = None   

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
    # Getting the positional index
    position = split_mdata1[1].split(':')[1]
    position = position.replace("'", '').strip()
    #print(position)
    # Getting the token
    token = ((split_mdata1[2].split(':')[1]).strip()).replace("'", "")
    #print(token)
    # Getting the frequency
    frequency = (split_mdata1[3].split(':')[1]).strip()
    #print(frequency)
#--------------------------------------------
# Reform metadata in the form (AA,BB,CC), where
    # AA - token, BB - frequency, CC - positional index of token
    #token_mdata = position+ "," + "(" + token + "," + frequency +")"
    #token_mdata = '%s^%s'%(token,frequency)
    token_mdata = '%s-%s^%s'%(position,token,frequency)
    #print(token_mdata)
#--------------------------------------------   
    if current_refID == refID:
          current_token_mdata = current_token_mdata + "|" + token_mdata   
    else:
        if current_refID:
	# Write result to STDOUT
            #finalDict = dict((int(k), v) for k, v in (e.split('-') for e in current_token_mdata.split('|')))
            finalDict = {int(k):v for k, v in (e.split('-') for e in current_token_mdata.split('|'))}
            sort_Dict = dict(sorted(finalDict.items(), key=lambda item: item[0]))
            #sort_Dict = sorted(finalDict.items())
            print('%s - %s - %s' % (current_refID, sort_Dict, "one"))
        current_refID = refID
        current_token_mdata = token_mdata
# Output the last record
if current_refID == refID:
    #finalDict = dict((int(k), v) for k, v in (e.split('-') for e in current_token_mdata.split('|')))
    finalDict = {int(k):v for k, v in (e.split('-') for e in current_token_mdata.split('|'))}
    sort_Dict = dict(sorted(finalDict.items(), key=lambda item: item[0]))
    #sort_Dict = sorted(finalDict.items())
    print('%s - %s - %s' % (current_refID, sort_Dict, "one"))
############################################################
#               END OF MAPPER       
############################################################