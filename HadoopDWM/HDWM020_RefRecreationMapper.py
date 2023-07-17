#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys
import re

 #################################################################
 #                  DEVELOPER's NOTES
 #          --Pre-Blocking Mapper (Make refID as Key) 
 #  This step is used to sorted the previous output by refID
 #  This is a mapper is used to get the refID as a key and is used 
 #  to produce two outputs in <HDWM020_FSR> and <HDWM020_TLR>
 #################################################################
#--------------------------------------------------------------------
for record in sys.stdin:
    record = record.strip()
    record_split = record.split("|")
    #print(record_split[1])

    # Getting the refID description and value from embedded metadata
    get_refID = str(record_split[1]).replace("{",'').replace("}",'')
    get_refID1 = get_refID.split(',')[0]
    #print(get_refID1)
#--------------------------------------------
# Extracting reference ID value only
    refID = (get_refID.split(',')[0]).split(":")[1]
    m_data = str(record_split[1])
    print ('%s | %s' % (refID, m_data))  
#---------------------------------------------
############################################################
#               END OF MAPPER       
############################################################