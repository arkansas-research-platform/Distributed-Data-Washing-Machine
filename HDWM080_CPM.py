#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import re
 #################################################################
 #                  DEVELOPER's NOTES
 #          --Cluster Profile Mapper 
 # Takes the LinkedIndex file as input and makes the clusterID 
 # column the key which will be sorted by the CPR 
 #################################################################
#--------------------------------------------------------------------
skipHeader = True
for line in sys.stdin:
    line = line.strip()
    if 'RefID' in line:
        skipHeader # Skip the first row (RefID * ClusterID)
    else:
        line_split = line.split("*")
        #print(line_split)
        refID = line_split[0].strip()
        clusterID = line_split[1].strip()
        print ('%s,%s' % (clusterID,refID))  
############################################################
#               END OF MAPPER       
############################################################