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
next(sys.stdin)  # Skip the first row (RefID * ClusterID)
for line in sys.stdin:
    line = line.strip()
    line_split = line.split("*")
    #print(line_split)

    refID = line_split[0].strip()
    clusterID = line_split[1].strip()
    print ('%s,%s' % (clusterID,refID))  
############################################################
#               END OF MAPPER       
############################################################