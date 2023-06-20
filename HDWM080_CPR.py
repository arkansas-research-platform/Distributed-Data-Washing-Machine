#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import os
 #########################################################
 #        Cluster Profile Reducer
 # Takes the LinkedIndex file as input and compute the 
 # profile of all the clusters that were formed   
 #########################################################
current_clusterID = None 
refID = None  
current_count = 0 
uniqueTokCnt = 0
# Read the data from STDIN 
for items in sys.stdin:
    line = items.strip()
    file = line.split(',')
    #print(file)
    clusterID = file[0]
    refID = file[1]    

    if current_clusterID == clusterID:
        current_count += 1
    else:
        if current_clusterID:
	  # Write result to STDOUT
            #print ('cluster --> ',current_clusterID, ' size --> ', current_count)
            print ('%s,%s'%(current_count, current_clusterID))
            #uniqueTokCnt +=1
        current_count = 1
        current_clusterID = clusterID
      
## Output the last word
if current_clusterID == clusterID:
    #print ('cluster --> ',current_clusterID, ' size --> ', current_count)
    print ('%s,%s'%(current_count, current_clusterID))
############################################################
#               END OF MAPPER       
############################################################