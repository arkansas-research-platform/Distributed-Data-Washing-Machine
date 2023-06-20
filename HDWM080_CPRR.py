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
# Loading the Log_File from the bash driver
logfile = open(os.environ["Log_File"],'a')

# Header for the final output
print('\n>> Starting Cluster Profile Process', file=logfile)
print('Cluster Profile')
print('\nCluster Profile', file=logfile)
print('ClusOfSize ', 'Count ', 'RefCnt')
print('ClusOfSize ', 'Count ', 'RefCnt', file=logfile)
current_cSize = None 
current_count = 0 
clusterCnt = 0
totalRefs = 0
totalClusters = 0
# Read the data from STDIN 
for items in sys.stdin:
    line = items.strip()
    file = line.split(',')
    #print(file)
    cSize = file[0]

    if current_cSize == cSize:
        current_count += 1
    else:
        if current_cSize:
            refCnt = int(current_cSize)*int(current_count)
            totalRefs += refCnt
            totalClusters += current_count
            print ('      %s      %s        %s' % (current_cSize, current_count,refCnt))
            print ('      %s      %s        %s' % (current_cSize, current_count,refCnt), file=logfile)
        current_count = 1
        current_cSize = cSize
        
## Output the last word
if current_cSize == cSize:
    refCnt = int(current_cSize)*int(current_count)
    totalRefs += refCnt
    totalClusters += current_count
    print ('      %s      %s        %s' % (current_cSize, current_count,refCnt))
    print ('      %s      %s        %s' % (current_cSize, current_count,refCnt), file=logfile)

print('           ', '----', '    ----')
print('           ', '----', '    ----',file=logfile)
print('Total:      ', totalClusters, '      ', totalRefs)  
print('Total:      ', totalClusters, '      ', totalRefs, file=logfile)
print('           ', '----', '    ----')
print('           ', '----', '    ----', file=logfile)
############################################################
#               END OF MAPPER       
############################################################