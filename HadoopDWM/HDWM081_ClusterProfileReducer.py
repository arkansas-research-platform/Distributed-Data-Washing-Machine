#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys
import os
from operator import itemgetter
 #########################################################
 #        Cluster Profile Reducer
 # Takes the LinkedIndex file as input and compute the 
 # profile of all the clusters that were formed   
 #########################################################
def ClusterProfile():
    # Header for the final output
    print('Cluster Profile')
    print('ClusOfSize ', 'Count ', 'RefCnt')
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
            current_count = 1
            current_cSize = cSize

    ## Output the last word
    if current_cSize == cSize:
        refCnt = int(current_cSize)*int(current_count)
        totalRefs += refCnt
        totalClusters += current_count
        print ('      %s      %s        %s' % (current_cSize, current_count,refCnt))

    # Calculate average record per cluster
    avgRefPerCluster = totalRefs/totalClusters

    print('Total:      ', totalClusters, '      ', totalRefs)  
    print('\nAverage Record per Cluster: ', round(avgRefPerCluster, 4)) 

if __name__ == '__main__':
    ClusterProfile()   
############################################################
#               END OF MAPPER       
############################################################