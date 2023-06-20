#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys 
# sortedcontainers  source: https://grantjenks.com/docs/sortedcontainers/

 ##############################################################
 #                     DEVELOPER's NOTES
 #  Adding Component Records after TC is done. These are the
 #  minimum refID in each cluster. For example in the cluster 
 #  'A770538':'A882820', 'A770538':'A816319','A770538':'A882820',
 #  the cluster ID is A770538 which is also the smallest and so
 #  'A770538':'A770538' will be added to this cluster
 ##############################################################
current_refID  = None 
current_clusterID= None   
clusterID = None   
compKey = None
single = False

###### Input Prepping ######
for cluster in sys.stdin:
    kv = cluster.split(':')
    clusterID = kv[0].strip()
    refID = kv[1].strip()
    
    if current_clusterID == clusterID:
        compKey = clusterID
        print(cluster.strip().replace(' ',''))

    else:
        if current_clusterID:
            single = True
        current_clusterID = clusterID    
        compKey = clusterID
        print('%s:%s'%(current_clusterID, compKey))
        print(cluster.strip().replace(' ','')) 
############################################################
#               END OF PROGRAM      
############################################################