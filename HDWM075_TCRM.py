#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import itertools
import sys
import os
 #########################################################
 #        TAGGING CLUSTERED REFERENCES Mapper
 # Tag refs that has been processed already in previous 
 # iterations. If not tagged, they will be processed again    
 #########################################################
# Loading the Log_File from the bash driver
#logfile = open(os.environ["Log_File"],'a')
# Read the data from STDIN (the output from mapper)
for items in sys.stdin:
    line = items.strip().replace('<>','-')
    #print(line)
    file = line.split('-') #max split is set to 1 to split on only the first comma
    # First split the line with Good Cluster
    if 'GoodCluster' in line:
    #print(file)
        refID = file[0]
        CID = file[1]
        oldTag = file[2]
        #print(oldTag)
        out1 = '%s|%s'%(refID,line)
        out2 = '%s|%s%s'%(CID,line,'*copy*')
        print(out1)
        print(out2)
    else:
        refID = file[0]
        mdata = file[1]
        oldTag = file[2]
        out3 = '%s|%s'%(refID,line)
        #out4 = '%s|%s%s'%(refID,mdata,'*copy*')
        print(out3)
        #print(out4)
############################################################
#               END OF MAPPER       
############################################################