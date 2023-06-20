#!/usr/bin/python
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
    line = items.strip().replace('-1 <> ','').replace('<>','-')
    if 'UnprocessedRef' in line:
        continue
    #print(line)
    #file = line.split('-') #max split is set to 1 to split on only the first comma
    #print(file)
    # First split the line with Good Cluster
    if 'GoodCluster' in line:
        file = line.strip().replace(',','-').split('-') #max split is set to 1 to split on only the first comma
        #print(file)
        refID = file[0]
        CID = file[1]
        oldTag = file[2]
        #print(oldTag)
        out1 = ('%s|%s'%(refID,line)).replace(',','-')
        out2 = ('%s|%s%s'%(CID,line,'*copy*')).replace(',','-')
        print(out1)
        print(out2)
    else:
        file = line.split('-')
        refID = file[0].strip().replace("'","")
        mdata = file[1].strip()
        oldTag = file[2].strip()
        out3 = '%s|%s'%(refID,line)
        print(out3)
############################################################
#               END OF MAPPER       
############################################################