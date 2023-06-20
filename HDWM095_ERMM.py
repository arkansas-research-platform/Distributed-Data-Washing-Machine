#!/usr/bin/env python
# coding: utf-8

import sys
import time
import datetime

 #########################################################
 #                  DEVELOPER's NOTES
 # ER Merge Mapper - Merges the truthSet file with the 
 # final output from the Transitive Closure. Meregs on
 # refID from both files
 #########################################################
# First row of the truthset is the column header. Need to be removed
next(sys.stdin) 
lineToKeep = True
for rec in sys.stdin:
    refID = -1    #default sorted as first
    truthID = -1  #default sorted as first
    clusterID = -1   #default sorted as first

    line = rec.strip().upper().replace('-','')
    if 'ID' in line:
        lineToKeep = False
        continue
    else: 
        lineToKeep
        #print(line)
        recPrep = line.strip().split(',')
        if len(recPrep) == 3:              #LinkedIndex file
            refID = recPrep[0].strip()
            clusterID = recPrep[1].strip()
        else:
            refID = recPrep[0]
            truthID = recPrep[1]

        print ('%s,%s,%s' % (refID, clusterID, truthID))