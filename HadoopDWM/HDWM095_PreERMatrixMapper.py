#!/usr/bin/env python
# coding: utf-8

import sys
import time
import datetime
import re
 #########################################################
 #                  DEVELOPER's NOTES
 # ER Merge Mapper - Merges the truthSet file with the 
 # final output from the Transitive Closure. Meregs on
 # refID from both files
 #########################################################
def PreMatrixMap():
    # First row of the truthset is the column header. Need to be removed
    next(sys.stdin) 
    lineToKeep = True
    for rec in sys.stdin:
        refID = -1    #default sorted as first
        truthID = -1  #default sorted as first
        clusterID = -1   #default sorted as first

        line = rec.strip().upper().replace("'","")
        if 'CLUSTERID' in line:
            lineToKeep = False
            continue
        else: 
            lineToKeep
            #print(line)
            splitLine = line.split(',')
            #print(splitLine)

            # First get refID and clusterID from the linked index lines
            if len(splitLine) == 1:
                lnkIndexSplit = splitLine[0].split('*')
                refID = lnkIndexSplit[0].strip()
                clusterID = lnkIndexSplit[1].strip()
                #print('--Debugging','RefID ',refID,'CID ',clusterID)
            # Next, get refID and truthID from truthfile
            else:
                refID = splitLine[0].strip()
                truthID = splitLine[1].strip()
            print ('%s,%s,%s' % (refID, clusterID, truthID))

if __name__ == '__main__':
    PreMatrixMap()   
############################################################
#               END OF MAPPER       
############################################################