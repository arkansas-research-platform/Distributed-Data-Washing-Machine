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
for rec in sys.stdin:
    refID = -1    #default sorted as first
    truthID = -1  #default sorted as first
    linkID = -1   #default sorted as first
    
    recPrep = rec.strip().replace("'", "").replace(':', ',x,')
    recSplit = recPrep.split(',')
    #print(recSplit)

    if len(recSplit) == 3:
        refID = recSplit[2].strip()
        linkID = recSplit[0].strip()
    else:
        refID = recSplit[0]
        truthID = recSplit[1]#+'@1'

    print ('%s,%s,%s' % (refID, linkID, truthID))
