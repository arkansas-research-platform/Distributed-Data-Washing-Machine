#!/usr/bin/python
# coding: utf-8

# Importing libraries
import sys 
import re
from operator import itemgetter
 ##############################################################
 #                     DEVELOPER's NOTES
 #  Re-Grouping to get pairs back
 ##############################################################
# This is the current word key
currentIdentifier  = None 
# Current word value (identifier)                        
currentFullRef= None   
identifier = None   

# Read the data from STDIN (the output from mapper)
for items in sys.stdin:
    line = items.strip()
    #print(line)
    lineSplit = line.split("-")
    identifier = lineSplit[0].strip()
    FullRef = lineSplit[1].strip()
    #line.split(":", maxsplit=1)

    if currentIdentifier == identifier:
        currentFullRef = currentFullRef + "<>" + FullRef        
    else:
        if currentIdentifier:
            print (currentFullRef)
        currentIdentifier = identifier
        currentFullRef= FullRef

# Output the last word
if currentIdentifier == identifier:
    print (currentFullRef)
############################################################
#               END OF MAPPER       
############################################################