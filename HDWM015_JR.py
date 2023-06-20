#!/usr/bin/env python
import sys
import string

 #########################################################
 #                  DEVELOPER's NOTES
 #   --JOIN Reducer (Update metadata with Frequency)--               
 # Takes the output from the Join Mapper and updates the 
 # metadata information by adding frequency info to rows.
 # A one-to-many join operation happens to add frequency 
 # information 
 #########################################################

# maps words to their counts
currentFreq = "-1"
currentKey = "-1"
isFrequencyMappingLine = False

for line in sys.stdin:
    line = line.strip()
    key,mdata,freq = line.split("|")
    #print(freq)

    # First line should be a mapping line that contains 
    # the frequency information for that key group
    if mdata == "-1":  #That is a line with frequency info that will be used to map
        currentFreq = freq
        currentKey = key
        isFrequencyMappingLine = True
    else:
        isFrequencyMappingLine = False
    #print ('%s|%s|%s' % (key,mdata,currentFreq))

    if mdata == "-1":  #Remove all previous frequency info line because they have been used to map
        continue
    #print ('%s | %s | %s' % (key,mdata,currentFreq))

    update_mdata = mdata.replace("}", ", 'freq':") + currentFreq + "}"
    print ('%s|%s' % (key,update_mdata))
############################################################
#               END OF REDUCER       
############################################################