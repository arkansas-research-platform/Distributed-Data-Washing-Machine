#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import sys 
from operator import itemgetter
import os
 ##############################################################
 #                     DEVELOPER's NOTES
 #     --BLOCK TOKEN PAIR Reducer-- 
 #  Input comes from output of Block Token Pair Mapper. 
 #  Takes the output from the pairs created in the BTPM step and 
 #  reduces based on the pair keys (Blocking Key Pairs) and 
 #  collects all the refIDs that fall under that pair key.
 ##############################################################
def BlockTokenPairReduce():
    # Current pairs of blocking keys                        
    current_block_keyPairs = None 
    # Current refID                      
    current_refID = None   
    block_keyPairs = None   
    blkPairCnt = 0
    isLinkedIndex = False
    isUsedRef = False

    # Read the data from STDIN (the output from mapper)
    for items in sys.stdin:
        line = items.strip()
        #print(line)
        # Check if the ref has already been used
        if '*used*' in line:
            isUsedRef = True
            print(line.replace(':',',')) 
            continue
        # Checking for only good cluster (skip those)
        if 'GoodCluster' in line:
            isLinkedIndex = True
            print(line.replace(':',','))
            continue
        line = line.split(":")
        block_keyPairs = line[0].strip()
        refID = line[1].strip()
        #block_keyPairs, refID = line.split(":")
        #print(current_refID)
        #print(block_keyPairs)
    #-----------------------------------------------------------
        if current_block_keyPairs == block_keyPairs:
            current_refID = current_refID + "|" + refID
        else:
            if current_block_keyPairs:
    			# Write result to STDOUT
    ##            print ('%s : %s' % (current_block_keyPairs, current_refID))  
                refID_split = current_refID.split('|')
                #print(refID_split)
                        # Creating pairs
                for x in range(0, len(refID_split)-1):
                    for y in range(x+1, len(refID_split)):
                        Xtoken = refID_split[x]
                        Ytoken = refID_split[y]
                        if Xtoken < Ytoken:
                            pair = (Xtoken + ',' + Ytoken)
                            print('%s : %s' % (pair, 'one'))
                            # Reporting to MapReduce Counter
                            sys.stderr.write("reporter:counter:Blocking Counters,Pairs Created-by-Blocks,1\n")
                            blkPairCnt +=1
                        else:
                            pair = (Ytoken + ',' + Xtoken )
                            print('%s : %s' % (pair, 'one'))
                            # Reporting to MapReduce Counter
                            sys.stderr.write("reporter:counter:Blocking Counters,Pairs Created-by-Blocks,1\n")
                            blkPairCnt +=1
            else:
                sys.stderr.write("reporter:counter:Blocking Counters,Pairs Created-by-Blocks,0\n")
            current_refID = refID
            current_block_keyPairs = block_keyPairs  

    # Process last group
    if current_block_keyPairs == block_keyPairs:
        refID_split = current_refID.split('|')
        # Creating pairs
        for x in range(0, len(refID_split)-1):
            for y in range(x+1, len(refID_split)):
                Xtoken = refID_split[x]
                Ytoken = refID_split[y]
                if Xtoken < Ytoken:
                    pair = (Xtoken + ',' + Ytoken)
                    print('%s : %s' % (pair, 'one'))
                    # Reporting to MapReduce Counter
                    sys.stderr.write("reporter:counter:Blocking Counters,Pairs Created-by-Blocks,1\n")
                    blkPairCnt +=1
                else:
                    pair = (Ytoken + ',' + Xtoken )
                    print('%s : %s' % (pair, 'one'))
                    # Reporting to MapReduce Counter
                    sys.stderr.write("reporter:counter:Blocking Counters,Pairs Created-by-Blocks,1\n")
                    blkPairCnt +=1

if __name__ == '__main__':
    BlockTokenPairReduce()      
############################################################
#               END OF REDUCER       
############################################################