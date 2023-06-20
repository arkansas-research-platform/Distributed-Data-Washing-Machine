#!/bin/bash

# Copy input data from local directory to HDFS
# hadoop fs -put <location to source in local dir> <HDFS destination location>
# Upload Parameter File to HDFS Distributed Cache before job start
#hadoop fs -put HDWM/HDWM005_Parameters HDWM

#### HDWM Run Script ####
# This is the main run scrip for for the HDWM program under developmet
#################################################
#### Required Settings ####
# Get data file name to run
#read -p "Enter input data name: " inputFile
# Get parameter file
read -p "Enter parameter file: " parmFile
#-files hdfs://snodemain:9000/user/nick/HDWM/$parameter_file 

# Get input file name from parameter file
#inputFile=$(awk -F "=" '/inputFileName/{print $NF}' HDWM/$parmFile)
#echo $file
# Get truthset file name from parameter file
#truthFile=$(awk -F "=" '/truthFileName/{print $NF}' HDWM/$parmFile)


while IFS='=' read -r line val
do
   if [[ "$line" = inputFileName* ]]
   then
      inputFile="$val"
      echo "Input File to process:--->$inputFile"
   elif [[ "$line" = truthFileName* ]]
   then
      truthFile="$val"
      echo "TruthSet File is:--->$truthFile"
   else
      continue
   fi
done < "HDWM/$parmFile"

# Copy input data and truthSet from local directory to HDFS
hadoop fs -put HDWM/$inputFile HDWM
hadoop fs -put HDWM/$truthFile HDWM

# Copy contents of the given parameter file to a staging area to be shipped to Distributed Cache
cp HDWM/$parmFile HDWM/parmStage.txt

# Create some variables to be reused. These are just paths to important repetitive JAR Libraries
Identity_Mapper=/bin/cat
Identity_Reducer=org.apache.hadoop.mapred.lib.IdentityReducer
STREAMJAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar

# Create HDFS directory
#hadoop fs -mkdir <Directory Name>

# REMOVE DIRECTORIES FROM HDFS IF ALREADY EXISTS
hadoop fs -rm -r HDWM/HDWM
hadoop fs -rm -r HDWM/job1_out 
hadoop fs -rm -r HDWM/job2_out 
hadoop fs -rm -r HDWM/job3_out 
hadoop fs -rm -r HDWM/job4_out 
hadoop fs -rm -r HDWM/job5_out 
hadoop fs -rm -r HDWM/job6_out 
hadoop fs -rm -r HDWM/job7_out 
hadoop fs -rm -r HDWM/job7_tmp_out 
hadoop fs -rm -r HDWM/job8_out 
hadoop fs -rm -r HDWM/job9_out 
hadoop fs -rm -r HDWM/job10_temp_in
hadoop fs -rm -r HDWM/job10_temp_out
hadoop fs -rm -r HDWM/job11_out
hadoop fs -rm -r HDWM/job12_out
hadoop fs -rm -r HDWM/job13_out
hadoop fs -rm -r HDWM/job14_out
#################################################
# START THE EXECUTION OF HDWM JOBS
#################################################
# JOB 1: Tokenization
#        one Mapper with Identity Reducer. outputs tokens with metadata (there are duplicates) without frequency
hadoop jar $STREAMJAR \
    -files HDWM/HDWM010_TM.py \
    -D mapreduce.job.reduces=1 \
    -input HDWM/$inputFile \
    -output HDWM/job1_out \
    -mapper HDWM010_TM.py \
    -reducer $Identity_Reducer

# JOB 2: Tokenization Frequency 
#        one Mapper & one Reducer....Outputs keys and their frequencies
hadoop jar $STREAMJAR \
    -files HDWM/HDWM010_TM.py,HDWM/HDWM010_TR.py \
    -D mapreduce.job.reduces=1 \
    -input HDWM/$inputFile \
    -output HDWM/job2_out \
    -mapper HDWM010_TM.py \
    -reducer HDWM010_TR.py

# JOB 3: Joining outputs of Job 1 & Job 2 
#        one Mapper & one Reducer....Outputs keys and their frequency
hadoop jar $STREAMJAR \
    -files HDWM/HDWM015_JM.py,HDWM/HDWM015_JR.py \
    -D mapreduce.job.reduces=1 \
    -Dstream.num.map.output.key.fields=2 \
    -input HDWM/job1_out \
    -input HDWM/job2_out \
    -output HDWM/job3_out \
    -mapper HDWM015_JM.py \
    -reducer HDWM015_JR.py

# In Jobs 4&5, the goal is to produce two types of outputs:
#     output1 (Job 4) - Full references with all tokens, frequencies, and position
#     output2 (Job 5) - List of all tokens that satisfy the BETA Threshold

# JOB 4: Pre-Blocking Full References 
#        one Mapper & one Reducer....Outputs rebuilt references for each refID
hadoop jar $STREAMJAR \
    -files HDWM/HDWM020_PBM.py,HDWM/HDWM020_FSR.py \
    -D mapreduce.job.reduces=1 \
    -input HDWM/job3_out \
    -output HDWM/job4_out \
    -mapper HDWM020_PBM.py \
    -reducer HDWM020_FSR.py

# JOB 5: Pre-Blocking of Blocking Tokens 
#        one Mapper & one Reducer....Outputs a list of all references that meet the BETA threshold
hadoop jar $STREAMJAR \
    -files HDWM/HDWM020_PBM.py,HDWM/HDWM020_TLR.py,HDWM/parmStage.txt \
    -D mapreduce.job.reduces=1 \
    -input HDWM/job3_out \
    -output HDWM/job5_out \
    -mapper HDWM020_PBM.py \
    -reducer HDWM020_TLR.py \

# JOB 6: Creation of Blocking refID Pairs
#        one Mapper & one Reducer....Outputs pairs of refIDs to be compared
hadoop jar $STREAMJAR \
    -files HDWM/HDWM025_BTPM.py,HDWM/HDWM025_BTPR.py,HDWM/parmStage.txt \
    -D mapreduce.job.reduces=1 \
    -input HDWM/job5_out \
    -output HDWM/job6_out \
    -mapper HDWM025_BTPM.py \
    -reducer HDWM025_BTPR.py

# JOB 7: Creation of Blocking refID Pairs (further split)
#        Identity Mapper & one Reducer....Outputs pairs of refIDs to be compared
hadoop jar $STREAMJAR \
    -files HDWM/HDWM030_RPDR.py \
    -D mapreduce.job.reduces=1  \
    -input HDWM/job6_out \
    -output HDWM/job7_out \
    -mapper $Identity_Mapper \
    -reducer HDWM030_RPDR.py

# JOB 8: Merge and Join BlockPairs with Original References to update BlockPairsRefID with full Metadata Information
#        One Mapper, One Reducer |sort| Another Reducer . Takes and merge two inputs (job4 output, job7 output)
hadoop jar $STREAMJAR \
    -files HDWM/HDWM035_RIDM.py,HDWM/HDWM035_RIDR.py \
    -D mapreduce.job.reduces=1 \
    -Dstream.num.map.output.key.fields=2 \
    -input HDWM/job4_out \
    -input HDWM/job7_out \
    -output HDWM/job7_tmp_out  \
    -mapper HDWM035_RIDM.py \
    -reducer HDWM035_RIDR.py

hadoop jar $STREAMJAR \
    -files HDWM/HDWM035_RIDRR.py \
    -D mapreduce.job.reduces=1 \
    -input HDWM/job7_tmp_out \
    -output HDWM/job8_out \
    -mapper $Identity_Mapper \
    -reducer HDWM035_RIDRR.py
#hadoop fs -rm -r HDWM/job7_tmp_out  #Removes tmp_out because it is not needed

# JOB 9: Linked Pairs
#        Identity Mapper & one Reducer....Outputs Linked Pairs
hadoop jar $STREAMJAR \
    -files HDWM/HDWM050_SMCR.py,HDWM/parmStage.txt \
    -D mapreduce.job.reduces=1  \
    -input HDWM/job8_out \
    -output HDWM/job9_out \
    -mapper $Identity_Mapper \
    -reducer HDWM050_SMCR.py

# Move job 9 output into a temp_in directory which will serve as input for TC 
hadoop fs -mv HDWM/job9_out HDWM/job10_temp_in

# JOB 10: Transitive Closure Iteration
# It finds all the connected components until no more merge state
#        Identity Mapper & one Reducer
iterationCounter=0
while true
do
    bool=$(cat ./HDWM/check.txt)
    echo "$bool"
    if [[ "$bool" == "True" ]]
    then
      hadoop jar $STREAMJAR \
        -files HDWM/HDWM055_CCMRR.py \
        -D mapreduce.job.reduces=1  \
        -input HDWM/job10_temp_in \
        -output HDWM/job10_temp_out \
        -mapper $Identity_Mapper \
        -reducer HDWM055_CCMRR.py
      hadoop fs -rm -r HDWM/job10_temp_in    
      hadoop fs -mv HDWM/job10_temp_out HDWM/job10_temp_in
      iterationCounter=$((iterationCounter+1))
    else
      echo "True" > ./HDWM/check.txt
      break
    fi
done

# JOB 11: Add the Component Records to the final iteration of Transitive Closure
# .i.e. Adds record matching itself to each cluster group
#        Identity Mapper & one Reducer
hadoop jar $STREAMJAR \
    -files HDWM/HDWM055_COMR.py \
    -D mapreduce.job.reduces=1  \
    -input HDWM/job10_temp_in \
    -output HDWM/job11_out \
    -mapper $Identity_Mapper \
    -reducer HDWM055_COMR.py
echo ----------------------------------------------------------------------------------
echo "Transitive Closure total iterations is:" $iterationCounter
echo ----------------------------------------------------------------------------------

echo ----------------------------------------------------------------------------------
echo "------ Starting ER Matrix ------"
echo ----------------------------------------------------------------------------------
# Calculate Matrix of the ER Process
# Make sure to use 1 mapper, 1 reducer and should be executed on only the master node
# JOB 12: Merge Truth Dataset and the outputs of Job 11
hadoop jar $STREAMJAR \
    -files HDWM/HDWM095_ERMM.py,HDWM/HDWM095_ERMR.py \
    -D mapreduce.job.reduces=1 \
    -Dstream.num.map.output.key.fields=2 \
    -input HDWM/$truthFile \
    -input HDWM/job11_out \
    -output HDWM/job12_out \
    -mapper HDWM095_ERMM.py \
    -reducer HDWM095_ERMR.py

# JOB 13: Takes output from job 12 and calculates Equivalent Pairs
hadoop jar $STREAMJAR \
    -files HDWM/HDWM098_EEPR.py \
    -D mapreduce.job.reduces=1 \
    -Dstream.num.map.output.key.fields=1 \
    -D mapred.text.key.partitioner.options=-k2,2 \
    -D mapred.text.key.comparator.options=-"-k2n,2 -t','" \
    -input HDWM/job12_out \
    -output HDWM/job13_out \
    -mapper $Identity_Mapper \
    -reducer HDWM098_EEPR.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

# JOB 14: Takes output from job 12 and calculates Equivalent Pairs
hadoop jar $STREAMJAR \
    -files HDWM/HDWM098_EFCR.py \
    -D mapreduce.job.reduces=1 \
    -Dstream.num.map.output.key.fields=2 \
    -D mapred.text.key.partitioner.options=-k1,2 \
    -D mapred.text.key.comparator.options=-"-k1,1 -k2n,2 -t','" \
    -input HDWM/job13_out \
    -input HDWM/job12_out \
    -output HDWM/job14_out \
    -mapper $Identity_Mapper \
    -reducer HDWM098_EFCR.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner