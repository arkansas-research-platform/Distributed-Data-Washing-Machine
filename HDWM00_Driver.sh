#!/bin/bash

#echo "HADOOP DATA WASHING MACHINE" << $logFile
#### GETTING PARAMETER FILE FROM USER ####
read -p "Enter parameter file: " parmFile
#-files hdfs://snodemain:9000/user/nick/$(pwd)/$parameter_file 
if [[ -f "$(pwd)/$parmFile" ]]
then
    # Creating logfile using current date and time
    startTime=$( date '+%F_%H:%M:%S' )
    export Log_File="$(pwd)/HDWM_log_$startTime.txt"
    while IFS='=' read -r line val
    do
        if [[ "$line" = inputFileName* ]]
        then
            echo "***********************************************" >> $Log_File
            echo "         Summary of Parameter Settings         " >> $Log_File
            echo "         -----------------------------         " >> $Log_File
            inputFile="$val"
            echo "Input File to process      -->  $inputFile" >> $Log_File       
            continue
        elif [[ "$line" = hasHeader* ]]
        then
            header="$val"
            echo "File has Header            -->  $header" >> $Log_File         
            continue
        elif [[ "$line" = delimiter* ]]
        then
            delimiter="$val"
            echo "Delimeter                  -->  '$delimiter' " >> $Log_File
            continue
        elif [[ "$line" = tokenizerType* ]]
        then
            tokenizer="$val"
            echo "Tokenizer type             -->  $tokenizer" >> $Log_File     
            continue
        elif [[ "$line" = truthFileName* ]]
        then
            truthFile="$val"
            echo "TruthSet File              -->  $truthFile" >> $Log_File    
            continue 
        elif [[ "$line" = beta* ]]
        then
            beta="$val"
            echo "Beta                       -->  $beta" >> $Log_File        
            continue 
        elif [[ "$line" = blockByPairs* ]]
        then
            blockByPairs="$val"
            echo "Block by Pairs             -->  $blockByPairs" >> $Log_File 
            continue 
        elif [[ "$line" = minBlkTokenLen* ]]
        then
            minBlkTokenLen="$val"
            echo "Min. Blocking Token Length -->  $minBlkTokenLen" >> $Log_File  
            continue 
        elif [[ "$line" = excludeNumericBlocks* ]]
        then
            excludeNumTok="$val"
            echo "Exclude Num. Block Tokens  -->  $excludeNumTok" >> $Log_File  
            continue 
        elif [[ "$line" = sigma* ]]
        then
            sigma="$val"
            echo "Sigma                      -->  $sigma" >> $Log_File        
            continue 
        elif [[ "$line" = removeDuplicateTokens* ]]
        then
            deDupTokens="$val"
            echo "Remove Dup. Ref. Tokens    -->  $deDupTokens" >> $Log_File  
            continue
        elif [[ "$line" = removeExcludedBlkTokens* ]]
        then
            removeExcBlkTok="$val"
            echo "Remove Excl. Block Tokens  -->  $removeExcBlkTok" >> $Log_File  
            continue
        elif [[ "$line" = mu ]]
        then
            mu="$val"
            echo "Mu                         -->  $mu" >> $Log_File           
            continue
        elif [[ "$line" = comparator* ]]
        then
            comparator="$val"
            echo "Matrix Comparator          -->  $comparator" >> $Log_File   
            echo "***********************************************" >> $Log_File 
            continue
        fi
    done < "$(pwd)/$parmFile"

    ######## BEGINNING OF HADOOP JOBS #########
    # Create HDFS directory
    #hdfs dfs -mkdir <Directory Name>
    # Once a job is started, a directory is automatically created in HDFS
    hdfs dfs -rm -r HadoopDWM   
    hdfs dfs -mkdir HadoopDWM

    # Copy input data and truthSet from local directory to HDFS
    hdfs dfs -put $(pwd)/$inputFile HadoopDWM
    hdfs dfs -put $(pwd)/$truthFile HadoopDWM

    # Copy contents of the given parameter file to a staging area to be shipped to Distributed Cache
    cp $(pwd)/$parmFile $(pwd)/parmStage.txt

    # Create some variables to be reused. These are just paths to important repetitive JAR Libraries
    Identity_Mapper=/bin/cat
    Identity_Reducer=org.apache.hadoop.mapred.lib.IdentityReducer
    STREAMJAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar

    #################################################
    # START EXECUTION OF HDWM JOBS
    #################################################
#--------->  PHASE 1: TOKENIZATION PROCESS <---------
    # JOB 1: Tokenization
    # Tokenize each row of Ref and form Metadata 
    #        one Mapper with Identity Reducer. outputs tokens with metadata (there are duplicates) without frequency
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM010_TM.py,$(pwd)/parmStage.txt \
        -D mapreduce.job.reduces=1 \
        -input HadoopDWM/$inputFile \
        -output HadoopDWM/job1_out \
        -mapper HDWM010_TM.py \
        -reducer $Identity_Reducer

#--------->  PHASE 2: FREQUENCY GENERATION PROCESS <---------
    # JOB 2: Calculate Frequency of Tokens 
    #        one Mapper & one Reducer....Outputs keys and their frequencies
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM010_TM.py,$(pwd)/HDWM010_TR.py,$(pwd)/parmStage.txt \
        -D mapreduce.job.reduces=1 \
        -input HadoopDWM/$inputFile \
        -output HadoopDWM/job2_out \
        -mapper HDWM010_TM.py \
        -reducer HDWM010_TR.py

    # JOB 3: Update the Metadata information with Calculated Freq (Joining outputs of Job 1 & Job 2)
    #        one Mapper & one Reducer....Outputs keys and their frequency
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM015_JM.py,$(pwd)/HDWM015_JR.py \
        -D mapreduce.job.reduces=1 \
        -Dstream.num.map.output.key.fields=2 \
        -input HadoopDWM/job1_out \
        -input HadoopDWM/job2_out \
        -output HadoopDWM/job3_out \
        -mapper HDWM015_JM.py \
        -reducer HDWM015_JR.py

    # In Jobs 4&5, the goal is to produce two types of outputs:
    #     output1 (Job 4) - Full references with all tokens, frequencies, and position
    #     output2 (Job 5) - List of all tokens that satisfy the BETA Threshold

#--------->  PHASE 3: REFERENCE RECREATION PROCESS <---------
    # JOB 4: Pre-Blocking Full References 
    #        one Mapper & one Reducer....Outputs rebuilt references for each refID
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM020_PBM.py,$(pwd)/HDWM020_FSR.py \
        -D mapreduce.job.reduces=1 \
        -input HadoopDWM/job3_out \
        -output HadoopDWM/job4_out \
        -mapper HDWM020_PBM.py \
        -reducer HDWM020_FSR.py

    # Copy job 4 output to a tmp file
    hdfs dfs -cp HadoopDWM/job4_out HadoopDWM/progLoop_in

####################################################
########## STARTING PROGRAM ITERATIVE LOOP #########
####################################################
    echo "--- STARTING PROGRAM ITERATIVE LOOP"
    echo "        " >> $Log_File
    echo "+++++++++ STARTING PROGRAM ITERATIVE LOOP +++++++++" >> $Log_File

    proLoopCounter=0
    while true
    do
    # Update loop counter
    echo "--- STARTING NEXT ITERATION!!!!"
    echo "        " >> $Log_File
    echo " >>>>> STARTING NEXT ITERATION >>>>>" >> $Log_File
    proLoopCounter=$((proLoopCounter+1))
    
    #--------->  PHASE 4: BLOCKING PROCESS <---------
        # JOB 5: Extract all Blocking Tokens, and Create of Blocking refID Pairs
        #        one Mapper & one Reducer....Outputs pairs of refIDs to be compared
        hdfs dfs -rm -r HadoopDWM/job5_out  
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM025_BTPM.py,$(pwd)/HDWM025_BTPR.py,$(pwd)/parmStage.txt \
            -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
            -Dstream.num.map.output.key.fields=2 \
            -D mapreduce.map.output.key.field.separator=, \
            -Dstream.num.reduce.output.key.fields=2 \
            -D mapreduce.reduce.output.key.field.separator=, \
            -D stream.map.input.field.separator=: \
            -D stream.map.output.field.separator=: \
            -D stream.reduce.input.field.separator=: \
            -D stream.reduce.output.field.separator=: \
            -D mapreduce.job.reduces=1 \
            -input HadoopDWM/progLoop_in \
            -output HadoopDWM/job5_out \
            -mapper HDWM025_BTPM.py \
            -reducer HDWM025_BTPR.py
        
        # Check if Block Pair List is empty
        blkPairListCheck=$(cat $(pwd)/reportBlkPairList.txt)
        if (( "$blkPairListCheck" == 0 ))
        then
            echo "--- Ending because Block Pair List is empty"
            echo "--- Ending because Block Pair List is empty" >> $Log_File
            echo "--- END OF PROGRAM LOOP"
            echo "        " >> $Log_File
            echo "+++++++++ END OF PROGRAM LOOP with $proLoopCounter total iterations +++++++++" >> $Log_File
            # Copy this job's output to a file ready to be processed for final LinkedIndex
            hdfs dfs -cp HadoopDWM/job5_out HadoopDWM/job_LinkIndexDirty
        break
        fi
        
        # JOB 6: Block Deduplication 
        #        Identity Mapper & one Reducer....Outputs pairs of refIDs to be compared
        hdfs dfs -rm -r HadoopDWM/job6_out
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM030_RPDR.py \
            -D mapreduce.job.reduces=1  \
            -input HadoopDWM/job5_out \
            -output HadoopDWM/job6_out \
            -mapper $Identity_Mapper \
            -reducer HDWM030_RPDR.py
    
        # JOB 7: Merge and Join BlockPairs with Original References to update BlockPairsRefID with full Metadata Information
        #        One Mapper, One Reducer |sort| Another Reducer . Takes and merge two inputs (job4 output, job7 output)
        hdfs dfs -rm -r HadoopDWM/job6_tmp_out
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM035_RIDM.py,$(pwd)/HDWM035_RIDR.py \
            -D mapreduce.job.reduces=1 \
            -Dstream.num.map.output.key.fields=2 \
            -input HadoopDWM/job4_out \
            -input HadoopDWM/job6_out \
            -output HadoopDWM/job6_tmp_out  \
            -mapper HDWM035_RIDM.py \
            -reducer HDWM035_RIDR.py
    
        hdfs dfs -rm -r HadoopDWM/job7_out
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM035_RIDRR.py \
            -D mapreduce.job.reduces=1 \
            -input HadoopDWM/job6_tmp_out \
            -output HadoopDWM/job7_out \
            -mapper $Identity_Mapper \
            -reducer HDWM035_RIDRR.py
    
    #--------->  PHASE 5: SIMILARITY COMPARISON & LINKING PROCESS <---------
        # JOB 9: Linked Pairs
        #        Identity Mapper & one Reducer....Outputs Linked Pairs
        hdfs dfs -rm -r HadoopDWM/job8_out
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM050_SMCR.py,$(pwd)/parmStage.txt \
            -input HadoopDWM/job7_out \
            -output HadoopDWM/job8_out \
            -mapper $Identity_Mapper \
            -reducer HDWM050_SMCR.py

        # Check if Linked Pair List is empty
        linkPairListCheck=$(cat $(pwd)/reportLinkPairList.txt)
        if (( "$linkPairListCheck" == 0 ))
        then
            echo "--- Ending because Link Pair List is empty"
            echo "--- Ending because Link Pair List is empty" >> $Log_File
            echo "--- END OF PROGRAM LOOP"
            echo "        " >> $Log_File
            echo "+++++++++ END OF PROGRAM LOOP with $proLoopCounter total iterations +++++++++" >> $Log_File
            # Copy this job's output to a file ready to be processed for final LinkedIndex
            hdfs dfs -cp HadoopDWM/job8_out HadoopDWM/job_LinkIndexDirty
        break
        fi

    #--------->  PHASE 6: TRANSITIVE CLOSURE PROCESS <---------
        # JOB 9: Transitive Closure Iteration
        # It finds all the connected components until no more merge state
        #        Identity Mapper & one Reducer
        
        # Move job 8 output into a temp_in directory which will serve as input for TC 
        hdfs dfs -rm -r HadoopDWM/job9_temp_in
        hdfs dfs -cp HadoopDWM/job8_out HadoopDWM/job9_temp_in
        iterationCounter=0
        while true
        do
            #bool=$(cat $(pwd)/$(pwd)/reportTCiteration.txt)
            count=$(cat $(pwd)/reportTCiteration.txt)
            echo "Current RunNextIteration Counter is:---->>>> $count"
            #if [[ "$bool" == "True" ]]
            if (( "$count" > 0 ))
            then
            hdfs dfs -rm -r HadoopDWM/job9_temp_out
            hadoop jar $STREAMJAR \
                -files $(pwd)/HDWM055_CCMRR.py \
                -D stream.map.output.field.separator=, \
                -D stream.num.map.output.key.fields=2 \
                -D mapreduce.job.reduces=1  \
                -input HadoopDWM/job9_temp_in \
                -output HadoopDWM/job9_temp_out \
                -mapper $Identity_Mapper \
                -reducer HDWM055_CCMRR.py
            hdfs dfs -rm -r HadoopDWM/job9_temp_in    
            hdfs dfs -mv HadoopDWM/job9_temp_out HadoopDWM/job9_temp_in
            iterationCounter=$((iterationCounter+1))
            else
            #echo "True" > $(pwd)/reportTCiteration.txt
            echo "9999" > $(pwd)/reportTCiteration.txt
            break
            fi
        done
        echo "          " >> $Log_File   
        echo ">> Starting Transitive Closure Process" >> $Log_File   
        echo "   Total Transitive Closure Iterations: " $iterationCounter >> $Log_File
    
        # Check if Cluster List is empty
        clusterListCheck=$(cat $(pwd)/reportClusterList.txt)
        if (( "$clusterListCheck" == 0 ))
        then
            echo "--- Ending because Cluster List is empty"
            echo "--- Ending because Cluster List is empty" >> $Log_File
            echo "--- END OF PROGRAM LOOP"
            echo "        " >> $Log_File
            echo "+++++++++ END OF PROGRAM LOOP with $proLoopCounter total iterations +++++++++" >> $Log_File
            # Copy this job's output to a file ready to be processed for final LinkedIndex
            hdfs dfs -cp HadoopDWM/job9_temp_in HadoopDWM/job_LinkIndexDirty
        break
        fi

    #--------->  PHASE 7: CLUSTER EVALUATION PROCESS <---------
        # JOB 10: Update RefIDs in Clusters with their token metadata
        #         by using output from Transitive Closure and original dataset
        hdfs dfs -rm -r HadoopDWM/job10_out
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM060_LKIM.py,$(pwd)/HDWM060_LKIR.py \
            -D stream.map.input.field.separator=, \
            -D stream.map.output.field.separator=, \
            -D stream.reduce.input.field.separator=, \
            -D mapreduce.map.output.key.field.separator=. \
            -D stream.num.map.output.key.fields=2 \
            -D mapreduce.reduce.output.key.field.separator=. \
            -D stream.num.reduce.output.key.fields=2 \
            -D mapreduce.job.reduces=1 \
            -input HadoopDWM/job9_temp_in \
            -input HadoopDWM/job4_out \
            -output HadoopDWM/job10_out  \
            -mapper HDWM060_LKIM.py \
            -reducer HDWM060_LKIR.py
    
        hdfs dfs -rm -r HadoopDWM/job11_tmp
        # JOB 11a: Calculate Entropy and Differentiate Good and Bad Clusters
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM070_CECR.py \
            -D mapreduce.job.reduces=1 \
            -input HadoopDWM/job10_out \
            -output HadoopDWM/job11_tmp \
            -mapper $Identity_Mapper \
            -reducer HDWM070_CECR.py

        hdfs dfs -rm -r HadoopDWM/job11_tmpLinkIndex
        # JOB 11b: Check if a ref is already processed, add another tag as used
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM075_TCRM.py,$(pwd)/HDWM075_TCRR.py \
            -D mapreduce.job.reduces=1 \
            -input HadoopDWM/job11_tmp \
            -output HadoopDWM/job11_tmpLinkIndex \
            -mapper HDWM075_TCRM.py \
            -reducer HDWM075_TCRR.py
    
        # Coping and Deleting
        hdfs dfs -rm -r HadoopDWM/progLoop_in  
        hdfs dfs -cp HadoopDWM/job11_tmpLinkIndex HadoopDWM/progLoop_in
    done    
####################################################
########## END OF PROGRAM LOOP #########
####################################################

    # JOB 12: Create a Linked Index File
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM077_LKINM.py,$(pwd)/HDWM077_LKINR.py \
        -D mapreduce.job.reduces=1 \
        -input HadoopDWM/job_LinkIndexDirty \
        -input HadoopDWM/job4_out \
        -output HadoopDWM/LinkedIndex_$inputFile \
        -mapper HDWM077_LKINM.py \
        -reducer HDWM077_LKINR.py
    
    # Copy LinkedIndex file from HDFS to local directory
    #hdfs dfs -get HadoopDWM/LinkedIndex_$inputFile $(pwd)

# job12_finalLinkedIndex
#--------->  PHASE 8: ER MATRIX PROCESS <---------
#    # Calculate Matrix of the ER Process
#    # Make sure to use 1 mapper, 1 reducer and should be executed on only the master node

    # JOB 13: Merge Truth Dataset and the outputs of Job 11
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM095_PERMM.py,$(pwd)/HDWM095_PERMR.py \
        -D mapreduce.job.reduces=1 \
        -input HadoopDWM/$truthFile \
        -input HadoopDWM/LinkedIndex_$inputFile \
        -output HadoopDWM/job13_out \
        -mapper HDWM095_PERMM.py \
        -reducer HDWM095_PERMR.py

    # JOB 14: Calculate E-pairs, L-pairs, TP-pairs, Precision, Recall, F-score
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM099_ERMR.py \
        -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
        -Dstream.num.map.output.key.fields=2 \
        -D mapreduce.map.output.key.field.separator=, \
        -D mapreduce.partition.keycomparator.options="-k1,1 -k2,2n" \
        -D mapreduce.job.reduces=1 \
        -input HadoopDWM/job13_out \
        -output HadoopDWM/job14_out \
        -mapper $Identity_Mapper \
        -reducer HDWM099_ERMR.py \

    # The Created directory is automatically deleted at the end of the job
    # Uncomment this line if you want to see all the processes that went on 
    #hdfs dfs -rm -r HadoopDWM    

    # Exiting program if the parameter file specified does not exists
    exit 0
fi
echo "The file, '$parmFile', is not a valid parameter file. Try again!" 
