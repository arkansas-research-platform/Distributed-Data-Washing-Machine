#!/bin/bash

#### GETTING PARAMETER FILE FROM USER ####
read -p "Enter parameter file: " parmFile
#-files hdfs://snodemain:9000/user/nick/$(pwd)/$parameter_file 
if [[ -f "$(pwd)/$parmFile" ]]
then
    # Creating logfile using current date and time
    startTime=$( date '+%F_%H:%M:%S' )
    #export Log_File="$(pwd)/HDWM_log_$startTime.txt"

    # Create a tmpDir for the job logs
    tmpDir="/usr/local/jobTmp"
    sudo mkdir -m777 "$tmpDir" && echo "Temp Directory for Job is Successfully Created."

    Log_File="$tmpDir/HDWM_log.txt"
    finalLogFile="HDWM_Log_$startTime"


    # Create tmp file for local reporting
    #touch "$(pwd)/tmpReport.txt"
    touch "$tmpDir/tmpReport.txt"

    # Create logfile for transitive closure loop
    touch "$tmpDir/reportTCiteration.txt" && echo "9999" > $tmpDir/reportTCiteration.txt

    # Create a file to log mu and epsilon value inside
    touch "$tmpDir/muReport.txt"
    touch "$tmpDir/epsilonReport.txt"

    while IFS='=' read -r line val
    do
        if [[ "$line" = inputFileName* ]]
        then
            echo "HADOOP DATA WASHING MACHINE" >> $Log_File
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
        elif [[ "$line" = comparator* ]]
        then
            comparator="$val"
            echo "Matrix Comparator          -->  $comparator" >> $Log_File   
            continue
        elif [[ "$line" = mu ]]
        then
            export mu="$val"
            echo "Mu                         -->  $mu" >> $Log_File           
            continue
        elif [[ "$line" = muIterate ]]
        then
            muIter="$val"
            echo "Mu Iterate                 -->  $muIter" >> $Log_File           
            continue
        elif [[ "$line" = epsilon ]]
        then
            export epsilon="$val"
            echo "Epsilon                    -->  $epsilon" >> $Log_File           
            continue
        elif [[ "$line" = epsilonIterate ]]
        then
            epsilonIter="$val"
            echo "Epsilon Iterate            -->  $epsilonIter" >> $Log_File 
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
    hdfs dfs -put $(pwd)/parmStage.txt HadoopDWM
    
    # Create some variables to be reused. These are just paths to important repetitive JAR Libraries
    Identity_Mapper=/bin/cat
    Identity_Reducer=org.apache.hadoop.mapred.lib.IdentityReducer
    STREAMJAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar

    #hdfs://snodemain:9000/user/nick/HadoopDWM/parmStage.txt

    #################################################
    # START EXECUTION OF HDWM JOBS
    #################################################
#--------->  PHASE 1: TOKENIZATION & FREQUENCY CALCULATION PROCESS <---------
    # JOB 1: Tokenize each row of Ref and form Metadata and Calculate Frequency of Tokens 
    #        one Mapper & one Reducer....Outputs keys and their frequencies
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM010_TM.py,$(pwd)/HDWM010_TR.py,hdfs://snodemain:9000/user/nick/HadoopDWM/parmStage.txt#parms \
        -input HadoopDWM/$inputFile \
        -output HadoopDWM/job1_Tokens-Freq \
        -mapper HDWM010_TM.py \
        -reducer HDWM010_TR.py

    # JOB 2: Update the Metadata information with Calculated Freq (Joining outputs of Job 1 & Job 2)
    #        one Mapper & one Reducer....Outputs keys and their frequency
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM015_JM.py,$(pwd)/HDWM015_JR.py \
        -input HadoopDWM/job1_Tokens-Freq \
        -output HadoopDWM/job2_Updated-Mdata \
        -mapper HDWM015_JM.py \
        -reducer HDWM015_JR.py

#--------->  PHASE 2: REFERENCE RECREATION PROCESS <---------
    # JOB 3: Pre-Blocking Full References 
    #        one Mapper & one Reducer....Outputs rebuilt references for each refID
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM020_PBM.py,$(pwd)/HDWM020_FSR.py \
        -input HadoopDWM/job2_Updated-Mdata\
        -output HadoopDWM/job3_RecreateRefs \
        -mapper HDWM020_PBM.py \
        -reducer HDWM020_FSR.py

    # Copy job 4 output to a tmp file
    hdfs dfs -cp HadoopDWM/job3_RecreateRefs HadoopDWM/progLoop_in

####################################################
########## STARTING PROGRAM ITERATIVE LOOP #########
####################################################
    echo "+++++++++ STARTING PROGRAM ITERATIVE LOOP +++++++++"
    echo "        " >> $Log_File
    echo "+++++++++ STARTING PROGRAM ITERATIVE LOOP +++++++++" >> $Log_File

    programCounter=0
    while true
    do
        # Update mu and Epsilon log files in tmpDir
        echo $mu > "$tmpDir/muReport.txt"
        echo $epsilon > "$tmpDir/epsilonReport.txt"

        # Update loop counter
        echo ">>>>> STARTING NEXT ITERATION at" $mu " Mu >>>>>"
        echo "        " >> $Log_File
        echo ">>>>> STARTING NEXT ITERATION >>>>>" >> $Log_File
        echo "   New Mu --> " $mu >> $Log_File
        echo "   New Epsilon --> " $epsilon >> $Log_File

        if [[ "$mu" > 1 ]]
        then
            echo "--- Ending because Mu > 1"
            echo "--- Ending because Mu > 1" >> $Log_File
            echo "--- END OF PROGRAM LOOP"
            echo "        " >> $Log_File
            echo "+++++++++ END OF PROGRAM LOOP WITH  [ $programCounter ] ITERATION(S) +++++++++" >> $Log_File
            # Copy this job's output to a file ready to be processed for final LinkedIndex
            hdfs dfs -cp HadoopDWM/progLoop_in HadoopDWM/job_LinkIndexDirty
        break
        fi
    
    #--------->  PHASE 4: BLOCKING PROCESS <---------
        # JOB 4: Extract all Blocking Tokens, and Create of Blocking refID Pairs
        #        one Mapper & one Reducer....Outputs pairs of refIDs to be compared
        hdfs dfs -rm -r HadoopDWM/job4_BlockTokens
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM025_BTPM.py,$(pwd)/HDWM025_BTPR.py,hdfs://snodemain:9000/user/nick/HadoopDWM/parmStage.txt#parms \
            -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
            -Dstream.num.map.output.key.fields=2 \
            -D mapreduce.map.output.key.field.separator=, \
            -Dstream.num.reduce.output.key.fields=2 \
            -D mapreduce.reduce.output.key.field.separator=, \
            -D stream.map.input.field.separator=: \
            -D stream.map.output.field.separator=: \
            -D stream.reduce.input.field.separator=: \
            -D stream.reduce.output.field.separator=: \
            -input HadoopDWM/progLoop_in \
            -output HadoopDWM/job4_BlockTokens \
            -mapper HDWM025_BTPM.py \
            -reducer HDWM025_BTPR.py
        
        # Check if Block Pair List is empty
        #blkPairListCheck=$(cat $(pwd)/tmpReport.txt)
        blkPairListCheck=$(cat $tmpDir/tmpReport.txt)
        if [[ "$blkPairListCheck" == "0" ]]
        then
            echo "--- Ending because Block Pair List is empty"
            echo "--- Ending because Block Pair List is empty" >> $Log_File
            echo "--- END OF PROGRAM LOOP"
            echo "        " >> $Log_File
            echo "+++++++++ END OF PROGRAM LOOP WITH  [ $programCounter ] ITERATION(S) +++++++++" >> $Log_File
            # Copy this job's output to a file ready to be processed for final LinkedIndex
            hdfs dfs -cp HadoopDWM/job4_BlockTokens HadoopDWM/job_LinkIndexDirty
        break
        fi
        
        # JOB 5: Block Deduplication 
        #        Identity Mapper & one Reducer....Outputs pairs of refIDs to be compared
        hdfs dfs -rm -r HadoopDWM/job5_BlockDedup
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM030_RPDR.py \
            -input HadoopDWM/job4_BlockTokens \
            -output HadoopDWM/job5_BlockDedup \
            -mapper $Identity_Mapper \
            -reducer HDWM030_RPDR.py
    
        # JOB 6a: Merge and Join BlockPairs with Original References to update BlockPairsRefID with full Metadata Information
        #        One Mapper, One Reducer |sort| Another Reducer . Takes and merge two inputs (job4 output, job7 output)
        hdfs dfs -rm -r HadoopDWM/job6_tmp_out
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM035_RIDM.py,$(pwd)/HDWM035_RIDR.py \
            -Dstream.num.map.output.key.fields=2 \
            -input HadoopDWM/job3_RecreateRefs \
            -input HadoopDWM/job5_BlockDedup \
            -output HadoopDWM/job6_tmp_out  \
            -mapper HDWM035_RIDM.py \
            -reducer HDWM035_RIDR.py
    
        # Job 6b: Final unduplicated Block Pairs
        hdfs dfs -rm -r HadoopDWM/job6_UndupBlockPairs
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM035_RIDRR.py \
            -input HadoopDWM/job6_tmp_out \
            -output HadoopDWM/job6_UndupBlockPairs \
            -mapper $Identity_Mapper \
            -reducer HDWM035_RIDRR.py
    
    #--------->  PHASE 5: SIMILARITY COMPARISON & LINKING PROCESS <---------
        # JOB 7: Linked Pairs
        #        Identity Mapper & one Reducer....Outputs Linked Pairs
        hdfs dfs -rm -r HadoopDWM/job7_LinkedPairs
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM050_SMCR.py,hdfs://snodemain:9000/user/nick/HadoopDWM/parmStage.txt#parms \
            -input HadoopDWM/job6_UndupBlockPairs \
            -output HadoopDWM/job7_LinkedPairs \
            -mapper $Identity_Mapper \
            -reducer HDWM050_SMCR.py

        # Check if Linked Pair List is empty
        #linkPairListCheck=$(cat $(pwd)/tmpReport.txt)
        linkPairListCheck=$(cat $tmpDir/tmpReport.txt)
        if [[ "$linkPairListCheck" == "0" ]]
        then
            echo "--- Ending because Link Pair List is empty"
            echo "--- Ending because Link Pair List is empty" >> $Log_File
            echo "--- END OF PROGRAM LOOP"
            echo "        " >> $Log_File
            echo "+++++++++ END OF PROGRAM LOOP WITH  [ $programCounter ] ITERATION(S) +++++++++" >> $Log_File
            # Copy this job's output to a file ready to be processed for final LinkedIndex
            hdfs dfs -cp HadoopDWM/job7_LinkedPairs HadoopDWM/job_LinkIndexDirty
        break
        fi

    #--------->  PHASE 6: TRANSITIVE CLOSURE PROCESS <---------
        # JOB 8: Transitive Closure Iteration
        # It finds all the connected components until no more merge state
        #        Identity Mapper & one Reducer
        
        # Move job 7 output into a temp_in directory which will serve as input for TC 
        hdfs dfs -rm -r HadoopDWM/job8_tmpIn
        hdfs dfs -cp HadoopDWM/job7_LinkedPairs HadoopDWM/job8_tmpIn
        iterationCounter=0
        # Write a '9999' value into tmpReport file to be used by 1st iteration
        #echo "9999" > $(pwd)/tmpReport.txt
        while true
        do
            #bool=$(cat $(pwd)/$(pwd)/reportTCiteration.txt)
            #count=$(cat $(pwd)/reportTCiteration.txt)
            count=$(cat $tmpDir/reportTCiteration.txt)
            #count=$(cat $(pwd)/tmpReport.txt)
            echo "Current RunNextIteration Counter is:---->>>> $count"
            #if [[ "$bool" == "True" ]]
            if (( "$count" > 0 ))
            then
            hdfs dfs -rm -r HadoopDWM/job8_tmpOut
            hadoop jar $STREAMJAR \
                -files $(pwd)/HDWM055_CCMRR.py \
                -D stream.map.output.field.separator=, \
                -D stream.num.map.output.key.fields=2 \
                -input HadoopDWM/job8_tmpIn \
                -output HadoopDWM/job8_tmpOut \
                -mapper $Identity_Mapper \
                -reducer HDWM055_CCMRR.py
            hdfs dfs -rm -r HadoopDWM/job8_tmpIn    
            hdfs dfs -mv HadoopDWM/job8_tmpOut HadoopDWM/job8_tmpIn
            iterationCounter=$((iterationCounter+1))
            else
            #echo "True" > $(pwd)/reportTCiteration.txt
            #echo "9999" > $(pwd)/reportTCiteration.txt
            echo "9999" > $tmpDir/reportTCiteration.txt
            break
            fi
        done
        echo "          " >> $Log_File   
        echo ">> Starting Transitive Closure Process" >> $Log_File   
        echo "   Total Transitive Closure Iterations: " $iterationCounter >> $Log_File
    
        # Check if Cluster List is empty
        #clusterListCheck=$(cat $(pwd)/tmpReport.txt)
        clusterListCheck=$(cat $tmpDir/tmpReport.txt)
        # Report clusterList to log file
        echo "   Size of Cluster List Formed from TC: " $clusterListCheck >> $Log_File
        if (( "$clusterListCheck"==0 ))
        then
            echo "--- Ending because Cluster List is empty"
            echo "--- Ending because Cluster List is empty" >> $Log_File
            echo "--- END OF PROGRAM LOOP"
            echo "        " >> $Log_File
            echo "+++++++++ END OF PROGRAM LOOP WITH  [ $programCounter ] ITERATION(S) +++++++++" >> $Log_File
            # Copy this job's output to a file ready to be processed for final LinkedIndex
            hdfs dfs -cp HadoopDWM/job8_tmpIn HadoopDWM/job_LinkIndexDirty
        break
        fi

    #--------->  PHASE 7: CLUSTER EVALUATION PROCESS <---------
        # JOB 9: Update RefIDs in Clusters with their token metadata
        #         by using output from Transitive Closure and original dataset
        hdfs dfs -rm -r HadoopDWM/job9_TCout-Mdata
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM060_LKIM.py,$(pwd)/HDWM060_LKIR.py \
            -input HadoopDWM/job8_tmpIn \
            -input HadoopDWM/job3_RecreateRefs \
            -output HadoopDWM/job9_TCout-Mdata  \
            -mapper HDWM060_LKIM.py \
            -reducer HDWM060_LKIR.py

        hdfs dfs -rm -r HadoopDWM/job10_ClusterEval
        # JOB 10a: Calculate Entropy and Differentiate Good and Bad Clusters
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM070_CECR.py,hdfs://snodemain:9000/user/nick/HadoopDWM/parmStage.txt#parms\
            -input HadoopDWM/job9_TCout-Mdata \
            -output HadoopDWM/job10_ClusterEval \
            -mapper $Identity_Mapper \
            -reducer HDWM070_CECR.py

        hdfs dfs -rm -r HadoopDWM/job10_tmpLinkIndex
        # JOB 10b: Check if a ref is already processed, add another tag as used
        hadoop jar $STREAMJAR \
            -files $(pwd)/HDWM075_TCRM.py,$(pwd)/HDWM075_TCRR.py \
            -input HadoopDWM/job10_ClusterEval \
            -output HadoopDWM/job10_tmpLinkIndex \
            -mapper HDWM075_TCRM.py \
            -reducer HDWM075_TCRR.py

        # Increase the program loop counter after each successful loop
        programCounter=$((programCounter+1))
        # Coping and Deleting
        hdfs dfs -rm -r HadoopDWM/progLoop_in  
        hdfs dfs -cp HadoopDWM/job10_tmpLinkIndex HadoopDWM/progLoop_in

        # Increase the values of Mu and Epsilon at the end of each iteration
        mu="$(awk 'BEGIN{ print '$mu'+'$muIter' }')"
        epsilon="$(awk 'BEGIN{ print '$epsilon'+'$epsilonIter' }')" 

    done    
####################################################
########## END OF PROGRAM LOOP #########
####################################################

    # JOB 11a: Create a Linked Index File
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM077_LKINM.py,$(pwd)/HDWM077_LKINR.py \
        -input HadoopDWM/job_LinkIndexDirty \
        -input HadoopDWM/job3_RecreateRefs \
        -output HadoopDWM/LinkedIndex_$inputFile \
        -mapper HDWM077_LKINM.py \
        -reducer HDWM077_LKINR.py

    # JOB 11b: Get Clusters and Sizes
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM080_CPM.py,$(pwd)/HDWM080_CPR.py \
        -input HadoopDWM/LinkedIndex_$inputFile \
        -output HadoopDWM/job_PreClusterProfile \
        -mapper HDWM080_CPM.py \
        -reducer HDWM080_CPR.py

    # JOB 11c: Generate Cluster Profile
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM080_CPRR.py \
        -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
        -Dstream.num.map.output.key.fields=2 \
        -D mapreduce.map.output.key.field.separator=, \
        -D mapreduce.partition.keycomparator.options="-k1,1n -k2,2" \
        -input HadoopDWM/job_PreClusterProfile \
        -output HadoopDWM/job11_ClusterProfile\
        -mapper $Identity_Mapper \
        -reducer HDWM080_CPRR.py

#--------->  PHASE 8: ER MATRIX PROCESS <---------
#    # Calculate Matrix of the ER Process
#    # Make sure to use 1 mapper, 1 reducer and should be executed on only the master node

    # JOB 12: Merge Truth Dataset and the outputs of Job 11
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM095_PERMM.py,$(pwd)/HDWM095_PERMR.py \
        -input HadoopDWM/$truthFile \
        -input HadoopDWM/LinkedIndex_$inputFile \
        -output HadoopDWM/job12_PreMatrix \
        -mapper HDWM095_PERMM.py \
        -reducer HDWM095_PERMR.py

    # JOB 13: Calculate E-pairs, L-pairs, TP-pairs, Precision, Recall, F-score
    hadoop jar $STREAMJAR \
        -files $(pwd)/HDWM099_ERMR.py \
        -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
        -Dstream.num.map.output.key.fields=2 \
        -D mapreduce.map.output.key.field.separator=, \
        -D mapreduce.partition.keycomparator.options="-k1,1 -k2,2n" \
        -input HadoopDWM/job12_PreMatrix \
        -output HadoopDWM/job13_ERmatrix \
        -mapper $Identity_Mapper \
        -reducer HDWM099_ERMR.py \
    
    echo "          " >> $Log_File
    echo "End of File $parmFile" >> $Log_File
    echo "End of Program" >> $Log_File 

    # Copy contents to a finalLogFile and Remove the tmpReporter file that was created at the start of the program
    sudo cp $Log_File $(pwd)/$finalLogFile
    #rm -r "$(pwd)/tmpReport.txt"
    #rm -r "$tmpDir/tmpReport.txt"
    sudo rm -r $tmpDir

    # Exiting program if the parameter file specified does not exists
    exit 0
fi
echo "The file, '$parmFile', is not a valid parameter file. Try again!" 
############################################################################################
################################### END OF DRIVER SCRIPT ###################################
############################################################################################

#### NOTES
# When using Identity Reducer
    #hadoop jar $STREAMJAR \
    #    -files $(pwd)/HDWM010_TM.py,$(pwd)/$(pwd)/parmStage.txt \
    #    -D mapreduce.job.reduces=1 \
    #    -input HadoopDWM/$inputFile \
    #    -output HadoopDWM/job1_out \
    #    -mapper HDWM010_TM.py \
    #    -reducer $Identity_Reducer

    # Note: The following used to belong for Cluster Eval
    #-D stream.map.input.field.separator=, \
    #-D stream.map.output.field.separator=, \
    #-D stream.reduce.input.field.separator=, \
    #-D mapreduce.map.output.key.field.separator=. \
    #-D stream.num.map.output.key.fields=2 \
    #-D mapreduce.reduce.output.key.field.separator=. \
    #-D stream.num.reduce.output.key.fields=2 \

#     #    -D mapreduce.job.reduces=1 \ This is removed

