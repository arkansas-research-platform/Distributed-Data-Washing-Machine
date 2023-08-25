#!/bin/bash

#### GETTING PARAMETER FILE FROM USER ####
#read -p "Enter parameter file: " parmVar
#-files hdfs://$host:9000/user/$username/$(pwd)/$parameter_file 

# Go to a previous directory to check if file exists
cd ..

#if [[ -f "$(pwd)/$parmVar" ]]
#then

# Creating logfile using current date and time
startTime=$( date '+%F_%H:%M:%S' )
#export Log_File="$(pwd)/HDWM_log_$startTime.txt"

# Get machine's hostname and Username
host=$(hostname)
username=$(whoami)
user_home=$(eval echo ~$USER)

# Zipping customized modules from legacy DWM
if [[ -f "$(pwd)/DWM-Modules.zip" ]]
then
    echo "Custom Modules zip file already exists"
else
    # Zip customized modules to be used by MapReduce
    echo "Zipping custom modules..."
    cd DWM-Modules  && zip -r ../DWM-Modules.zip ./ && cd ../
fi

# Create a Tmp Directory locally to report logs, only if the directory doesn't exist
if [ ! -d "$user_home/JobLog" ]
    then
        sudo mkdir -m777 "$user_home/JobLog"
        echo "Local Logging Directory Successfully Created in '$user_home'."
else
    sudo rm -r $user_home/JobLog/*
    echo "Local Logging Directory Found in '$user_home'."
fi

    # Create a tmp logging directory 
    tmpDir="$user_home/JobLog" 

    # Final Log file to give to user
    Log_File="$(pwd)/HDWM_Log_$startTime.txt"

# Log File for parameter summary
Sum_Log="$(pwd)/parmSummary.txt"

# For Kris - Create a file to collect Statistics for PDP 
PDP_File="$(pwd)/PDP-Statistics/PDP_Collector.txt"
echo ">>> Starting PDP Statistics Collection for $parmVar" > $PDP_File

# Create tmp file for local reporting
touch "$tmpDir/tmpReport.txt"

# Create logfile for transitive closure loop
touch "$tmpDir/reportTCiteration.txt" && echo "9999" > $tmpDir/reportTCiteration.txt

# Create a file to log mu and epsilon value inside
touch "$tmpDir/muReport.txt" && touch "$tmpDir/epsilonReport.txt"

# Reading Parameter File locally for local reporting
while IFS='=' read -r line val
do
    if [[ "$line" = inputFileName* ]]
    then
        echo "HADOOP DATA WASHING MACHINE" >> $Log_File
        echo "***********************************************" > $Log_File
        echo "***********************************************" > $Sum_Log
        #echo "         Summary of Parameter Settings         " >> $Log_File
        #echo "         -----------------------------         " >> $Log_File
        inputFile="$val"
        # Copy Input file to a Stagging file
        cp $(pwd)/$inputFile $(pwd)/inputStage.txt
        echo "Input File to process      -->  $inputFile " >> $Sum_Log  
        echo "Input File to process      -->  $inputFile " >> $Log_File       
        continue
    elif [[ "$line" = hasHeader* ]]
    then
        header="$val"
        # Eliminae Header from original data if it's present
        if [[ $header = "True" ]]
        then
            sed -i '1d' $(pwd)/inputStage.txt
        fi
        echo "File has Header            -->  $header" >> $Sum_Log         
        continue
    elif [[ "$line" = delimiter* ]]
    then
        delimiter="$val"
        echo "Delimeter                  -->  '$delimiter' " >> $Sum_Log
        continue
    elif [[ "$line" = tokenizerType* ]]
    then
        tokenizer="$val"
        echo "Tokenizer type             -->  $tokenizer" >> $Sum_Log     
        continue
    elif [[ "$line" = truthFileName* ]]
    then
        truthFile="$val"
        echo "TruthSet File              -->  $truthFile" >> $Sum_Log    
        continue 
    elif [[ "$line" = beta* ]]
    then
        beta="$val"
        echo "Beta                       -->  $beta" >> $Sum_Log        
        continue 
    elif [[ "$line" = blockByPairs* ]]
    then
        blockByPairs="$val"
        echo "Block by Pairs             -->  $blockByPairs" >> $Sum_Log 
        continue 
    elif [[ "$line" = minBlkTokenLen* ]]
    then
        minBlkTokenLen="$val"
        echo "Min. Blocking Token Length -->  $minBlkTokenLen" >> $Sum_Log  
        continue 
    elif [[ "$line" = excludeNumericBlocks* ]]
    then
        excludeNumTok="$val"
        echo "Exclude Num. Block Tokens  -->  $excludeNumTok" >> $Sum_Log  
        continue 
    elif [[ "$line" = sigma* ]]
    then
        sigma="$val"
        echo "Sigma                      -->  $sigma" >> $Sum_Log        
        continue 
    elif [[ "$line" = removeDuplicateTokens* ]]
    then
        deDupTokens="$val"
        echo "Remove Dup. Ref. Tokens    -->  $deDupTokens" >> $Sum_Log  
        continue
    elif [[ "$line" = removeExcludedBlkTokens* ]]
    then
        removeExcBlkTok="$val"
        echo "Remove Excl. Block Tokens  -->  $removeExcBlkTok" >> $Sum_Log  
        continue
    elif [[ "$line" = comparator* ]]
    then
        comparator="$val"
        echo "Matrix Comparator          -->  $comparator" >> $Sum_Log   
        continue
    elif [[ "$line" = mu ]]
    then
        export mu="$val"
        echo "Mu                         -->  $mu" >> $Sum_Log           
        continue
    elif [[ "$line" = muIterate ]]
    then
        muIter="$val"
        echo "Mu Iterate                 -->  $muIter" >> $Sum_Log           
        continue
    elif [[ "$line" = epsilon ]]
    then
        export epsilon="$val"
        echo "Epsilon                    -->  $epsilon" >> $Sum_Log           
        continue
    elif [[ "$line" = epsilonIterate ]]
    then
        epsilonIter="$val"
        echo "Epsilon Iterate            -->  $epsilonIter" >> $Sum_Log 
        echo "***********************************************" >> $Sum_Log           
        continue
    fi
done < "$(pwd)/$parmVar"

######## BEGINNING OF HADOOP JOBS #########
# Create HDFS directory
#hdfs dfs -mkdir <Directory Name>
# Once a job is started, a directory is automatically created in HDFS
hdfs dfs -rm -r /HadoopDWM   
hdfs dfs -mkdir /HadoopDWM

# Copy input data and truthSet from local directory to HDFS
hdfs dfs -put $(pwd)/inputStage.txt /HadoopDWM
    
if [[ "$truthFile" != "" ]]
then
    hdfs dfs -put $(pwd)/$truthFile /HadoopDWM
fi

# Copy contents of the given parameter file to a staging area to be shipped to Distributed Cache
cp $(pwd)/$parmVar $(pwd)/parmStage.txt
#hdfs dfs -put $(pwd)/parmStage.txt /HadoopDWM
    
# Create some variables to be reused. These are just paths to important repetitive JAR Libraries
Identity_Mapper=/bin/cat
Identity_Reducer=org.apache.hadoop.mapred.lib.IdentityReducer
streamLocation=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar
STREAMJAR=$streamLocation

    #$(pwd)/parmStage.txt
    #################################################
    # START EXECUTION OF HadoopDWM JOBS
    #################################################
#--------->  PHASE 1: TOKENIZATION & FREQUENCY CALCULATION PROCESS <---------
#---> JOB 1: Tokenize each row of Ref and form Metadata and Calculate Frequency of Tokens 
#        one Mapper & one Reducer....Outputs keys and their frequencies
echo "        "
echo ">> Starting Tokenization Process"
echo "        " >> $Log_File
echo ">> Starting Tokenization Process" >> $Log_File
hadoop jar $STREAMJAR \
    -files $(pwd)/HadoopDWM/HDWM010_TokenizationMapper.py,$(pwd)/HadoopDWM/HDWM010_TokenizationReducer.py,$(pwd)/parmStage.txt,$(pwd)/DWM-Modules.zip \
    -input /HadoopDWM/inputStage.txt \
    -output /HadoopDWM/job1_Tokens-Freq \
    -mapper HDWM010_TokenizationMapper.py \
    -reducer HDWM010_TokenizationReducer.py

echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
# Analyzing JOB 1 Counters for useful Statistics
# Phase 1: Getting the Job Counter Logs
mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
tokJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
mapred job -history $tokJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
refsRead=$( grep 'Map input records' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
toksFound=$( grep 'Tokens Found' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
tFound=$(echo "$toksFound"  | tr -d ",") #This removes all ',' in between numbers
numToks=$( grep 'Numeric Tokens' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
remainToks=$( grep 'Remaining Tokens' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
tRemain=$(echo "$remainToks"  | tr -d ",") #This removes all ',' in between numbers
dupToks=$(( $tFound-$tRemain ))
uniqueToks=$( grep 'Unique Tokens' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J1maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J1reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J1mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J1redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
# Phase 2: Logging to logfile
echo "  ----- Job Statistics ----- " >> $Log_File
echo "   Total References Read: $refsRead" >> $Log_File
echo "References Read: $refsRead" >> $PDP_File
echo "   Total Tokens Found: $toksFound" >> $Log_File
echo "Tokens Found: $toksFound" >> $PDP_File
echo "   Total Numeric Tokens: $numToks" >> $Log_File 
echo "Numeric Tokens: $numToks" >> $PDP_File 
echo "   Duplicate Tokens: $dupToks" >> $Log_File 
echo "   Remaining Tokens: $remainToks" >> $Log_File 
echo "   Unique Tokens: $uniqueToks" >> $Log_File 
echo "Unique Tokens: $uniqueToks" >> $PDP_File 
#echo "  ----- MapReduce Statistics ----- " >> $Log_File
#echo "   Total Map tasks: $J1maps" >> $Log_File
#echo "   Total Reduce tasks: $J1reds" >> $Log_File
#echo "   Total time taken by all map tasks (ms): $J1mapTime" >> $Log_File
#echo "   Total time taken by all reduce tasks (ms): $J1redTime" >> $Log_File 
    
#---> JOB 2: Update the Metadata information with Calculated Freq (Joining outputs of Job 1 & Job 2)
#        one Mapper & one Reducer....Outputs keys and their frequency
echo "        "
echo ">> Starting Metadata with Frequency Update Process"
#echo "        " >> $Log_File 
#echo ">> Starting Metadata with Frequency Update  Process" >> $Log_File 
hadoop jar $STREAMJAR \
    -files $(pwd)/HadoopDWM/HDWM015_JoinMapper.py,$(pwd)/HadoopDWM/HDWM015_JoinReducer.py \
    -input /HadoopDWM/job1_Tokens-Freq \
    -output /HadoopDWM/job2_Updated-Mdata \
    -mapper HDWM015_JoinMapper.py \
    -reducer HDWM015_JoinReducer.py

echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
# Phase 1: Getting the Job Counter Logs
mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
frqJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
mapred job -history $frqJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
J2maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J2reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J2mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J2redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
# Phase 2: Logging to logfile
#echo "  ----- MapReduce Statistics ----- " >> $Log_File
#echo "   Total Map tasks: $J2maps" >> $Log_File
#echo "   Total Reduce tasks: $J2reds" >> $Log_File
#echo "   Total time taken by all map tasks (ms): $J2mapTime" >> $Log_File
#echo "   Total time taken by all reduce tasks (ms): $J2redTime" >> $Log_File 

#--------->  PHASE 2: REFERENCE RECREATION PROCESS <---------
#---> JOB 3: Pre-Blocking Full References 
#        one Mapper & one Reducer....Outputs rebuilt references for each refID
echo "        "
echo ">> Starting Reference Reformation Process"
#echo "        " >> $Log_File
#echo ">> Starting Reference Reformation Process" >> $Log_File
hadoop jar $STREAMJAR \
    -files $(pwd)/HadoopDWM/HDWM020_RefRecreationMapper.py,$(pwd)/HadoopDWM/HDWM020_RefRecreationReducer.py \
    -input /HadoopDWM/job2_Updated-Mdata\
    -output /HadoopDWM/job3_RecreateRefs \
    -mapper HDWM020_RefRecreationMapper.py \
    -reducer HDWM020_RefRecreationReducer.py

echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
# Phase 1: Getting the Job Counter Logs
mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
recJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
mapred job -history $recJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
J3maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J3reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J3mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J3redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
# Phase 2: Logging to logfile
#echo "  ----- MapReduce Statistics ----- " >> $Log_File
#echo "   Total Map tasks: $J3maps" >> $Log_File
#echo "   Total Reduce tasks: $J3reds" >> $Log_File
#echo "   Total time taken by all map tasks (ms): $J3mapTime" >> $Log_File
#echo "   Total time taken by all reduce tasks (ms): $J3redTime" >> $Log_File 

# Copy job3_RecreateRefs output to a tmp file
hdfs dfs -cp /HadoopDWM/job3_RecreateRefs /HadoopDWM/progLoop_in

####################################################
########## STARTING PROGRAM ITERATIVE LOOP #########
####################################################
echo "        "
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
    echo "        "
    echo ">>>>> STARTING NEXT ITERATION at" $mu " Mu >>>>>"
    echo "        " >> $Log_File
    echo ">>>>> STARTING NEXT ITERATION >>>>>" >> $Log_File
    echo ">>>>> STARTING NEXT ITERATION >>>>>" >> $PDP_File
    echo "   New Mu --> " $mu >> $Log_File
    echo "   New Epsilon --> " $epsilon >> $Log_File
    echo "   New Mu --> " $mu >> $PDP_File
    echo "   New Epsilon --> " $epsilon >> $PDP_File

    if [[ "$mu" > 1 ]]
    then
        echo "--- Ending because Mu > 1"
        echo "--- Ending because Mu > 1" >> $Log_File
        echo "--- END OF PROGRAM LOOP"
        echo "        " >> $Log_File
        echo "+++++++++ END OF PROGRAM LOOP WITH  [ $programCounter ] ITERATION(S) +++++++++" >> $Log_File
        # Copy this job's output to a file ready to be processed for final LinkedIndex
        hdfs dfs -cp /HadoopDWM/progLoop_in /HadoopDWM/job_LinkIndexDirty
    break
    fi
    
#--------->  PHASE 4: BLOCKING PROCESS <---------
#---> JOB 4: Extract all Blocking Tokens, and Create of Blocking refID Pairs
    #        one Mapper & one Reducer....Outputs pairs of refIDs to be compared
    echo "        "
    echo ">> Starting Blocking Process"
    echo "        " >> $Log_File
    echo ">> Starting Blocking Process" >> $Log_File
    hdfs dfs -rm -r /HadoopDWM/job4_BlockTokens
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM025_BlockTokenPairMapper.py,$(pwd)/HadoopDWM/HDWM025_BlockTokenPairReducer.py,$(pwd)/parmStage.txt,$(pwd)/DWM-Modules.zip \
        -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
        -Dstream.num.map.output.key.fields=2 \
        -D mapreduce.map.output.key.field.separator=, \
        -Dstream.num.reduce.output.key.fields=2 \
        -D mapreduce.reduce.output.key.field.separator=, \
        -D stream.map.input.field.separator=: \
        -D stream.map.output.field.separator=: \
        -D stream.reduce.input.field.separator=: \
        -D stream.reduce.output.field.separator=: \
        -input /HadoopDWM/progLoop_in \
        -output /HadoopDWM/job4_BlockTokens \
        -mapper HDWM025_BlockTokenPairMapper.py \
        -reducer HDWM025_BlockTokenPairReducer.py
    
    # Analyzing JOB 4 (Blocking) Counters for useful Statistics
        # Phase 1: Getting the Job Counter Logs
    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    blkJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $blkJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    selectRefs=$( grep 'Refs for Reprocessing' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    excludeRefs=$( grep 'Excluded References' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 ) 
    remainRefs=$( grep 'Remaining References' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    blckRefCreate=$( grep 'Blocking References Created' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    pairsGen=$( grep 'Pairs Created-by-Blocks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    pairGenCnt=$(echo "$pairsGen"  | tr -d ",") #This removes all ',' in between numbers
    J4maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J4reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J4mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J4redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

    # Phase 2: Logging to logfile
    echo "  ----- Job Statistics ----- " >> $Log_File
    echo "   Total References Selected for Reprocessing: $selectRefs" >> $Log_File 
    echo "   Total Record Excluded: : $excludeRefs" >> $Log_File
    echo "   Total Record Left for Blocks Creation: $remainRefs" >> $Log_File 
    echo "   Total Blocking Records Created: $blckRefCreate" >> $Log_File 
    echo "   Total Pairs Generated by Blocks: $pairsGen" >> $Log_File 
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J3maps" >> $Log_File
    #echo "   Total Reduce tasks: $J3reds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J3mapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J3redTime" >> $Log_File 

    # Phase 3: Check if Block Pair List is empty
    #blkPairListCheck=$(cat $tmpDir/tmpReport.txt)
    if [ "$pairGenCnt" -eq "0" ]
    then
        echo "--- Ending because Block Pair List is empty"
        echo "--- Ending because Block Pair List is empty" >> $Log_File
        echo "--- END OF PROGRAM LOOP"
        echo "        " >> $Log_File
        echo "+++++++++ END OF PROGRAM LOOP WITH  [ $programCounter ] ITERATION(S) +++++++++" >> $Log_File
        # Copy this job's output to a file ready to be processed for final LinkedIndex
        hdfs dfs -cp /HadoopDWM/job4_BlockTokens /HadoopDWM/job_LinkIndexDirty
    break
    fi
        
#---> JOB 5: Block Deduplication 
    #        Identity Mapper & one Reducer....Outputs pairs of refIDs to be compared
    echo "        "
    echo ">> Starting Blocking Key Deduplication Process"
    #echo "        " >> $Log_File
    #echo ">> Starting Blocking Key Deduplication Process" >> $Log_File
    hdfs dfs -rm -r /HadoopDWM/job5_BlockDedup
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM030_RefPairDedupReducer.py \
        -input /HadoopDWM/job4_BlockTokens \
        -output /HadoopDWM/job5_BlockDedup \
        -mapper $Identity_Mapper \
        -reducer HDWM030_RefPairDedupReducer.py
        
    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    # Phase 1: Getting the Job Counter Logs
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    dJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $dJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    J5maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J5reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J5mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J5redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    # Phase 2: Logging to logfile
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J5maps" >> $Log_File
    #echo "   Total Reduce tasks: $J5reds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J5mapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J5redTime" >> $Log_File 

#--> JOB 6a: Merge and Join BlockPairs with Original References to update BlockPairsRefID with full Metadata Information
    #        One Mapper, One Reducer |sort| Another Reducer . Takes and merge two inputs (job4 output, job7 output)
    echo "        "
    echo ">> Starting Block & Reformed Refs Join Process"
    #echo "        " >> $Log_File 
    #echo ">> Starting Block & Reformed Refs Join Process" >> $Log_File 
    hdfs dfs -rm -r /HadoopDWM/job6_tmp_out
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM035_RefIDMetadataMapper.py,$(pwd)/HadoopDWM/HDWM035_RefIDMetadataReducer.py \
        -Dstream.num.map.output.key.fields=2 \
        -input /HadoopDWM/job3_RecreateRefs \
        -input /HadoopDWM/job5_BlockDedup \
        -output /HadoopDWM/job6_tmp_out  \
        -mapper HDWM035_RefIDMetadataMapper.py \
        -reducer HDWM035_RefIDMetadataReducer.py

    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    # Phase 1: Getting the Job Counter Logs
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    jJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $jJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    J6amaps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J6areds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J6amapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J6aredTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    # Phase 2: Logging to logfile
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J6amaps" >> $Log_File
    #echo "   Total Reduce tasks: $J6areds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J6amapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J6aredTime" >> $Log_File 

#---> Job 6b: Final unduplicated Block Pairs
    echo "        "
    echo ">> Starting Unduplicated Blocking Ref Pairs  Process"
    #echo "        " >> $Log_File 
    #echo ">> Starting Unduplicated Blocking Ref Pairs  Process" >> $Log_File 
    hdfs dfs -rm -r /HadoopDWM/job6_UndupBlockPairs
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM036_UnduplicateRefsReducer.py \
        -input /HadoopDWM/job6_tmp_out \
        -output /HadoopDWM/job6_UndupBlockPairs \
        -mapper $Identity_Mapper \
        -reducer HDWM036_UnduplicateRefsReducer.py

    # Analyzing JOB 6b (Unduplicated Block Pairs) Counters for useful Statistics
        # Phase 1: Getting the Job Counter Logs
    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    undupJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $undupJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    undupBlck=$( grep 'Unduplicated Block Pairs' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J6bmaps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J6breds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J6bmapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J6bredTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

    # Phase 2: Logging to logfile
    #echo "  ----- Job Statistics ----- " >> $Log_File
    echo "   Total Unduplicated Blocks: $undupBlck" >> $Log_File 
    echo "TotalMuCnts(Unduplicated Blocks): $undupBlck" >> $PDP_File 
    # Phase 2: Logging to logfile
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J6bmaps" >> $Log_File
    #echo "   Total Reduce tasks: $J6breds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J6bmapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J6bredTime" >> $Log_File 

    #--------->  PHASE 5: SIMILARITY COMPARISON & LINKING PROCESS <---------
#---> JOB 7: Linked Pairs
        #        Identity Mapper & one Reducer....Outputs Linked Pairs
    echo "        "
    echo ">> Starting Similarity Comparison Process"
    echo "        " >> $Log_File
    echo ">> Starting Similarity Comparison Process" >> $Log_File
    hdfs dfs -rm -r /HadoopDWM/job7_LinkedPairs
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM050_SimilarityComparisonReducer.py,$(pwd)/parmStage.txt,$tmpDir/muReport.txt,$(pwd)/DWM-Modules.zip \
        -input /HadoopDWM/job6_UndupBlockPairs \
        -output /HadoopDWM/job7_LinkedPairs \
        -mapper $Identity_Mapper \
        -reducer HDWM050_SimilarityComparisonReducer.py

    # Analyzing JOB 7 Counters for useful Statistics
        # Phase 1: Getting the Job Counter Logs
    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    lnkJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $lnkJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    linkPairs=$( grep 'Linked Pairs' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    lnkPairCnt=$(echo "$linkPairs"  | tr -d ",") #This removes all ',' in between numbers
    J7maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J7reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J7mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J7redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

    # Phase 2: Logging to logfile
    echo "  ----- Job Statistics ----- " >> $Log_File
    echo "   Number of Pairs Linked: $linkPairs" >> $Log_File
    echo "Number of Pairs Linked(greaterThanMu): $linkPairs" >> $PDP_File
    # Phase 2: Logging to logfile
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J7maps" >> $Log_File
    #echo "   Total Reduce tasks: $J7reds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J7mapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J7redTime" >> $Log_File 

    # Phase 3: Check if Linked Pair List is empty
    #linkPairListCheck=$(cat $tmpDir/tmpReport.txt)
    if [ "$lnkPairCnt" -eq "0" ]
    then
        echo "--- Ending because Link Pair List is empty"
        echo "--- Ending because Link Pair List is empty" >> $Log_File
        echo "--- END OF PROGRAM LOOP"
        echo "        " >> $Log_File
        echo "+++++++++ END OF PROGRAM LOOP WITH  [ $programCounter ] ITERATION(S) +++++++++" >> $Log_File
        # Copy this job's output to a file ready to be processed for final LinkedIndex
        hdfs dfs -cp /HadoopDWM/job7_LinkedPairs /HadoopDWM/job_LinkIndexDirty
    break
    fi

    #--------->  PHASE 6: TRANSITIVE CLOSURE PROCESS <---------
#---> JOB 8: Transitive Closure Iteration
    # It finds all the connected components until no more merge state
    echo "        "
    echo ">> Starting Transitive Closure Process"
    echo "        " >> $Log_File
    echo ">> Starting Transitive Closure Process" >> $Log_File       
    # Move job 7 output into a temp_in directory which will serve as input for TC 
    hdfs dfs -rm -r /HadoopDWM/job8_tmpIn
    hdfs dfs -cp /HadoopDWM/job7_LinkedPairs /HadoopDWM/job8_tmpIn
    iterationCounter=0
    while true
    do
        count=$(cat $tmpDir/reportTCiteration.txt)
        msCnt=$(echo "$count"  | tr -d ",")
        echo "**** Current RunNextIteration Counter is:---->>>> $msCnt"
        if [ "$msCnt" != "0" ]
        then
            hdfs dfs -rm -r /HadoopDWM/job8_tmpOut
            hadoop jar $STREAMJAR \
                -files $(pwd)/HadoopDWM/HDWM055_TransitiveClosureCCMR.py,$(pwd)/parmStage.txt \
                -D stream.map.output.field.separator=, \
                -D stream.num.map.output.key.fields=2 \
                -input /HadoopDWM/job8_tmpIn \
                -output /HadoopDWM/job8_tmpOut \
                -mapper $Identity_Mapper \
                -reducer HDWM055_TransitiveClosureCCMR.py
            hdfs dfs -rm -r /HadoopDWM/job8_tmpIn    
            hdfs dfs -mv /HadoopDWM/job8_tmpOut /HadoopDWM/job8_tmpIn

            # Analyzing JOB 8 Counters for useful Statistics (Merge-State Statistics)
                # Phase 1: Getting the Job Counter Logs
            echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
            mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
            mStateJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
            mapred job -history $mStateJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
            mergeState=$( grep 'Merge State' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
            # Update count to be used in next iteration
            echo $mergeState > $tmpDir/reportTCiteration.txt

            # Update Transitive Closure Loop Counter
            iterationCounter=$((iterationCounter+1))
        # Stop TC Loop if count of last job is 0 and rest count for next Program iteration
        else
            # Analyzing JOB 8 Counters for useful Statistics
                # Phase 1: Getting the Job Counter Logs
            echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
            mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
            locMaxStateJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
            mapred job -history $locMaxStateJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
            clusterListCheck=$( grep 'Cluster List' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
            clusterListCnt=$(echo "$clusterListCheck"  | tr -d ",") #This removes all ',' in between numbers
            J8maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
            J8reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
            J8mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
            J8redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    
                # Phase 2: Logging to logfile
            echo "  ----- Job Statistics ----- " >> $Log_File
            echo "   Total Transitive Closure Iterations: $iterationCounter" >> $Log_File
            echo "   Size of Cluster List Formed from TC: $clusterListCheck" >> $Log_File
            #echo "  ----- MapReduce Statistics ----- " >> $Log_File
            #echo "   Total Map tasks: $J8maps" >> $Log_File
            #echo "   Total Reduce tasks: $J8reds" >> $Log_File
            #echo "   Total time taken by all map tasks (ms): $J8mapTime" >> $Log_File
            #echo "   Total time taken by all reduce tasks (ms): $J8redTime" >> $Log_File 

            # Update reportTCiteration file for future job
            echo "9999" > $tmpDir/reportTCiteration.txt

            # Phase 3: Check if Cluster List is empty
            if [[ "$clusterListCnt" == "0" ]]
            then
                echo "--- Ending because Cluster List is empty"
                echo "--- Ending because Cluster List is empty" >> $Log_File
                echo "--- END OF PROGRAM LOOP"
                echo "        " >> $Log_File
                echo "+++++++++ END OF PROGRAM LOOP WITH  [ $programCounter ] ITERATION(S) +++++++++" >> $Log_File
                # Copy this job's output to a file ready to be processed for final LinkedIndex
                hdfs dfs -cp /HadoopDWM/job8_tmpIn /HadoopDWM/job_LinkIndexDirty
            break
            fi
        break
        fi
    done  

    #--------->  PHASE 7: CLUSTER EVALUATION PROCESS <---------
#---> JOB 9: Update RefIDs in Clusters with their token metadata
    #         by using output from Transitive Closure and original dataset
    echo "        "
    echo ">> Starting Update RefID with Metadata Process"
    #echo "        " >> $Log_File
    #echo ">> Starting Update RefID with Metadata Process" >> $Log_File
    hdfs dfs -rm -r /HadoopDWM/job9_TCout-Mdata
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM060_ClusterRemainRefMapper.py,$(pwd)/HadoopDWM/HDWM060_ClusterRemainRefReducer.py \
        -input /HadoopDWM/job8_tmpIn \
        -input /HadoopDWM/job3_RecreateRefs \
        -output /HadoopDWM/job9_TCout-Mdata  \
        -mapper HDWM060_ClusterRemainRefMapper.py \
        -reducer HDWM060_ClusterRemainRefReducer.py

    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    # Phase 1: Getting the Job Counter Logs
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    refMJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $refMJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    J9maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J9reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J9mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J9redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    # Phase 2: Logging to logfile
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J9maps" >> $Log_File
    #echo "   Total Reduce tasks: $J9reds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J9mapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J9redTime" >> $Log_File 

#---> JOB 10a: Calculate Entropy and Differentiate Good and Bad Clusters
    echo "        "
    echo ">> Starting Cluster Evaluation Process"
    echo "        " >> $Log_File
    echo ">> Starting Cluster Evaluation Process" >> $Log_File  
    hdfs dfs -rm -r /HadoopDWM/job10_ClusterEval
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM070_ClusterEvaluationReducer.py,$tmpDir/epsilonReport.txt \
        -input /HadoopDWM/job9_TCout-Mdata \
        -output /HadoopDWM/job10_ClusterEval \
        -mapper $Identity_Mapper \
        -reducer HDWM070_ClusterEvaluationReducer.py

    # Analyzing JOB 10a (Cluster Evaluation) Counters for useful Statistics
        # Phase 1: Getting the Job Counter Logs
    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    clusEvalJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $clusEvalJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    clusProcessed=$( grep 'Total Clusters Processed' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    refsInClus=$( grep 'Total References in Clusters' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 ) 
    cluSizeThanOne=$( grep 'Cluster Size Greater than 1' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    goodClus=$( grep 'Total Good Clusters' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    refsInGood=$( grep 'References in Good Clusters' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J10amaps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J10areds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J10amapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J10aredTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

    # Phase 2: Logging to logfile
    echo "  ----- Job Statistics ----- " >> $Log_File
    echo "   Total Clusters Processed: $clusProcessed" >> $Log_File 
    echo "   Total References in Clusters: : $refsInClus" >> $Log_File
    echo "   Number of Cluster > 1: $cluSizeThanOne" >> $Log_File 
    echo "   Total Good Cluster: $goodClus at epsilon, $epsilon" >> $Log_File 
    echo "   Total References in Good Cluster: $refsInGood" >> $Log_File 
    echo "GreaterThanEpsilon(RefsInGoodCluster): $refsInGood" >> $PDP_File 
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J10amaps" >> $Log_File
    #echo "   Total Reduce tasks: $J10areds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J10amapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J10aredTime" >> $Log_File 

#---> JOB 10b: Check if a ref is already processed, add another tag as used
    echo "        "
    echo ">> Starting Reference Tagging Process"
    #echo "        " >> $Log_File 
    #echo ">> Starting Reference Tagging Process" >> $Log_File 
    hdfs dfs -rm -r /HadoopDWM/job10_TaggedRefs
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM075_TagClusterRefsMapper.py,$(pwd)/HadoopDWM/HDWM075_TagClusterRefsReducer.py \
        -input /HadoopDWM/job10_ClusterEval \
        -output /HadoopDWM/job10_TaggedRefs \
        -mapper HDWM075_TagClusterRefsMapper.py \
        -reducer HDWM075_TagClusterRefsReducer.py

    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    # Phase 1: Getting the Job Counter Logs
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    rTagJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $rTagJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    J10bmaps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J10breds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J10bmapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J10bredTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    # Phase 2: Logging to logfile
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J10bmaps" >> $Log_File
    #echo "   Total Reduce tasks: $J10breds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J10bmapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J10bredTime" >> $Log_File 

    # Increase the program loop counter after each successful loop
    programCounter=$((programCounter+1))
    # Coping and Deleting
    hdfs dfs -rm -r /HadoopDWM/progLoop_in  
    hdfs dfs -cp /HadoopDWM/job10_TaggedRefs /HadoopDWM/progLoop_in

    # Increase the values of Mu and Epsilon at the end of each iteration
    mu="$(awk 'BEGIN{ print '$mu'+'$muIter' }')"
    epsilon="$(awk 'BEGIN{ print '$epsilon'+'$epsilonIter' }')" 

done    
####################################################
########## END OF PROGRAM LOOP #########
####################################################

#---> JOB 11a: Create a Linked Index File
echo "        "
echo ">> Starting Write-To-LinkIndex Process"
echo "        " >> $Log_File
echo ">> Starting Write-To-LinkIndex Process" >> $Log_File  
hadoop jar $STREAMJAR \
    -files $(pwd)/HadoopDWM/HDWM077_LinkedIndexMapper.py,$(pwd)/HadoopDWM/HDWM077_LinkedIndexReducer.py \
    -input /HadoopDWM/job_LinkIndexDirty \
    -input /HadoopDWM/job3_RecreateRefs \
    -output /HadoopDWM/LinkedIndex_$inputFile \
    -mapper HDWM077_LinkedIndexMapper.py \
    -reducer HDWM077_LinkedIndexReducer.py

# Log LinkedIndex to a File
# Linked Index file to give to user
#LnkIndexFile="$(pwd)/HDWM_LinkedIndex_$inputFile"
    
#ind=$(hdfs dfs -cat /HadoopDWM/LinkedIndex_$inputFile/part-*)
#echo "$ind" >> $LnkIndexFile

# Analyzing JOB 11a (Link Index Process) Counters for useful Statistics
    # Phase 1: Getting the Job Counter Logs
echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
lnkIndJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
mapred job -history $lnkIndJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
indRefs=$( grep 'Reduce output records' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
indRefCnt=$(echo "$indRefs"  | tr -d ",") #This removes all ',' in between numbers
lnkIndRefs=$(( $indRefCnt-1 ))
J11amaps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11areds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11amapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11aredTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

# Phase 2: Logging to logfile
echo "  ----- Job Statistics ----- " >> $Log_File
echo "   Total Records Written to Linked Index File: $lnkIndRefs" >> $Log_File 
#echo "  ----- MapReduce Statistics ----- " >> $Log_File
#echo "   Total Map tasks: $J11abmaps" >> $Log_File
#echo "   Total Reduce tasks: $J11areds" >> $Log_File
#echo "   Total time taken by all map tasks (ms): $J11amapTime" >> $Log_File
#echo "   Total time taken by all reduce tasks (ms): $J11aredTime" >> $Log_File 

#---> JOB 11b: Get Clusters and Sizes
echo "        "
echo ">> Starting Pre-Cluster Profile Process"
#echo "        " >> $Log_File 
#echo ">> Starting Pre-Cluster Profile Process" >> $Log_File 
hadoop jar $STREAMJAR \
    -files $(pwd)/HadoopDWM/HDWM080_PreClusterProfileMapper.py,$(pwd)/HadoopDWM/HDWM080_PreClusterProfileReducer.py \
    -input /HadoopDWM/LinkedIndex_$inputFile \
    -output /HadoopDWM/job_PreClusterProfile \
    -mapper HDWM080_PreClusterProfileMapper.py \
    -reducer HDWM080_PreClusterProfileReducer.py

    # Phase 1: Getting the Job Counter Logs
echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
pcpIndJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
mapred job -history $pcpIndJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
J11bmaps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11breds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11bmapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11bredTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

# Phase 2: Logging to logfile
#echo "  ----- MapReduce Statistics ----- " >> $Log_File
#echo "   Total Map tasks: $J11bmaps" >> $Log_File
#echo "   Total Reduce tasks: $J11breds" >> $Log_File
#echo "   Total time taken by all map tasks (ms): $J11bmapTime" >> $Log_File
#echo "   Total time taken by all reduce tasks (ms): $J11bredTime" >> $Log_File 

#---> JOB 11c: Generate Cluster Profile
echo "        "
echo ">> Starting Cluster Profile Process"
echo "        " >> $Log_File 
echo ">> Starting Cluster Profile Process" >> $Log_File 
hadoop jar $STREAMJAR \
    -files $(pwd)/HadoopDWM/HDWM081_ClusterProfileReducer.py \
    -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -Dstream.num.map.output.key.fields=2 \
    -D mapreduce.map.output.key.field.separator=, \
    -D mapreduce.partition.keycomparator.options="-k1,1n -k2,2" \
    -input /HadoopDWM/job_PreClusterProfile \
    -output /HadoopDWM/job11_ClusterProfile \
    -mapper $Identity_Mapper \
    -reducer HDWM081_ClusterProfileReducer.py

# Log Cluster Profile to LogFile
profile=$(hdfs dfs -cat /HadoopDWM/job11_ClusterProfile/part-*)
echo "$profile" >> $Log_File

    # Phase 1: Getting the Job Counter Logs
echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
cpIndJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
mapred job -history $cpIndJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
J11cmaps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11creds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11cmapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
J11credTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

# Phase 2: Logging to logfile
#echo "       " >> $Log_File
#echo "  ----- MapReduce Statistics ----- " >> $Log_File
#echo "   Total Map tasks: $J11cmaps" >> $Log_File
#echo "   Total Reduce tasks: $J11creds" >> $Log_File
#echo "   Total time taken by all map tasks (ms): $J11cmapTime" >> $Log_File
#echo "   Total time taken by all reduce tasks (ms): $J11credTime" >> $Log_File 

#--------->  PHASE 8: ER MATRIX PROCESS <---------
#    # Calculate Matrix of the ER Process. Only execute these scripts if the truthFile is available
#    # Make sure to use 1 mapper, 1 reducer and should be executed on only the master node
    
if [[ "$truthFile" != "" ]] 
then
#---> JOB 12: Merge Truth Dataset and the outputs of Job 11
    echo "        "
    echo ">> Starting Pre-ER Matrix Process"
    #echo "        " >> $Log_File
    #echo ">> Starting Pre-ER Matrix Process" >> $Log_File
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM095_PreERMatrixMapper.py,$(pwd)/HadoopDWM/HDWM095_PreERMatrixReducer.py \
        -input /HadoopDWM/$truthFile \
        -input /HadoopDWM/LinkedIndex_$inputFile \
        -output /HadoopDWM/job12_PreMatrix \
        -mapper HDWM095_PreERMatrixMapper.py \
        -reducer HDWM095_PreERMatrixReducer.py

        # Phase 1: Getting the Job Counter Logs
    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    permIndJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $permIndJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    J12maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J12reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J12mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J12redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

    # Phase 2: Logging to logfile
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J12maps" >> $Log_File
    #echo "   Total Reduce tasks: $J12reds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J12mapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J12redTime" >> $Log_File 

#---> JOB 13: Calculate E-pairs, L-pairs, TP-pairs, Precision, Recall, F-score
    echo "        "
    echo ">> Starting ER Matrix Process"
    echo "        " >> $Log_File
    echo ">> Starting ER Matrix Process" >> $Log_File
    hadoop jar $STREAMJAR \
        -files $(pwd)/HadoopDWM/HDWM099_ERMatrixReducer.py \
        -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
        -Dstream.num.map.output.key.fields=2 \
        -D mapreduce.map.output.key.field.separator=, \
        -D mapreduce.partition.keycomparator.options="-k1,1 -k2,2n" \
        -input /HadoopDWM/job12_PreMatrix \
        -output /HadoopDWM/job13_ERmatrix \
        -mapper $Identity_Mapper \
        -reducer HDWM099_ERMatrixReducer.py \

    # Log ER Matrix to LogFile
    matrix=$(hdfs dfs -cat /HadoopDWM/job13_ERmatrix/part-*)
    echo "$matrix" >> $Log_File
    echo "$matrix" >> $PDP_File

        # Phase 1: Getting the Job Counter Logs
    echo ">> HANG ON, CALCULATING JOB STATISTICS FOR LOGFILE ..."
    mapred job -list all > $user_home/JobLog/yarn-appIDs.txt # Get a list of all Yarn Application IDs up till now   
    ermIndJobID=$( cat $user_home/JobLog/yarn-appIDs.txt |sort -n| head -n -2 | tail -n 1 | cut -f1 ) # Extract the application ID from the last line in the list
    mapred job -history $ermIndJobID > $user_home/JobLog/yarn-appLogs.txt  # Get the job history counter
    J13maps=$( grep 'Launched map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J13reds=$( grep 'Launched reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J13mapTime=$( grep 'Total time spent by all map tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )
    J13redTime=$( grep 'Total time spent by all reduce tasks' $user_home/JobLog/yarn-appLogs.txt | cut -d'|' -f6 )

    # Phase 2: Logging to logfile
    #echo "          " >> $Log_File
    #echo "  ----- MapReduce Statistics ----- " >> $Log_File
    #echo "   Total Map tasks: $J13maps" >> $Log_File
    #echo "   Total Reduce tasks: $J13reds" >> $Log_File
    #echo "   Total time taken by all map tasks (ms): $J13mapTime" >> $Log_File
    #echo "   Total time taken by all reduce tasks (ms): $J13redTime" >> $Log_File 
fi

echo "          " >> $Log_File
echo "End of File $parmVar" >> $Log_File
echo "End of Program" >> $Log_File 

# Copy contents to a finalLogFile and Remove the tmpReporter file that was created at the start of the program
#sudo cp $PDP_File $(pwd)/PDP-Statistics
#sudo rm -r $tmpDir
    
#    # Exiting program if the parameter file specified does not exists
#    exit 0
#fi
#echo "The file, '$parmVar', is not a valid parameter file. Try again!" 
############################################################################################
################################### END OF DRIVER SCRIPT ###################################
############################################################################################