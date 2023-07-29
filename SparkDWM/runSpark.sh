#!/bin/bash

#### GETTING PARAMETER FILE FROM USER ####
read -p "Enter parameter file: " parmFile

# Go to a previous directory to check if file exists
cd ..

if [[ -f "$(pwd)/$parmFile" ]]
then

	# Get machine's hostname and Username
	host=$(hostname)
	username=$(whoami)
	user_home=$(eval echo ~$USER)

	# Log File for parameter summary
    Sum_Log="$(pwd)/parmSummary.txt"

    # Reading Parameter File locally for local reporting
    while IFS='=' read -r line val
    do
        if [[ "$line" = inputFileName* ]]
        then
		    echo "SPARK DATA WASHING MACHINE" > $Sum_Log
            echo "***********************************************" >> $Sum_Log
            inputFile="$val"
            # Copy Input file to a Stagging file
            cp $(pwd)/$inputFile $(pwd)/inputStage.txt
            echo "Input File to process      -->  $inputFile " >> $Sum_Log       
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
    done < "$(pwd)/$parmFile"

#========= HDFS SETTINGS =========
	# Delete HDFS Directory and make new one
	#hdfs dfs -rm -r /user/nick/SparkDWM
	#hdfs dfs -mkdir /user/nick/SparkDWM

    # Copy input data and truthSet from local directory to HDFS
    #hdfs dfs -put $(pwd)/inputStage.txt SparkDWM
    
    #if [[ "$truthFile" != "" ]]
    #then
    #    hdfs dfs -put $(pwd)/$truthFile SparkDWM
    #fi

    # Copy contents of the given parameter file to a staging area to be shipped to Distributed Cache
    cp $(pwd)/$parmFile $(pwd)/parmStage.txt
    #hdfs dfs -put $(pwd)/parmStage.txt SparkDWM

#====================================================	

#========= Submit spark Application =========
	# --master <local, standalone, or yarn> eg. local[4] (local mode with 4 cpu cores)
	# --deploy-mode < client or cluster >
	#--conf spark.eventLog.enabled=true \
	#--conf spark.eventLog.dir='/home/nick/Distributed-DWM/SDWM/tmp/spark-events' \
	spark-submit \
		--name 'Spark Data Washing Machine' \
		--master local[4] \
		--deploy-mode client \
		--py-files $(pwd)/SDWM/DWM10_Parms.py,$(pwd)/SDWM/DWM65_ScoringMatrixStd.py,$(pwd)/SDWM/DWM66_ScoringMatrixKris.py,$(pwd)/SDWM/StopWord.py,$(pwd)/SDWM/SDWM010_Tokenization.py,$(pwd)/SDWM/SDWM025_Blocking.py,$(pwd)/SDWM/SDWM050_SimilarityComparison.py \
		--files $(pwd)/parmStage.txt \
		$(pwd)/SDWM/SDWM00_Driver.py
    #$(pwd)/DWM-Modules.zip,

    # Exiting program if the parameter file specified does not exists
    exit 0
fi
echo "The file, '$parmFile', is not a valid parameter file. Try again!" 