#!/bin/bash

# ----------------> Getting system paths of packages <----------------
env |grep SPARK_HOME > sysrequirement.txt
env |grep HADOOP_HOME >> sysrequirement.txt
hadoop_ver=$(hadoop version | grep 'Hadoop' | awk  '{print $2}') 
textDis=$(pip show textdistance | grep 'Location' | awk  '{print $2}')
echo 'textDisDir='$textDis >> sysrequirement.txt
pyVer=$(python3 -V | awk  '{print $2}' | cut -d '.' -f 1-2)
py=$(which python)
echo 'pyPath='#!/usr/bin/env $py$pyVer >> sysrequirement.txt

# Read the SysRequirements.txt file and update all .py scripts and Driver script with needed paths
while IFS='=' read -r line val
do 
    # Edit the driver script with Hadoop Streaming jar file location
    if [[ "$line" = HADOOP_HOME ]]
    then
        streamDir=${val}/share/hadoop/tools/lib/hadoop-streaming-${hadoop_ver}.jar
        ##sed -i "s|streamLocation=.*|streamLocation=$streamDir|g" $(pwd)/HadoopDWM/HDWM00_Driver.sh
        #echo $streamDir
        continue
    fi

    # Edit the Similarity Comparison script with TextDistance library location
    if [[ "$line" = textDisDir ]]
    then
        textDisLocation="$val"
        ##sed -i "s|textdistanceDir=.*|textdistanceDir='$textDisLocation'|g" $(pwd)/HadoopDWM/HDWM*SimilarityComparison*.py
        #echo $textDisLocation
        continue
    fi

    # Edit all .py scripts with python path in the Sheband
    if [[ "$line" = pyPath ]]
    then
        pythonPath="$val"
        # Edit driver script with pyPath
        ##sed -i "s|#!/usr/bin/env .*|$pythonPath|g" $(pwd)/HadoopDWM/HDWM*.py
        #echo $pythonPath
        continue
    fi
done < "$(pwd)/sysrequirement.txt"

# ----------------> GETTING PARAMETER FILE FROM USER <----------------
read -p "Select processing framework.. [1]->PySpark, [2]->MapReduce: " option

# -----> Settings for PySpark Option
if [[ "$option" -eq 1 ]]
then
    sysOption=PySpark
    echo "You opted to use, $sysOption, for data processing."

    # Confirm user's choice before proceesing
    read -p "Proceed with this choice? [y/n] " -n 1 -r
    echo    # (optional) move to a new line
    if [[ ! $REPLY =~ ^[Yy]$ ]]
    then
        [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
    fi

    # Get a Parms File name
    read -p "Enter parameter file: " parmFile

    if [[ -f "$(pwd)/$parmFile" ]]
    then
        # Update .py files and driver file with system requirements
        echo "Sending system requirements to program..."
            # Adding all transfers here
        echo "System requirements successfully transferred to program..."

        # Execute the HadoopDWM Driver Script
        echo "Executing '$sysOption Data Washing Machine' Driver Script ..."
        ./script.sh
        #cd SparkDWM && ./runSparkDWM.sh

        # Exiting program if the parameter file specified does not exists
        exit 0
    fi
    echo "The file, '$parmFile', is not a valid parameter file. Try again!" 

# -----> Settings for MapReduce Option
elif [[ "$option" -eq 2 ]]
then
    sysOption=MapReduce

    echo "You opted to use, $sysOption, for data processing."

    # Confirm user's choice before proceesing
    read -p "Proceed with this choice? " -n 1 -r
    echo    # (optional) move to a new line
    if [[ ! $REPLY =~ ^[Yy]$ ]]
    then
        [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
    fi

    # Get a Parms File name
    read -p "Enter parameter file: " parmFile

    if [[ -f "$(pwd)/$parmFile" ]]
    then
        parmVar="$parmFile"
        export parmVar
        # Update .py files and driver file with system requirements
        echo "Sending system requirements to program..."
        sed -i "s|streamLocation=.*|streamLocation=$streamDir|g" $(pwd)/HadoopDWM/runHadoopDWM.sh
        sed -i "s|textdistanceDir=.*|textdistanceDir='$textDisLocation'|g" $(pwd)/HadoopDWM/HDWM*SimilarityComparison*.py
        sed -i "s|#!/usr/bin/env .*|$pythonPath|g" $(pwd)/HadoopDWM/HDWM*.py
        echo "System requirements successfully transferred to program..."

        # Execute the HadoopDWM Driver Script
        echo "Executing '$sysOption Data Washing Machine' Driver Script ..."
        #./script.sh #testing script
        cd HadoopDWM && ./runHadoopDWM.sh

        # Exiting program if the parameter file specified does not exists
        exit 0
    fi
    echo "The file, '$parmFile', is not a valid parameter file. Try again!" 


# -----> Exiting Program if neither PySpark nor MapReduce was selected by user
else
    # Exiting program if the system processing option is unknown 
    echo "The option, '$option', is unknown. Try again!"
    exit 0
fi



##grep 'usr/bin/env*' $(pwd)/HadoopDWM/deb-*.py | xargs sed -i 's|usr/bin/env*|usr/bin/env $pythonPath|g'
##xargs sed -i 's/somematchstring/somereplacestring/g'
##sed -i '1d' $(pwd)/inputStage.txt
##sed -n -e '|POP3_SERVER_NAME| s|.*\= */|p' test.dat


# ------ Asking User for choice confirmation
    #Solution 1
    #echo "Do you wish to install this program?"
    #select yn in "Yes" "No"; do
    #  case $yn in
    #    Yes ) make install;;
    #    No ) exit;;
    #  esac
    #done

    #Solution 2
    #while true; do
    #read -p "Do you want to proceed? (y/n) " yn
    #case $yn in 
    #	[yY] ) echo ok, we will proceed;
    #		break;;
    #	[nN] ) echo exiting...;
    #		exit;;
    #	* ) echo invalid response;;
    #esac
    #done
    #echo doing stuff...

#read -p "Are you sure? " -n 1 -r
#echo    # (optional) move to a new line
#if [[ ! $REPLY =~ ^[Yy]$ ]]
#then
#    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
#fi