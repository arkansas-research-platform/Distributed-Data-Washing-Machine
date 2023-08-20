#!/bin/bash

# Getting system paths of packages
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
        sed -i "s|streamLocation=.*|streamLocation=$streamDir|g" $(pwd)/HadoopDWM/HDWM00_Driver.sh
        #echo $streamDir
        continue
    fi

    # Edit the Similarity Comparison script with TextDistance library location
    if [[ "$line" = textDisDir ]]
    then
        textDisLocation="$val"
        sed -i "s|textdistanceDir=.*|textdistanceDir='$textDisLocation'|g" $(pwd)/HadoopDWM/HDWM*SimilarityComparison*.py
        #echo $textDisLocation
        continue
    fi

    # Edit all .py scripts with python path in the Sheband
    if [[ "$line" = pyPath ]]
    then
        pythonPath="$val"
        # Edit driver script with pyPath
        sed -i "s|#!/usr/bin/env .*|$pythonPath|g" $(pwd)/HadoopDWM/HDWM*.py
        #echo $pythonPath
        continue
    fi
done < "$(pwd)/sysrequirement.txt"

#grep 'usr/bin/env*' $(pwd)/HadoopDWM/deb-*.py | xargs sed -i 's|usr/bin/env*|usr/bin/env $pythonPath|g'
#xargs sed -i 's/somematchstring/somereplacestring/g'
#sed -i '1d' $(pwd)/inputStage.txt
#sed -n -e '|POP3_SERVER_NAME| s|.*\= */|p' test.dat