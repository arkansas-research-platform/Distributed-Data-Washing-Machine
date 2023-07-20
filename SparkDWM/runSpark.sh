#!/bin/bash

# Get machine's hostname and Username
host=$(hostname)
username=$(whoami)
user_home=$(eval echo ~$USER)


# Delete HDFS Directory and make new one
#hdfs dfs -rm -r /user/nick/SparkDWM
#hdfs dfs -mkdir /user/nick/SparkDWM

# Copy input data and truthSet from local directory to HDFS
#hdfs dfs -put S8P-copy.txt SparkDWM

#hdfs dfs -put $(pwd)/inputStage.txt HadoopDWM

# Submit spark Application
# --master <local, standalone, or yarn> eg. local[4] (local mode with 4 cpu cores)
# --deploy-mode < client or cluster >
spark-submit \
	--name 'Spark Data Washing Machine' \
	--master local[4] \
	--deploy-mode client \
	--py-files DWM-Modules.zip,SDWM010_Tokenization.py \
	--files S8P-parms-copy.txt \
	SDWM00_Driver.py