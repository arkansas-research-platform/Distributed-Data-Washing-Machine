#!/bin/bash

# Delete HDFS Directory
#hdfs dfs -rm -r /user/nick/HadoopDWM/Spark-PDP

# Submit spark Application
spark-submit \
	--master local[4] \
	krisCalculator.py