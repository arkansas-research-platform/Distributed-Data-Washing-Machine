#Script to do the following:
#  1. Create a directory in HDFS
#  2. Transfer data from local directory to HDFS
#  3. Copy files from HFDFS to local directory

#--------------------------------------------------
# 1. Create HDFS directory
#hadoop fs -mkdir <Directory Name>


# 2. Transfer data file from local directory to HDFS
# hadoop fs -put <location to source in local dir> <HDFS destination location>
#hadoop fs -put HDWM/S10PX.txt HDWM
#hadoop fs -put HDWM/S11PX.txt HDWM
#hadoop fs -put HDWM/S12PX.txt HDWM
#hadoop fs -put HDWM/S13GX.txt HDWM
#hadoop fs -put HDWM/S14GX.txt HDWM
#hadoop fs -put HDWM/S15GX.txt HDWM
hadoop fs -put HDWM/S16PX.txt HDWM
#hadoop fs -put HDWM/S17PX.txt HDWM
#hadoop fs -put HDWM/S18PX.txt HDWM
#hadoop fs -put HDWM/S2G.txt HDWM
#hadoop fs -put HDWM/S3Rest.txt HDWM
#hadoop fs -put HDWM/S4G.txt HDWM
#hadoop fs -put HDWM/S5G.txt HDWM
#hadoop fs -put HDWM/S6GeCo.txt HDWM
#hadoop fs -put HDWM/S7GX.txt HDWM
#hadoop fs -put HDWM/S9P.txt HDWM
