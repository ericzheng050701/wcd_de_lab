#!/bin/bash
shopt -s xpg_echo 
##(cmd "shopt" set output format. -s enable, -u disable : enable expg_echo: $ echo "Hello\nworld" --> Hello\nworld; disable expg_echo: $ echo "Hello\nworld" --> Hello world)
# set xv 
## (run in debug mode)

##########################################################
# Part 2: SET DEFAUL VARIABLES
HOST=$HOSTNAME

SHORT_DATE=`date '+%Y-%m-%d'`

TIME=`date '+%H%M'`

SCRIPT_TYPE=`basename $0 | cut -d '.' -f1` ##(get the first line of the file )

filenametime1=$(date +"%m%d%Y%H%M%S")
filenametime2=$(date +"%Y-%m-%d %H:%M:%S")
#########################################################
# Part 2: SET VARIABLES 
export BASE_FOLDER='/home/ezheng/curriculum/linux_docker/'
export SCRIPTS_FOLDER='/home/ezheng/curriculum/linux_docker/scripts'
export INPUT_FOLDER='/home/ezheng/curriculum/linux_docker/input'
export OUT_FOLDER='/home/ezheng/curriculum/linux_docker/input'
export LOGDIR='/home/ezheng/curriculum/linux_docker/logs'
export SHELL_SCRIPT_NAME='shell_script'
export LOG_FILE=${LOGDIR}/${SHELL_SCRIPT_NAME}_${filenametime1}.log
#########################################################
# PART 3: GO TO SCRIPT FOLDER AND RUN
cd ${SCRIPTS_FOLDER}

#########################################################
# PART 4: SET LOG RULES

exec > >(tee ${LOG_FILE}) 2>&1

#########################################################
# PART 5: DOWNLOAD DATA
echo "Start download data"

for year in {2020..2022}; # or use (seq 2019 2022)
do wget -N --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=48549&Year=${year}&Month=2&Day=14&timeframe=1&submit= Download+Data" -O ${INPUT_FOLDER}/${year}.csv;
done;

RC1=$?
if [ ${RC1} != 0 ]; then
	echo "DOWNLOAD DATA FAILED"
	echo "[ERROR:] RETURN CODE:  ${RC1}"
	echo "[ERROR:] REFER TO THE LOG FOR THE REASON FOR THE FAILURE."
	exit 1
fi
###${RC1} = 0 means sucessful running the script
#########################################################
# PART 5: RUN PYTHON
echo "Start to run Python Script"
python3 ${SCRIPTS_FOLDER}/python_script.py


RC1=$?
if [ ${RC1} != 0 ]; then
	echo "PYTHON RUNNING FAILED"
	echo "[ERROR:] RETURN CODE:  ${RC1}"
	echo "[ERROR:] REFER TO THE LOG FOR THE REASON FOR THE FAILURE."
	exit 1
fi

echo "PROGRAM SUCCEEDED"

exit 0 
## Must have, to indicate exiting the program