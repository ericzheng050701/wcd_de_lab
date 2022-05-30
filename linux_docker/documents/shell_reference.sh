#!/bin/bash
. /home/precedm/.bash_profile
. /etc/profile
shopt -s xpg_echo
shopt -s expand_aliases

# set -xv
##############################################################
# 1. Default Variables

HOST=$HOSTNAME

SHORT_DATE=`date '+%Y-%m-%d'`

TIME=`date '+%H%M'`

SCRIPT_NAME=`basename $0 | cut -d '.' -f1`


##############################################################
# Product Variables

PRODUCT_USERNAME=`whoami`

##############################################################
######### DO NOT MODIFY ABOVE THIS LINE ######################

# Setting up environment variables

filenametime1=$(date +"%m%d%Y%H%M%S")
filenametime2=$(date +"%Y-%m-%d %H:%M:%S")

export BASE_PATH="/home/precedm/puma"
export SCRIPTS_FOLDER="/home/precedm/puma/scripts"
export ENV_ACTIVATION_LOCATION="/home/precedm/puma/venv/bin/activate"

export LOGDIR='/home/precedm/puma/logs'
export DOCDIR='/home/precedm/puma/docs'

export SUP_CLIENT='RAD'
export SUP_TASK='puma vendor api call'

export SCRIPT='rad_puma_vendor_api_call'
export LOG_FILE=${LOGDIR}/${SCRIPT}_${filenametime1}.log
export PYTHON_LOG_FILE=${LOGDIR}/${SCRIPT}_python_${filenametime1}.log


cd ${SCRIPTS_FOLDER}

# exec 2> ${LOG_FILE} 1>&2
exec > >(tee ${LOG_FILE})
exec 2> >(tee ${LOG_FILE} >&2)

##############################################################
## JOB START
echo "\nSTART"

echo "\n[SYSINFO]: EXECUTING ON SERVER: ${SHOST}\n---------------------------"

echo "[JOB]: ${SUP_CLIENT} ${SUP_TASK} PROCESS START AT $(date)\n---------------------------\n"

source ${ENV_ACTIVATION_LOCATION}

echo "[SNOWFLAKE]: FETCH vendor_source_table FROM puma_control_table"
export VENDOR_SOURCE_TABLE=$(snowsql -c precedm -o log_level=DEBUG  -o friendly=false -o header=false -o timing=false -o output_format=tsv -q "select variable_value from precedm.puma_control_table where script_name = 'rad_puma_vendor_api_call' and variable = 'vendor_source_table' and active=TRUE;")
echo "VENDOR_SOURCE_TABLE=$VENDOR_SOURCE_TABLE\n"

echo "[SNOWFLAKE]: FETCH client_vendor_num FROM puma_control_table"
export CLIENT_VENDOR_NUM=$(snowsql -c precedm -o log_level=DEBUG  -o friendly=false -o header=false -o timing=false -o output_format=tsv -q "select variable_value from precedm.puma_control_table where script_name = 'rad_puma_vendor_api_call' and variable = 'client_vendor_num' and active=TRUE;")
echo "CLIENT_VENDOR_NUM=$CLIENT_VENDOR_NUM\n"

echo "[SNOWFLAKE]: FETCH client_vendor_name FROM puma_control_table"
export CLIENT_VENDOR_NAME=$(snowsql -c precedm -o log_level=DEBUG  -o friendly=false -o header=false -o timing=false -o output_format=tsv -q "select variable_value from precedm.puma_control_table where script_name = 'rad_puma_vendor_api_call' and variable = 'client_vendor_name' and active=TRUE;")
echo "CLIENT_VENDOR_NAME=$CLIENT_VENDOR_NAME\n"

echo "[SNOWFLAKE]: FETCH vendor_api_put_url FROM puma_control_table"
export VENDOR_API_PUT_URL=$(snowsql -c precedm -o log_level=DEBUG  -o friendly=false -o header=false -o timing=false -o output_format=tsv -q "select variable_value from precedm.puma_control_table where variable = 'vendor_api_put_url' and active=TRUE;")
echo "VENDOR_API_PUT_URL=$VENDOR_API_PUT_URL\n"

echo "[SNOWFLAKE]: FETCH api_secret FROM puma_control_table"
export API_SECRET=$(snowsql -c precedm -o log_level=DEBUG  -o friendly=false -o header=false -o timing=false -o output_format=tsv -q "select variable_value from precedm.puma_control_table where variable = 'api_secret' and active=TRUE;")
echo "API_SECRET=$API_SECRET\n"


##############################################################
# Begin PYTHON SCRIPT

echo "[PROCESS]: STARTING RUN PYTHON SCRIPT '${SCRIPT}.py'.\n"
python3 ${SCRIPTS_FOLDER}/rad_puma_vendor_api_call.py

RC1=$?
if [ ${RC1} != 0 ]; then
	echo "\n[ERROR:] ERROR FOR SCRIPT ${SCRIPT}.py"
	echo "[ERROR:] RETURN CODE:  ${RC1}"
	echo "[ERROR:] REFER TO THE LOG FOR THE REASON FOR THE FAILURE."
	echo "[ERROR:] LOG FILE NAME: "${PYTHON_LOG_FILE}
	exit 1
fi

echo "\n[SUCESS]:SCRIPT ${SCRIPT}.py RUNNING SUCCEDED"
echo "[PROCESS]: END SCRIPT RUNNING PROCESS"

##ENDING PROCESS
echo "\n[JOB]: LOAD SESSION OF ${SUP_CLIENT} ${SUP_TASK} PROCESS COMPLETED SUCCESSFULLY."
echo "[JOB]: ${SUP_CLIENT} ${SUP_TASK} PROCESS END AT $(date)"
echo -e "\nEND"

exit 0
