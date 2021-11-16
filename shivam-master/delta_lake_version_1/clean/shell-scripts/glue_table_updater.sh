#!/bin/bash

CODE_HOME_DIR="/home/hadoop/clean"
PYSPARK_CODE_DIR="$CODE_HOME_DIR""/pyspark_app"
#PYTHON_CODE_DIR="$CODE_HOME_DIR""/python"
SHELL_CODE_DIR="$CODE_HOME_DIR""/shell-scripts"

while getopts d:g: option
do
case $option in
d)
DATASET_NAME=$OPTARG
;;
g)
GLUE_DATABASE=$OPTARG
;;
esac
done

temp_json_file=`cat "$SHELL_CODE_DIR"/${DATASET_NAME}_temp_glue.json`
#temp_json_file=`cat /home/hadoop/contract_temp_glue.json`
echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Updating the Table schema with new location"

aws glue update-table --database-name "${GLUE_DATABASE}" --table-input "$temp_json_file"
if [[ $? -eq 0 ]];
		then
			echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Glue table location updation completed for : "$DATASET_NAME
		else
			echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Glue table location updation failed for : "$DATASET_NAME
			exit 1
fi
