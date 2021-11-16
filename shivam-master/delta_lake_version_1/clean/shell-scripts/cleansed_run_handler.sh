#!/bin/bash

#######################################Script Information############################################################
#  Script Name         : CleansedRunHandler                                                                       	#
#  Purpose             : Performing copy from raw to raw_staging to avail the input for CleansedDataCreation module	#
#                        and maintaining the glue catalog using Glue API Call                           		       	#
#  Input Parameters    : Input json files                                                   				            		#
#  Output Value        : creating the backup of input json files in raw_archive directory on s3.                    #
#####################################################################################################################

CODE_HOME_DIR="/home/hadoop/clean"
PYSPARK_CODE_DIR=${CODE_HOME_DIR}"/pyspark_app"
#PYTHON_CODE_DIR=${CODE_HOME_DIR}"/python/utils"
#SHELL_CODE_DIR=${CODE_HOME_DIR}"/shell-scripts"

while getopts d:b:r:f:g:e: option
do
case $option in
d)
DATASET_NAME=$OPTARG
;;
b)
S3_BUCKET_NAME=$OPTARG
;;
r)
RAW_PATH_INPUT=$OPTARG
;;
f)
INPUT_FILE_FORMAT=$OPTARG
;;
g)
GLUE_DATABASE=$OPTARG
;;
e)
ENV=$OPTARG
;;
esac
done

GLUE_TABLENAME=${DATASET_NAME}'_cln'

RAW_PATH=${ENV}'/flattened_raw/'${RAW_PATH_INPUT}
RAW_STAGING_PATH=${ENV}'/flattened_raw_staging/'${RAW_PATH_INPUT}
RAW_ARCHIVE_PATH=${ENV}'/flattened_raw_archive/'${RAW_PATH_INPUT}

# Path for source files
SOURCE_FILE_PATH='s3://'${S3_BUCKET_NAME}'/'${RAW_STAGING_PATH}

# Cleansed path is created from input of dataset for maintaining the standard folder structure for cleansed layer process.
CLEANSED_OUTPUT_LOCATION='s3://'${S3_BUCKET_NAME}'/'${ENV}'/clean/'${DATASET_NAME}'_cln'

echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Raw to cleansed layer process started for dataset : "$DATASET_NAME

echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Checking if new files present in flattened_raw folder for file format ::- "$INPUT_FILE_FORMAT
file_exist_check=`aws s3 ls s3://${S3_BUCKET_NAME}/${RAW_PATH}/ --recursive | grep ${INPUT_FILE_FORMAT}| wc -c`

# Checking if file is present in raw folder or not. If not then no need to run cleanse layer process.
if [[ "${file_exist_check}" -ne 0 ]];
then
	echo `date "+%Y-%m-%d %H:%M:%S"` " :::: New files present in raw folder"

	#raw_archive_count=`aws s3 ls s3://${S3_BUCKET_NAME}/${RAW_ARCHIVE_PATH}/ --recursive | grep "json"| wc -c`

	# Call runModeDecider.py and get resp from dynamodb table name
	export run_mode=`/usr/lib/spark/bin/spark-submit --master yarn ${PYSPARK_CODE_DIR}/run_mode_decider.py ${DATASET_NAME}`
	echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Run mode retrieved from dataset_info dynamodb table is : "$run_mode

	if [[ "${run_mode}" == "full_load" ]];
	then
	  # Remove Cleansed files from s3
	  aws s3 rm ${CLEANSED_OUTPUT_LOCATION}  --recursive

	  # This is first run. So we will process full load for dataset
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Full load clean layer process starting for dataset : " $DATASET_NAME

	  # Move raw files to raw_staging for processing
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Moving file from raw folder to raw_staging for processing."
	  aws s3 mv s3://${S3_BUCKET_NAME}/${RAW_PATH}/ s3://${S3_BUCKET_NAME}/${RAW_STAGING_PATH}/ --recursive

	  #Providing the empty dummy schema for first run.
	  #export existing_table_schema_json="{}"

	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Clean Layer Spark Process starting for dataset : "$DATASET_NAME
	  /usr/lib/spark/bin/spark-submit --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.databricks.delta.schema.autoMerge.enabled=true" --master yarn ${PYSPARK_CODE_DIR}/cleansed_data_creation.py ${DATASET_NAME} ${SOURCE_FILE_PATH} ${CLEANSED_OUTPUT_LOCATION} ${GLUE_DATABASE} ${INPUT_FILE_FORMAT}
	  # Check if spark program run successfully or not.
	  if [[ $? -eq 0 ]];
	  then
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Clean Layer Spark Process successfully completed for full load for dataset : "$DATASET_NAME
	  else
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Clean Layer Spark Process failed"
		#If spark program fails, we will put files back at raw from raw_staging for next run.
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Moving files back to raw from raw_staging for next run"
		aws s3 mv s3://${S3_BUCKET_NAME}/${RAW_STAGING_PATH}/ s3://${S3_BUCKET_NAME}/${RAW_PATH}/  --recursive
		exit 1
	  fi

	  # After successfully completion of Spark program,move raw_staging files to raw archive which is the backup location of each file
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Moving files from raw_staging to raw_archive folder for backup"
		aws s3 cp s3://${S3_BUCKET_NAME}/${RAW_STAGING_PATH}/ s3://${S3_BUCKET_NAME}/${RAW_ARCHIVE_PATH}/ --recursive
	  if [[ $? -eq 0 ]];
	  then
		aws s3 rm s3://${S3_BUCKET_NAME}/${RAW_STAGING_PATH}/  --recursive
	  else
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Moving of files is failed from raw_staging to raw_archive"
		exit 1
	  fi
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Full load process successfully completed for dataset : "$DATASET_NAME

	elif [[ "${run_mode}" == "delta_load" ]];
	then
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Incremental load clean layer process starting for dataset : " $DATASET_NAME

	  #Move raw files to raw_staging for processing
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Moving file from raw folder to raw_staging for processing."
	  aws s3 mv s3://${S3_BUCKET_NAME}/${RAW_PATH}/ s3://${S3_BUCKET_NAME}/${RAW_STAGING_PATH}/ --recursive

	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Getting the table schema from existimg glue table for dataset : " $DATASET_NAME
	  #export existing_table_schema_json=`aws glue get-table --database-name "${GLUE_DATABASE}" --name "${GLUE_TABLENAME}"`

	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Clean Layer Spark Process starting for dataset : "$DATASET_NAME
	  /usr/lib/spark/bin/spark-submit --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.databricks.delta.schema.autoMerge.enabled=true" --master yarn ${PYSPARK_CODE_DIR}/cleansed_data_creation.py ${DATASET_NAME} ${SOURCE_FILE_PATH} ${CLEANSED_OUTPUT_LOCATION} ${GLUE_DATABASE} ${INPUT_FILE_FORMAT}
	  if [[ $? -eq 0 ]];
	  then
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Clean Layer Spark Process successfully completed for incremental load for dataset : "$DATASET_NAME
	  else
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Clean Layer Spark Process failed"
		#If spark program fails, we will put files back at raw from raw_staging for next run.
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Moving files back to raw from raw_staging for next run"
		aws s3 mv s3://${S3_BUCKET_NAME}/${RAW_STAGING_PATH}/ s3://${S3_BUCKET_NAME}/${RAW_PATH}/  --recursive
		exit 1
	  fi

		# After successfully completion of Spark program,Move raw_staging files to raw archive which is the backup location of each file
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Moving files from raw_staging to raw_archive folder for backup"
	  aws s3 cp s3://${S3_BUCKET_NAME}/${RAW_STAGING_PATH}/ s3://${S3_BUCKET_NAME}/${RAW_ARCHIVE_PATH}/ --recursive
	  if [[ $? -eq 0 ]];
	  then
		aws s3 rm s3://${S3_BUCKET_NAME}/${RAW_STAGING_PATH}/  --recursive
	  else
		echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Moving of files is failed from raw_staging to raw_archive"
		exit 1
	  fi

	  # Delete the temporary glue json file from hadoop location
	  #rm /home/hadoop/${DATASET_NAME}_temp_glue.json
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Incremental load process successfully completed for dataset : "$DATASET_NAME

	else
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Failed while fetching run mode from dataset_info table"
	  echo `date "+%Y-%m-%d %H:%M:%S"` " :::: Error : "$run_mode
	  exit 1
	fi

else
	echo `date "+%Y-%m-%d %H:%M:%S"` " :::: flattened_raw folder is empty. So Clean Layer process will not trigger for dataset : "$DATASET_NAME

	# Adding sleep time to handle airflow error. Otherwise airflow task will fail in a second.
	sleep 10s
fi
