#!/bin/bash -e
######################################################################################
# Author: Sellbrite
# Create Date: 07/20/2020
# Purpose: shell script to trigger rtf renewals process
######################################################################################
#cd /home/hadoop/pydeequ
echo "starting pydeequ/run_checks pyspark script $(date '+%Y-%m-%dT%H%M%S')"
/usr/lib/spark/bin/spark-submit --packages com.amazon.deequ:deequ:1.0.5 --exclude-packages net.sourceforge.f2j:arpack_combined_all --master yarn s3://aws-poc-serverless-analytics/pydeequ/v2/pyspark_app/run_checks.py dev s3://aws-poc-serverless-analytics/pydeequ/v2/configs/checks.json s3://aws-poc-serverless-analytics/pydeequ/v2/results/
if [[ $? -eq 0 ]];
then
	echo "completed pydeequ/run_checks pyspark script $(date '+%Y-%m-%dT%H%M%S')"
else
	echo "pydeequ/run_checks pyspark script failed $(date '+%Y-%m-%dT%H%M%S')"
	exit 1
fi
