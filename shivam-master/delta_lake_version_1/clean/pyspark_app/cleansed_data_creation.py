#!/usr/bin/python
# -*- coding: utf-8 -*-

#######################################Module Information########################################################
#  Module Name         : CleansedDataCreation                                               					#
#  Purpose             : Performing incremental logic on input data with glue catalog change					#
#  Input Parameters    : Input json/txt/csv files                                              					#
#  Output Value        : cleansed records in parquet format which are readable from athena without any downtime #
#################################################################################################################

import sys
import os
# script_dir = os.getcwd()
script_dir = '/home/hadoop/clean'
sys.path.insert(1, script_dir)

# Library and external modules declaration
from configs.cleansed_run_constants import CleansedRunConstants
from python.utils.send_sns_notification import SendSnsNotification
from python.utils.glue_catalog_handler import GlueCatalogHandler
from data_validation import DataValidation
from python.utils.audit_handler import AuditHandler
from editable_space import EditableSpace
from pyspark.sql.window import Window
from python.utils.dataset_info import DatasetInfo
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import subprocess


start_datetime = datetime.now()
start_ts_str = start_datetime.strftime("%Y%m%d%H%M%S")
curr_ts = start_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
sns_topic_arn = CleansedRunConstants.SNS_TOPIC_ARN

#year = start_datetime.strftime("%Y")
#month = start_datetime.strftime("%m")
#day = start_datetime.strftime("%d")
#hour = start_datetime.strftime("%H")
#minute = start_datetime.strftime("%M")

class CleansedDataCreation(object):
    """
        Class contains all the functions related to Cleansed process.
    """

    def s3_to_emr_read(self, dataset, job_key, raw_staging_path,
                       clean_output_location, glue_database, dataset_key, dataset_info_table, dataset_refresh_start_ts,
                       file_extension, is_cross_account, cross_account_role_arn):
        """
            This function is used for reading source and target path from s3.
        """
        try:
            print("####### - Reading input data from s3 for the cleansed process")

            # Reading raw_staging_path from Handler file and adding subdirectories path if any.
            raw_staging_location = raw_staging_path + CleansedRunConstants.SUB_DIRECTORIES_PATH
            custom_schema_flag = CleansedRunConstants.UPDATE_KEYS[dataset]['CUSTOM_SCHEMA_FLAG']
            delimiter = CleansedRunConstants.DELIMITER

            # Reading source data
            if custom_schema_flag:
                print("####### - Reading source data with provided custom schema")
                source_staging_load_df = self.get_source_df_with_custom_schema(raw_staging_location, file_extension,
                                                                               delimiter)
            else:
                print("####### - Reading source data without custom schema")
                source_staging_load_df = self.get_source_df(raw_staging_location, file_extension, delimiter)            
            
            source_staging_load_df.show()
            # Dropping duplicates from input data
            source_staging_load_df = source_staging_load_df.dropDuplicates()

            # Column Name changing to appropriate column names.
            mapping = CleansedRunConstants.COLUMN_MAPPING[dataset]
            source_staging_df = source_staging_load_df.select(
                [col(c).alias(mapping.get(c, c)) for c in source_staging_load_df.columns])

            timezone = CleansedRunConstants.DATA_TIMEZONE
            timezone_var = '_ts_' + timezone

            # Changing date column datatype from string to timestamp.
            for a_dftype in source_staging_df.dtypes:
                col_name = a_dftype[0]
                if col_name.lower().endswith(timezone_var):
                    source_staging_df = source_staging_df.withColumn(col_name, col(col_name).cast("timestamp"))

            # Changing the timestamp data to respective timezone data.
            # source_staging_df = DataValidation().change_timezone(spark, source_staging_df, timezone)

            # Calling editable access method if user want to add any extra column in raw data.
            editable_access = CleansedRunConstants.UPDATE_KEYS[dataset]['EDITABLE_ACCESS_FLAG']
            if editable_access:
                source_staging_df = EditableSpace().editable_snippet(spark, source_staging_df)

            source_records = source_staging_df.count()
            print("####### - Total number of records from source path : " + str(source_records))

            if CleansedRunConstants.UPDATE_KEYS[dataset]['SCD_TYPE'] == 'OVERWRITE':
                self.overwrite_run(source_staging_df, clean_output_location, dataset, job_key, source_records,
                                   glue_database, dataset_info_table, dataset_key, dataset_refresh_start_ts)
            else:
                resp = DatasetInfo().get_dataset(dataset_info_table, dataset_key, is_cross_account,
                                                 cross_account_role_arn)
                print("####### - Run mode retrieved from dataset_info dynamodb table is" + str(resp))
                run_mode = resp["Item"]["run_mode"]
                if run_mode == "full_load":
                    self.initial_run(source_staging_df, clean_output_location, dataset, job_key, source_records,
                                     glue_database, dataset_info_table, dataset_key, dataset_refresh_start_ts)

                elif run_mode == "delta_load":
                    self.incremental_run(source_staging_df, clean_output_location, dataset,
                                         job_key, source_records, glue_database, dataset_refresh_start_ts)

        except Exception as e:
            print("####### - Failed while reading s3 files. " + str(e))
            AuditHandler().update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                             cross_account_role_arn, source_records=0, target_records=0,
                                             job_status="FAILED", error_message=str(e))
            raise e

    def get_source_df(self, path, file_extension, delimiter):
        """
            This function is used for reading data when no custom schema provided
        """
        if file_extension == '.csv' or file_extension == '.txt':
            input_df = spark.read.csv(path, header=CleansedRunConstants.HEADER, sep=delimiter)
        elif file_extension == '.parquet':
            input_df = spark.read.parquet(path)
        elif file_extension == '.json':
            input_df = spark.read.json(path)
        else:
            print("####### - File Format Not Supported")
        return input_df

    def get_source_df_with_custom_schema(self, path, file_extension, delimiter):
        """
            This function is used for reading data on providing custom schema
        """
        schema = CleansedRunConstants.schema_dict
        for i, k in schema.items():
            if dataset == i:
                final_schema = k
        final_struc = StructType(fields=final_schema)

        if file_extension == '.csv' or file_extension == '.txt':
            input_df = spark.read.csv(path, header=CleansedRunConstants.HEADER, sep=delimiter, schema=final_struc)
        elif file_extension == '.parquet':
            input_df = spark.read.parquet(path, schema=final_struc)
        elif file_extension == '.json':
            input_df = spark.read.json(path, schema=final_struc)
        else:
            print("####### - File Format Not Supported")
        return input_df

    def initial_run(self, source_staging_df, clean_output_location, dataset, job_key, source_records, glue_database,
                    dataset_info_table, dataset_key, dataset_refresh_start_ts):
        """
            This function is being used when system runs the Full load for the first time
        """
        try:
            print("####### - Full load run started for dataset - " + dataset)

            #partition_key_list = CleansedRunConstants.UPDATE_KEYS[dataset]['PARTITION_KEY']
            scd_type = CleansedRunConstants.UPDATE_KEYS[dataset]['SCD_TYPE']
            print("######## SCD Type : ", scd_type)
            #table_name = dataset + "_cln"
            pk = CleansedRunConstants.UPDATE_KEYS[dataset]['PK_COLUMN']

            source_staging_df = source_staging_df.withColumn('updated_at', col('updated_at').cast('timestamp'))

            if scd_type == 'SCD1':
                source_staging_df = source_staging_df.withColumn("active", lit("true"))
                final_df = source_staging_df
                final_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").save(clean_output_location)

            elif scd_type == 'SCD2':

                loc = DatasetInfo().get_dataset(dataset_info_table, dataset_key, is_cross_account,
                                                cross_account_role_arn)
                data_location = loc["Item"]["data_location"]

                existing_delta_table = DeltaTable.forPath(spark, data_location)
                existing_df = existing_delta_table.toDF()
                existing_df = existing_df.withColumn('updated_at', col('updated_at').cast('timestamp'))

                new_records_to_insert = source_staging_df \
                    .alias("updates") \
                    .join(existing_df.alias("existing"), pk) \
                    .where("existing.active = true AND updates.updated_at <> existing.updated_at")

                staged_updates = new_records_to_insert.selectExpr("NULL as mergeKey", "updates.*").union(
                    source_staging_df.selectExpr(eval('pk') + " as mergeKey", "*"))

                existing_delta_table.alias("existing").merge(
                    staged_updates.alias("updates"), "existing." + eval('pk') + " = mergeKey") \
                    .whenMatchedUpdate(
                    condition="existing.active = true AND existing.updated_at <> updates.updated_at",
                    set={
                        "active": "false"
                    }
                ).whenNotMatchedInsertAll().execute()

                existing_delta_table = existing_delta_table.toDF().na.fill("true", ['active'])
                final_df = existing_delta_table
                final_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").save(
                    clean_output_location)

            else:
                raise Exception("####### - Please specify correct SCD mode(SCD1 / SCD2)")

            target_records = final_df.count()
            print("####### - Total number of output records : " + str(target_records))

            dataset_refresh_end_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

           # if query_result == 'SUCCEEDED':
            DatasetInfo().update_dataset(dataset_info_table, dataset_key, dataset_refresh_start_ts, dataset_refresh_end_ts,
                                         is_cross_account, cross_account_role_arn, sla="0",
                                         run_mode="delta_load", data_location=clean_output_location)

            # Updating the audit log table with success entry.
            AuditHandler().update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                             cross_account_role_arn, source_records,
                                             target_records, job_status="SUCCEEDED",
                                             error_message="null")
            #else:
             #   raise Exception("####### - Failed while preforming msck repair command")

        except Exception as e:
            print("####### - Failed while performing Initial load " + str(e))
            AuditHandler().update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                             cross_account_role_arn, source_records=0, target_records=0,
                                             job_status="FAILED", error_message=str(e))

            raise e

    def incremental_run(self, source_staging_df, clean_output_location, dataset, job_key,
                        source_records, glue_database, dataset_refresh_start_ts):
        """
            This function is used for every incremental transaction run
        """
        print("####### - Incremental load run started for dataset - " + dataset)
        try:
            scd_type = CleansedRunConstants.UPDATE_KEYS[dataset]['SCD_TYPE']

            table_name = dataset + "_cln"
            pk = CleansedRunConstants.UPDATE_KEYS[dataset]['PK_COLUMN']

            loc = DatasetInfo().get_dataset(dataset_info_table, dataset_key, is_cross_account,
                                            cross_account_role_arn)
            data_location = loc["Item"]["data_location"]

            existing_delta_table = DeltaTable.forPath(spark, data_location)
            existing_df = existing_delta_table.toDF()
            existing_df = existing_df.withColumn('updated_at', col('updated_at').cast('timestamp'))

            source_staging_df = source_staging_df.withColumn('updated_at', col('updated_at').cast('timestamp'))

            if scd_type == 'SCD1':
                print("####### - The clean process is running for SCD1")
                staged_data = source_staging_df
                staged_data = staged_data.withColumn("active", lit("true"))

                existing_delta_table.alias('target').merge(
                    staged_data.alias("stg"),
                    "target." + eval('pk') + " = stg." + eval('pk')) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()

            elif scd_type == 'SCD2':

                new_records_to_insert = source_staging_df \
                    .alias("updates") \
                    .join(existing_df.alias("existing"), pk) \
                    .where("existing.active = true AND updates.updated_at <> existing.updated_at")

                staged_updates = new_records_to_insert.selectExpr("NULL as mergeKey", "updates.*").union(
                    source_staging_df.selectExpr(eval('pk') + " as mergeKey", "*"))

                existing_delta_table.alias("existing").merge(
                    staged_updates.alias("updates"), "existing." + eval('pk') + " = mergeKey") \
                    .whenMatchedUpdate(
                    condition="existing.active = true AND existing.updated_at <> updates.updated_at",
                    set={
                        "active": "false"
                    }
                ).whenNotMatchedInsertAll().execute()

                final_df = existing_delta_table.toDF().na.fill("true", ['active'])
                final_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").save(
                    clean_output_location)

            else:
                raise Exception("####### - Please specify correct SCD mode(SCD1 / SCD2)")

            target_records = existing_delta_table.toDF().count()
            print("####### - Total number of output records : " + str(target_records))

            dataset_refresh_end_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            DatasetInfo().update_dataset(dataset_info_table, dataset_key, dataset_refresh_start_ts, dataset_refresh_end_ts,
                                         is_cross_account, cross_account_role_arn, sla="0",
                                         run_mode="delta_load", data_location=clean_output_location)

            # Adding success entry in audit table
            AuditHandler().update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                             cross_account_role_arn, source_records,
                                             target_records, job_status="SUCCEEDED",
                                             error_message="null")

        except Exception as e:
            print("####### - Failed while performing incremental load " + str(e))
            AuditHandler().update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                             cross_account_role_arn, source_records=0, target_records=0,
                                             job_status="FAILED", error_message=str(e))

            raise e

    def overwrite_run(self, source_staging_df, clean_output_location, dataset, job_key, source_records, glue_database,
                      dataset_info_table, dataset_key, dataset_refresh_start_ts):
        """
            This function is being used when system runs the Full load for the first time
        """
        try:
            print("####### - Overwrite load run started for dataset - " + dataset)

            # partition key
            partition_key_list = CleansedRunConstants.UPDATE_KEYS[dataset]['PARTITION_KEY']

            table_name = dataset + "_cln"

            target_records = source_staging_df.count()

            #source_staging_df.show()
            source_staging_df = source_staging_df.withColumn("active", lit("true"))
            source_staging_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").save(clean_output_location)

            
            dataset_refresh_end_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            DatasetInfo().update_dataset(dataset_info_table, dataset_key, dataset_refresh_start_ts, dataset_refresh_end_ts,
                                         is_cross_account, cross_account_role_arn, sla="0",
                                         run_mode="full_load", data_location=clean_output_location)

            # Updating the audit log table with success entry.
            AuditHandler().update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                             cross_account_role_arn, source_records,
                                             target_records, job_status="SUCCEEDED",
                                             error_message="null")
        except Exception as e:
            print("####### - Failed while performing Initial load " + str(e))
            AuditHandler().update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                         cross_account_role_arn, source_records=0, target_records=0,
                                         job_status="FAILED", error_message=str(e))

            raise e


if __name__ == "__main__":
    dataset = sys.argv[1]
    raw_staging_path = sys.argv[2]
    clean_output_location = sys.argv[3]
    glue_database = sys.argv[4]
    file_extension = sys.argv[5]

    dataset_refresh_start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if dataset is None or raw_staging_path is None or clean_output_location is None \
            or glue_database is None or file_extension is None:
        raise Exception("####### - All required argument are not provided for the process")

    job_key = CleansedRunConstants.AUDIT_PREFIX + "_" + dataset + "_" + start_ts_str
    job_name = CleansedRunConstants.AUDIT_PREFIX + "_" + dataset
    dynamoDB_table = CleansedRunConstants.DYNAMODB_TABLE
    source = CleansedRunConstants.SOURCE
    layer_name = 'Clean'
    job_run_date = datetime.now().strftime("%Y-%m-%d")

    dataset_info_table = CleansedRunConstants.DATASET_INFO_TABLE
    dataset_key = CleansedRunConstants.AUDIT_PREFIX + "_" + dataset
    is_cross_account = CleansedRunConstants.CROSS_ACCOUNT_FLAG
    cross_account_role_arn = CleansedRunConstants.CROSS_ACCOUNT_ROLE_ARN

    dataset_id = AuditHandler().get_dataset_dataset_id(dataset_info_table, dataset_key, False, "")
    audit_item_json = AuditHandler().audit_input_json_generator(dataset, start_datetime, job_key, dataset_id, job_name, source,
                                                                layer_name, job_run_date)

    response, temp_key = AuditHandler().insert_audit_info(dynamoDB_table, start_datetime, audit_item_json,
                                                          is_cross_account, cross_account_role_arn)

    # Spark session initialisation with application name
    applicationName = "CleanDataCreation for " + dataset
    spark = SparkSession.builder.appName(applicationName) \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.initialExecutors", "1") \
        .config("spark.dynamicAllocation.maxExecutors", CleansedRunConstants.MAX_EXECUTORS) \
        .config("spark.dynamicAllocation.minExecutors", CleansedRunConstants.MIN_EXECUTORS) \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.speculation", "true") \
        .config("spark.sql.codegen.wholeStage", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    from delta.tables import *

    #existing_table_schema_json = os.environ["existing_table_schema_json"]
    #existing_table_schema_json = json.loads(existing_table_schema_json, encoding='utf-8')
    print("####### - Starting the process for Cleansed run")
    cleansed_layer_run = CleansedDataCreation()
    cleansed = cleansed_layer_run.s3_to_emr_read(dataset, job_key, raw_staging_path,
                                                 clean_output_location, glue_database, dataset_key,
                                                 dataset_info_table, dataset_refresh_start_ts, file_extension, is_cross_account,
                                                 cross_account_role_arn)
