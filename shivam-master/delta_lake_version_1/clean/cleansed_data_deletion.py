#!/usr/bin/python
# -*- coding: utf-8 -*-

#######################################Module Information########################################################
#  Module Name         : CleansedDataCreation                                               					#
#  Purpose             : Performing incremental logic on input data with glue catalog change					#
#  Input Parameters    : Input json/txt/csv files                                              					#
#  Output Value        : cleansed records in parquet format which are readable from athena without any downtime #
#################################################################################################################


# Library and external modules declaration
from cleansed_run_constants import CleansedRunConstants
from send_sns_notification import SendSnsNotification
# from glue_catalog_handler import GlueCatalogHandler
from data_validation import DataValidation
from audit_handler import AuditHandler
from editable_space import EditableSpace
from pyspark.sql.window import Window
from dataset_info import DatasetInfo
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json, sys, os
import subprocess

start_datetime = datetime.now()
start_ts_str = start_datetime.strftime("%Y%m%d%H%M%S")
curr_ts = start_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
sns_topic_arn = CleansedRunConstants.SNS_TOPIC_ARN

# year = start_datetime.strftime("%Y")
# month = start_datetime.strftime("%m")
# day = start_datetime.strftime("%d")
# hour = start_datetime.strftime("%H")
# minute = start_datetime.strftime("%M")


class CleansedDataCreation(object):
    """
        Class contains all the functions related to Cleansed process.
    """

    def s3_to_emr_read(self, dataset, job_key, delete_raw_staging_path, delete_clean_output_location, clean_output_location, glue_database, dataset_key, dataset_info_table, run_start_ts,
                       file_extension, is_cross_account, cross_account_role_arn):
        """
            This function is used for reading source and target path from s3.
        """
        try:
            print("####### - Reading input data from s3 for the cleansed process")

            # Reading raw_staging_path from Handler file and adding subdirectories path if any.
            delete_raw_staging_location = delete_raw_staging_path + CleansedRunConstants.SUB_DIRECTORIES_PATH
            custom_schema_flag = CleansedRunConstants.UPDATE_KEYS[dataset]['CUSTOM_SCHEMA_FLAG']
            delimiter = CleansedRunConstants.DELIMITER

            # Reading source data
            if custom_schema_flag:
                print("####### - Reading source data with provided custom schema")
                delete_source_staging_load_df = self.get_source_df_with_custom_schema(delete_raw_staging_location, file_extension,
                                                                               delimiter)
            else:
                print("####### - Reading source data without custom schema")
                delete_source_staging_load_df = self.get_source_df(delete_raw_staging_location, file_extension, delimiter)

            # Dropping duplicates from input data
            delete_source_staging_load_df = delete_source_staging_load_df.dropDuplicates()

            # Column Name changing to appropriate column names.
            mapping = CleansedRunConstants.COLUMN_MAPPING[dataset]
            delete_source_staging_df = delete_source_staging_load_df.select(
                [col(c).alias(mapping.get(c, c)) for c in delete_source_staging_load_df.columns])
            #source_staging_df.printSchema()

            timezone = CleansedRunConstants.DATA_TIMEZONE
            timezone_var = '_ts_' + timezone

            # Changing date column datatype from string to timestamp.
            for a_dftype in delete_source_staging_df.dtypes:
                col_name = a_dftype[0]
                if col_name.lower().endswith(timezone_var):
                    delete_source_staging_df = delete_source_staging_df.withColumn(col_name, col(col_name).cast("timestamp"))

            # Changing the timestamp data to respective timezone data.
            # source_staging_df = DataValidation().change_timezone(spark, source_staging_df, timezone)

            # Calling editable access method if user want to add any extra column in raw data.
            # editable_access = CleansedRunConstants.UPDATE_KEYS[dataset]['EDITABLE_ACCESS_FLAG']
            # if editable_access:
            #     source_staging_df = EditableSpace().editable_snippet(spark, delete_source_staging_df)

            delete_source_records = delete_source_staging_df.count()
            print("####### - Total number of records from source path : " + str(delete_source_records))

            resp = DatasetInfo().get_dataset(dataset_info_table, dataset_key, is_cross_account,
                                             cross_account_role_arn)
            run_mode = resp["Item"]["run_mode"]
            # if CleansedRunConstants.UPDATE_KEYS[dataset]['SCD_TYPE'] == 'OVERWRITE':
            #     self.overwrite_run(source_staging_df, clean_output_location, dataset, job_key, source_records,
            #                        glue_database, dataset_info_table, dataset_key, run_start_ts)
            # else:
            #     print("####### - Run mode retrieved from dataset_info dynamodb table is" + str(resp))
            #
            #     if run_mode == "full_load":
            #         self.initial_run(source_staging_df, clean_output_location, dataset, job_key, source_records,
            #                          glue_database, dataset_info_table, dataset_key, run_start_ts)
            #
            #     elif run_mode == "delta_load":
            #         self.incremental_run(source_staging_df, clean_output_location, dataset,
            #                              job_key, source_records, glue_database, run_start_ts)

            if CleansedRunConstants.UPDATE_KEYS[dataset]['CONTAIN_DELETE'] == True:
                self.delete_run(delete_source_staging_df, delete_clean_output_location, clean_output_location, dataset, job_key, delete_source_records,
                                   glue_database, dataset_info_table, dataset_key, run_start_ts, run_mode)


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


    def delete_run(self, delete_staging_df, delete_clean_output_location, clean_output_location, dataset, job_key, source_records, glue_database,
                      dataset_info_table, dataset_key, run_start_ts, run_mode):
        """
            This function is being used when system runs the Full load for the first time
        """
        try:
            print("####### - Overwrite load run started for dataset - " + dataset)

            # partition key
            partition_key_list = CleansedRunConstants.UPDATE_KEYS[dataset]['PARTITION_KEY']
            pk_columns = CleansedRunConstants.UPDATE_KEYS[dataset]['PK_COLUMN']
            order_by_column = CleansedRunConstants.UPDATE_KEYS[dataset]['ORDER_BY_COLUMN']
            hudi_table_type = CleansedRunConstants.UPDATE_KEYS[dataset]['HUDI_TABLE_TYPE']
            save_delete_datasets_flag = CleansedRunConstants.UPDATE_KEYS[dataset]['SAVE_DELETE_DATASETS']
            create_delete_table_flag = CleansedRunConstants.UPDATE_KEYS[dataset]['CREATE_DELETE_TABLE']
            table_name_cln = dataset + "_cln"
            table_name_del = dataset + "_delete_cln"

            if len(partition_key_list) > 1:
                print("####### - The clean process is running for overwrite mode with data partition column")
                hudi_configurations = {
                    'hoodie.table.name': table_name_cln,
                    'hoodie.table.type': hudi_table_type,
                    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
                    'hoodie.datasource.write.recordkey.field': pk_columns,
                    'hoodie.datasource.write.partitionpath.field': partition_key_list,
                    'hoodie.datasource.write.precombine.field': order_by_column,
                    'hoodie.datasource.write.operation': 'delete',
                    # 'hoodie.datasource.write.insert.drop.duplicates': 'true',
                    'hoodie.datasource.write.hive_style_partitioning': True,
                    'hoodie.datasource.hive_sync.enable': 'true',
                    'hoodie.datasource.hive_sync.database': glue_database,
                    'hoodie.datasource.hive_sync.table': table_name_cln,
                    'hoodie.datasource.hive_sync.partition_fields': partition_key_list,
                    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'

                }
            elif len(partition_key_list) == 0:
                print("####### - The clean process is running for overwrite mode without data partition column")
                hudi_configurations = {
                    'hoodie.table.name': table_name_cln,
                    'hoodie.table.type': hudi_table_type,
                    #'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
                    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.CustomKeyGenerator',
                    'hoodie.datasource.write.recordkey.field': pk_columns,
                    'hoodie.datasource.write.partitionpath.field': '',
                    'hoodie.datasource.write.precombine.field': order_by_column,
                    'hoodie.datasource.write.operation': 'delete',
                    # 'hoodie.datasource.write.insert.drop.duplicates': 'true',
                    'hoodie.datasource.write.hive_style_partitioning': True,
                    'hoodie.datasource.hive_sync.enable': 'true',
                    'hoodie.datasource.hive_sync.database': glue_database,
                    'hoodie.datasource.hive_sync.table': table_name_cln,
                    'hoodie.datasource.hive_sync.partition_fields': '',
                    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor'

                }
            else:
                raise Exception("####### - Please specify correct data partition name")

            delete_staging_df.write \
                .format('org.apache.hudi') \
                .options(**hudi_configurations) \
                .mode('append') \
                .save(clean_output_location)

            target_records = delete_staging_df.count()
            print("####### - Total number of output records : " + str(target_records))

            if save_delete_datasets_flag and create_delete_table_flag:
                if len(partition_key_list) > 1:
                    print("####### - The clean process is running for overwrite mode with data partition column")
                    hudi_configurations = {
                        'hoodie.table.name': table_name_del,
                        'hoodie.table.type': hudi_table_type,
                        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
                        'hoodie.datasource.write.recordkey.field': pk_columns,
                        'hoodie.datasource.write.partitionpath.field': partition_key_list,
                        'hoodie.datasource.write.precombine.field': order_by_column,
                        'hoodie.datasource.write.operation': 'upsert',
                        # 'hoodie.datasource.write.insert.drop.duplicates': 'true',
                        'hoodie.datasource.write.hive_style_partitioning': True,
                        'hoodie.datasource.hive_sync.enable': 'true',
                        'hoodie.datasource.hive_sync.database': glue_database,
                        'hoodie.datasource.hive_sync.table': table_name_del,
                        'hoodie.datasource.hive_sync.partition_fields': partition_key_list,
                        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'

                    }
                elif len(partition_key_list) == 0:
                    print("####### - The clean process is running for overwrite mode without data partition column")
                    hudi_configurations = {
                        'hoodie.table.name': table_name_del,
                        'hoodie.table.type': hudi_table_type,
                        #'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
                        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.CustomKeyGenerator',
                        'hoodie.datasource.write.recordkey.field': pk_columns,
                        'hoodie.datasource.write.partitionpath.field': '',
                        'hoodie.datasource.write.precombine.field': order_by_column,
                        'hoodie.datasource.write.operation': 'upsert',
                        # 'hoodie.datasource.write.insert.drop.duplicates': 'true',
                        'hoodie.datasource.write.hive_style_partitioning': True,
                        'hoodie.datasource.hive_sync.enable': 'true',
                        'hoodie.datasource.hive_sync.database': glue_database,
                        'hoodie.datasource.hive_sync.table': table_name_del,
                        'hoodie.datasource.hive_sync.partition_fields': '',
                        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor'

                    }
                else:
                    raise Exception("####### - Please specify correct data partition name")

                delete_staging_df.write \
                    .format('org.apache.hudi') \
                    .options(**hudi_configurations) \
                    .mode('append') \
                    .save(delete_clean_output_location)

            elif save_delete_datasets_flag :
                if len(partition_key_list) > 1:
                    print("####### - The clean process is running for overwrite mode with data partition column")
                    hudi_configurations = {
                        'hoodie.table.name': table_name_del,
                        'hoodie.table.type': hudi_table_type,
                        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
                        'hoodie.datasource.write.recordkey.field': pk_columns,
                        'hoodie.datasource.write.partitionpath.field': partition_key_list,
                        'hoodie.datasource.write.precombine.field': order_by_column,
                        'hoodie.datasource.write.operation': 'upsert',
                        # 'hoodie.datasource.write.insert.drop.duplicates': 'true',
                        # 'hoodie.datasource.write.hive_style_partitioning': True,
                        # 'hoodie.datasource.hive_sync.enable': 'true',
                        # 'hoodie.datasource.hive_sync.database': glue_database,
                        # 'hoodie.datasource.hive_sync.table': table_name_del,
                        # 'hoodie.datasource.hive_sync.partition_fields': partition_key_list,
                        # 'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'

                    }
                elif len(partition_key_list) == 0:
                    print("####### - The clean process is running for overwrite mode without data partition column")
                    hudi_configurations = {
                        'hoodie.table.name': table_name_del,
                        'hoodie.table.type': hudi_table_type,
                        #'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
                        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.CustomKeyGenerator',
                        'hoodie.datasource.write.recordkey.field': pk_columns,
                        'hoodie.datasource.write.partitionpath.field': '',
                        'hoodie.datasource.write.precombine.field': order_by_column,
                        'hoodie.datasource.write.operation': 'upsert',
                        # 'hoodie.datasource.write.insert.drop.duplicates': 'true',
                        # 'hoodie.datasource.write.hive_style_partitioning': True,
                        # 'hoodie.datasource.hive_sync.enable': 'true',
                        # 'hoodie.datasource.hive_sync.database': glue_database,
                        # 'hoodie.datasource.hive_sync.table': table_name_del,
                        # 'hoodie.datasource.hive_sync.partition_fields': '',
                        # 'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor'
                    }
                else:
                    raise Exception("####### - Please specify correct data partition name")

                delete_staging_df.write \
                    .format('org.apache.hudi') \
                    .options(**hudi_configurations) \
                    .mode('append') \
                    .save(delete_clean_output_location)



            run_end_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            DatasetInfo().update_dataset(dataset_info_table, dataset_key, run_start_ts, run_end_ts,
                                         is_cross_account, cross_account_role_arn, sla="",
                                         run_mode="delta_load", data_location=clean_output_location)

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
    delete_raw_staging_path = sys.argv[2]
    delete_clean_output_location = sys.argv[3]
    glue_database = sys.argv[4]
    file_extension = sys.argv[5]
    clean_output_location = sys.argv[6]


    run_start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if dataset is None or clean_output_location is None \
            or glue_database is None or file_extension is None \
            or delete_clean_output_location is None or delete_raw_staging_path is None:
        raise Exception("####### - All required argument are not provided for the process")

    job_key = CleansedRunConstants.AUDIT_PREFIX + "_" + dataset + "_delete_" + start_ts_str
    job_name = CleansedRunConstants.AUDIT_PREFIX + "_" + dataset
    dynamoDB_table = CleansedRunConstants.DYNAMODB_TABLE
    source = CleansedRunConstants.SOURCE
    layer_name = 'Clean'
    job_run_date = datetime.now().strftime("%Y-%m-%d")

    dataset_info_table = CleansedRunConstants.DATASET_INFO_TABLE
    dataset_key = CleansedRunConstants.AUDIT_PREFIX + "_" + dataset
    is_cross_account = CleansedRunConstants.CROSS_ACCOUNT_FLAG
    cross_account_role_arn = CleansedRunConstants.CROSS_ACCOUNT_ROLE_ARN

    audit_item_json = AuditHandler().audit_input_json_generator(dataset, start_datetime, job_key, job_name, source,
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
        .enableHiveSupport() \
        .getOrCreate()

    print("####### - Starting the process for Deletes run")
    cleansed_layer_run = CleansedDataCreation()
    cleansed = cleansed_layer_run.s3_to_emr_read(dataset, job_key, delete_raw_staging_path, delete_clean_output_location,
                                                 clean_output_location, glue_database, dataset_key, dataset_info_table,
                                                 run_start_ts, file_extension, is_cross_account, cross_account_role_arn)
