#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'GDE'

#######################################Script Information####################################
#  Script Name         : CleansedRunConstants                                               #
#  Description         : This file contains all the constants for Cleansed layer run        #
#############################################################################################
from pyspark.sql.types import *


class CleansedRunConstants:
    """ Dynamodb table name for process log"""
    DYNAMODB_TABLE = 'job_run_details_dev'

    """ Config Table info """
    DATASET_INFO_TABLE = 'dataset_info_dev'

    """ Subdirectories path from the parent path to json/parquet file """
    SUB_DIRECTORIES_PATH = '/*/'

    """ Below parameter will be useful if we are working on other account and using acquisition data-lab account's
    DynamoDB for log auditing """
    CROSS_ACCOUNT_FLAG = False
    CROSS_ACCOUNT_ROLE_ARN = ''

    # Spark properties. Setting maximum no. of executor.
    MAX_EXECUTORS = "100"
    MIN_EXECUTORS = "1"

    """ Mention the correct delimiter if the input file format is csv or txt. If we are using any other file format then
    we can provide empty string """
    DELIMITER = ','
    """ Provide header True if we want to read header from input files otherwise False """
    HEADER = True

    # Audit table prefix
    AUDIT_PREFIX = 'delta_clean'
    SOURCE = 'DELTA'

    DATA_TIMEZONE = 'utc'

    # athena alter table query result storage s3 location
    ALTER_QUERY_OUTPUT_LOC = \
        's3://gd-emeadataunif-dev-private-gde-emea-rtf/athena_query_results/alter_query_result/'
    MSCK_QUERY_OUTPUT_LOC = \
        's3://gd-emeadataunif-dev-private-gde-emea-rtf/athena_query_results/msck_query_result/'

    # SNS topic arn
    SNS_TOPIC_ARN = 'arn:aws:sns:us-west-2:342445952537:gde-emea-rtf-alert'

    # old and new column name mapping
    COLUMN_MAPPING = {

        'merchants': {
            'id': 'merchants_id'

        }

    }

    """ UPDATE_KEYS details:
        where,
        PK_COLUMN = If the data has primary key, we have mention here in list and if primary key is not present in data
        then we have to keep empty list.
        ORDER_BY_COLUMN = Mention the column name who decides the latest record.
        PARTITION_KEY = Its a data partition key helpful in storing and retrieving the data.
        CUSTOM_SCHEMA_FLAG = If we want to provide custom schema for any dataset then this flag should be true(and 
        provide custom schema details in this same config) else keep False.
        SCD_TYPE = This has three options
            SCD1 : Keeping latest data in clean output
            SCD2 : Maintaining history of data in clean output
            OVERWRITE : Each time overwriting the complete data.
        EDITABLE_ACCESS_FLAG = To enable editable space for adding extra column or implementing any specific 
        business logic , keep this flag as True else False
    """

    UPDATE_KEYS = {
        'merchants': {'PK_COLUMN': 'merchants_id',
                      'ORDER_BY_COLUMN': 'merchants_id',
                      'PARTITION_KEY': ['merchants_id'],
                      'CUSTOM_SCHEMA_FLAG': False,
                      'SCD_TYPE': 'SCD2',
                      'EDITABLE_ACCESS_FLAG': False}

    }

    # Custom schema
    merchants_Schema = [StructField("id", StringType(), True),
                        StructField("name", StringType(), True)
                        ]

    schema_dict = {
        "merchants": merchants_Schema
    }
