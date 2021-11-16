#!/usr/bin/python
# -*- coding: utf-8 -*-

#######################################Module Information####################################
#  Module Name         : DataValidation                                                     #
#  Purpose             : To validate the date pattern                                       #
#  Input Parameters    : Spark Session , input dataframe                                    #
#  Output Value        : Bad records , valid date pattern dataframe                         #
#  Pre-requisites      : Spark Session to be created in parent module.                      #
#############################################################################################


# Library and external modules declaration
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re

today_date = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')

# Setting variable for checking date pattern
regex = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"


class DataValidation(object):
    """
    Class contains all the function related to data validation.
    """

    def date_pattern_validator(self, spark, source_staging_load_df, S3_Bucket, dataset_name):
        """
            This function is used to validate the input data has ISO8601 date format or not.
        """
        # Importing s3 bucket name and dataset for error record path
        error_path = 's3://' + S3_Bucket + '/validation_error_records/' + dataset_name + '/report_' + today_date

        source_staging_load_df = source_staging_load_df.withColumn("valid_pattern_status", lit(0))
        # Checking the date pattern in each date column
        for a_dftype in source_staging_load_df.dtypes:
            col_name = a_dftype[0]
            # Checking the date pattern on columns with date keyword and having not nulls.
            if col_name.lower().endswith('_at'):
                print("######## - Date pattern check performed on : " + col_name)
                source_staging_load_df = source_staging_load_df.withColumn("valid_pattern_status", when(
                    (col(col_name).isNull()), col("valid_pattern_status") + 0)
                                                                           .when((col(col_name) == ""),
                                                                                 col("valid_pattern_status") + 0)
                                                                           .when((col(col_name).rlike(regex)),
                                                                                 col("valid_pattern_status") + 0)
                                                                           .otherwise(col("valid_pattern_status") + 1))
            else:
                print("######## - Date pattern check not performed on : " + col_name)

        # Filtering out error records
        error_df = source_staging_load_df.filter(col("valid_pattern_status") > 0)

        if error_df.count() > 0:
            # Writing bad records on s3 for report
            error_df = error_df.drop("valid_pattern_status")
            error_df.repartition(1).write.format('csv').option('header', 'true').save(error_path)
            print("######## - Input has invalid date records and successfully written bad records on s3")
        else:
            print("######## - There is no invalid date pattern record in input")

        # Filtering out correct records
        source_staging_df = source_staging_load_df.filter(col("valid_pattern_status") == 0)
        source_staging_df = source_staging_df.drop("valid_pattern_status")
        return source_staging_df

    def extra_col_schema_check(self, spark, source_staging_df, source_cleansed_df, src_desired_cols, partition_key_list):
        """
            This function is for handling the scenario if source comes with extra columns
        """
        # Generating extra column list
        extra_col_list = list(set(source_staging_df.dtypes).difference(set(source_cleansed_df.dtypes)))
        extra_col_names = list(set(source_staging_df.columns).difference(set(source_cleansed_df.columns)))

        # Adding Extra column to cleansed dataframe
        for col_val in extra_col_list:
            col_name = col_val[0]
            col_type = col_val[1]
            source_cleansed_df = source_cleansed_df.withColumn(col_name, lit(None)).withColumn(col_name,
                                                                                               col(col_name).cast(
                                                                                                   col_type))

        source_staging_df = source_staging_df.select(sorted(source_staging_df.columns))
        source_cleansed_df = source_cleansed_df.select(sorted(source_cleansed_df.columns))
        load_df = source_cleansed_df.unionAll(source_staging_df)

        # Arranging all column in order and system column at the end
        all_cols = load_df.columns
        other_cols = list(set(all_cols).difference(set(src_desired_cols)))
        other_cols.sort()
        new_column_order = src_desired_cols + other_cols

        partition_key = []
        if partition_key_list:
            for i in partition_key_list:
                for each in new_column_order:
                    if each == i:
                        partition_key.append(each)
                        new_column_order.remove(each)

        final_column_order = new_column_order + partition_key

        load_df = load_df[final_column_order]

        # Casting into timestamp and converting camel case to snake case for column name.
        for col_val in extra_col_list:
            col_name = col_val[0]
            if col_name.endswith('Timestamp'):
                load_df = load_df.withColumn(col_name, col(col_name).cast("timestamp"))
                load_df = load_df.withColumnRenamed(col_name, re.sub('([a-z0-9])([A-Z])', r'\1_\2', col_name).lower())
            else:
                # load_df = load_df.withColumn(col_name,when((col(col_name).rlike(regex)), col(col_name).cast("timestamp")).otherwise(col_name))
                load_df = load_df.withColumnRenamed(col_name, re.sub('([a-z0-9])([A-Z])', r'\1_\2', col_name).lower())

        load_df = load_df.select([col(c).alias(c.replace('_timestamp', '_ts_utc')) for c in load_df.columns])
        return load_df, extra_col_names

    def missing_col_schema_check(self, spark, source_staging_df, source_cleansed_df, cleansed_desired_cols, partition_key_list):
        """
            This function is for handling the scenario if source comes with extra columns
        """
        # Generating missing column list
        extra_col_list = list(set(source_cleansed_df.dtypes).difference(set(source_staging_df.dtypes)))
        extra_col_names = list(set(source_cleansed_df.columns).difference(set(source_staging_df.columns)))

        # Adding Extra column to cleansed dataframe
        for col_val in extra_col_list:
            col_name = col_val[0]
            col_type = col_val[1]
            source_staging_df = source_staging_df.withColumn(col_name, lit(None)).withColumn(col_name,
                                                                                             col(col_name).cast(
                                                                                                 col_type))

        source_staging_df = source_staging_df.select(sorted(source_staging_df.columns))
        source_cleansed_df = source_cleansed_df.select(sorted(source_cleansed_df.columns))
        load_df = source_cleansed_df.unionAll(source_staging_df)

        # Arranging all column in order and system column at the end
        all_cols = load_df.columns
        other_cols = list(set(all_cols).difference(set(cleansed_desired_cols)))
        other_cols.sort()
        new_column_order = cleansed_desired_cols + other_cols

        partition_key = []
        if partition_key_list:
            for i in partition_key_list:
                for each in new_column_order:
                    if each == i:
                        partition_key.append(each)
                        new_column_order.remove(each)

        final_column_order = new_column_order + partition_key

        load_df = load_df[final_column_order]

        # Casting into timestamp and converting camel case to snake case for column name.
        for col_val in extra_col_list:
            col_name = col_val[0]
            if col_name.endswith('Timestamp'):
                load_df = load_df.withColumn(col_name, col(col_name).cast("timestamp"))
                load_df = load_df.withColumnRenamed(col_name, re.sub('([a-z0-9])([A-Z])', r'\1_\2', col_name).lower())
            else:
                load_df = load_df.withColumnRenamed(col_name, re.sub('([a-z0-9])([A-Z])', r'\1_\2', col_name).lower())

        load_df = load_df.select([col(c).alias(c.replace('_timestamp', '_ts_utc')) for c in load_df.columns])
        return load_df, extra_col_names

    def change_timezone(self, spark, df, timezone):
        """
        This function is used to convert utc to different timezones.
        """
        for a_dftype in df.dtypes:
            col_name = a_dftype[0]
            col_type = a_dftype[1]
            if col_type == 'timestamp' and timezone == 'mst':
                df = df.withColumn(col_name, from_utc_timestamp(col(col_name), timezone.upper()))
            if col_type == 'timestamp' and timezone == 'utc':
                df = df
        return df



