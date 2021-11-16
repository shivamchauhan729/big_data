from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from python.utils.glue_catalog_handler import GlueCatalogHandler


class CleansedDataCreation(object):
    """
        Class contains all the functions related to Cleansed process.
    """

    def s3_to_emr_read(self, spark, dataset, raw_staging_path, sub_directory_path, clean_output_path, file_extension, scd, delimiter, audit_obj, dynamoDB_table, is_cross_account, cross_account_role_arn, job_key, run_constants, Env, glue_database, glue_table):
        """
            This function is used for reading source and target path from s3.
        """
        try:
            print("####### - Reading input data from s3 for the cleansed process")
            raw_staging_location = raw_staging_path + sub_directory_path
            custom_schema_flag = run_constants[Env][dataset]['custom_schema_flag']

            if eval(custom_schema_flag):
                print("####### - Reading source data with provided custom schema")
                #c_schema = run_constants[Env][dataset]['schema']
                c_schema_list = run_constants[Env][dataset]['schema']
                struct_field_list = list(map(lambda l:([StructField(l[0], l[1])]),eval(c_schema_list[0])))
                final_struct_field = [s for sf_list in struct_field_list for s in sf_list]

                source_staging_load_df = self.get_source_df_with_custom_schema(spark, final_struct_field, raw_staging_location, file_extension, delimiter)
            else:
                print("####### - Reading source data without custom schema")
                source_staging_load_df = self.get_source_df(spark, raw_staging_location, file_extension, delimiter)
            
            mapping = run_constants[Env][dataset]['column_mapping']
            source_staging_load_df = source_staging_load_df.select(
                [col(c).alias(mapping.asDict().get(c, c)) for c in source_staging_load_df.columns])
            # Dropping duplicates from input data
            source_staging_df = source_staging_load_df.dropDuplicates()
            
            # creating glue database table
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {glue_database}")
            spark.sql(f"USE {glue_database}")

            source_records = source_staging_df.count()
            print("######### Source records",source_records)
            if scd == 'scd0':
                self.overwrite_run(spark, audit_obj, dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_staging_df, clean_output_path, dataset, source_records, run_constants, Env, glue_table, glue_database)
            elif scd == 'scd1':
                self.scd_type1(spark, audit_obj, dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_staging_df, clean_output_path, dataset, source_records, run_constants, Env, glue_table, glue_database)
            elif scd == 'scd2':
                self.scd_type2(spark, audit_obj, dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_staging_df, clean_output_path, dataset, source_records, run_constants, Env, glue_table, glue_database)
            else:
                raise Exception("####### - Please specify correct SCD mode")

        except Exception as e:
            print("####### - Failed while reading s3 files. " + str(e))
            audit_obj.update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                                 cross_account_role_arn, source_records=0, target_records=0,
                                                job_status="FAILED", error_message=str(e))
            raise e


    def get_source_df(self, spark, path, file_extension, delimiter):
        """
            This function is used for reading data when no custom schema provided
        """
        if file_extension == '.csv' or file_extension == '.txt':
            input_df = spark.read.csv(path, header=True, sep=delimiter)
        elif file_extension == '.parquet':
            input_df = spark.read.parquet(path)
        elif file_extension == '.json':
            input_df = spark.read.json(path)
        else:
            print("####### - File Format Not Supported")
        return input_df

    def get_source_df_with_custom_schema(self, spark, c_schema, path, file_extension, delimiter):
        """
            This function is used for reading data on providing custom schema
        """
        final_struc = StructType(c_schema)
       
        if file_extension == '.csv' or file_extension == '.txt':
            input_df = spark.read.csv(path, header=CleansedRunConstants.HEADER, sep=delimiter, schema=final_struc)
        elif file_extension == '.parquet':
            input_df = spark.read.parquet(path, schema=final_struc)
        elif file_extension == '.json':
            input_df = spark.read.json(path, schema=final_struc)
        else:
            print("####### - File Format Not Supported")
        return input_df


    def overwrite_run(self, spark, audit_obj, dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_staging_df, clean_output_location, dataset, source_records, run_constants, Env, glue_table, glue_database):
        """
            This function is being used when system runs the Full load for the first time
        """
        try:
            print("####### - Overwrite load run started for dataset - " + dataset)
            partition_key_list = run_constants[Env][dataset]['partition_key']
            source_staging_df = source_staging_df.withColumn("active", lit("true"))

            if len(partition_key_list)>=1:
                source_staging_df.write.mode('overwrite').format("delta").partitionBy(partition_key_list).option("overwriteSchema", "true").saveAsTable(glue_table)
            else:
                source_staging_df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(glue_table)
            

            GlueCatalogHandler().generate_manifest(spark, glue_table)
            manifest_location = clean_output_location + glue_database + ".db/" + glue_table + "/_symlink_format_manifest/" 
            GlueCatalogHandler().create_external_table(spark, glue_table, partition_key_list, manifest_location)
            GlueCatalogHandler().msck_repair_partitions(spark, glue_table)

            target_records = source_staging_df.count()

            print("######### Clean Layer Spark Process successfully completed for overwrite run for dataset")

            # Updating the audit log table with success entry.
            audit_obj.update_audit_info(dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_records,
                                             target_records, job_status="SUCCEEDED",
                                             error_message="null")
            

        except Exception as e:
            print("####### - Failed while performing Overwrite Run " + str(e))
            audit_obj.update_audit_info(dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_records=0, target_records=0,
                                             job_status="FAILED", error_message=str(e))

            raise e



    def scd_type1(self, spark, audit_obj, dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_staging_df, clean_output_path, dataset, source_records, run_constants, Env, glue_table, glue_database):
        
        print("####### - The clean process is running for SCD1")
        try:
            pk = run_constants[Env][dataset]['primary_key']
            partition_key_list = run_constants[Env][dataset]['partition_key']

            staged_data = source_staging_df.withColumn("active", lit("true"))
            staged_data.write.mode("overwrite").format("delta").option("overwriteSchema","true").partitionBy(partition_key_list).saveAsTable(f'{glue_table}_stg')
            spark.sql(f""" MERGE INTO {glue_table} USING {glue_table}_stg on {glue_table}.{pk} = {glue_table}_stg.{pk} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * """)

            GlueCatalogHandler().generate_manifest(spark, glue_table)
            GlueCatalogHandler().msck_repair_partitions(spark, glue_table)

            print("######### Clean Layer Spark Process successfully completed for SCD-1 run for dataset")
            target_records = spark.sql(f"""select * from {glue_table}""").count()

            # Adding success entry in audit table
            audit_obj.update_audit_info(dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_records,
                                                target_records, job_status="SUCCEEDED",
                                                error_message="null")

        except Exception as e:
            print("####### - Failed while performing SCD-1 " + str(e))
            audit_obj.update_audit_info(dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_records=0, target_records=0,
                                             job_status="FAILED", error_message=str(e))

            raise e



    def scd_type2(self, spark, audit_obj, dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_staging_df, clean_output_path, dataset, source_records, run_constants, Env, glue_table, glue_database):

        print("####### - The clean process is running for SCD2")
        try:
            pk = run_constants[Env][dataset]['primary_key']
            partition_key_list = run_constants[Env][dataset]['partition_key']

            staged_data = source_staging_df.withColumn("active", lit("true"))
            staged_data.write.mode("overwrite").format("delta").option("overwriteSchema","true").partitionBy(partition_key_list).saveAsTable(f'{glue_table}_stg')

            spark.sql(f""" MERGE INTO {glue_table}
            USING (
            SELECT {glue_table}_stg.{pk} as mergeKey, {glue_table}_stg.*
            FROM {glue_table}_stg
            
            UNION ALL
                SELECT NULL as mergeKey, {glue_table}_stg.*
                FROM {glue_table}_stg JOIN {glue_table}
                ON {glue_table}_stg.{pk} = {glue_table}.{pk} 
                WHERE {glue_table}.active = true AND {glue_table}_stg.updated_at <> {glue_table}.updated_at 
            
            ) staged_updates
            ON {glue_table}.{pk} = mergeKey
            WHEN MATCHED AND {glue_table}.active = true AND {glue_table}.updated_at <> staged_updates.updated_at THEN  
            UPDATE SET active = false
            WHEN NOT MATCHED THEN 
            INSERT *
                        """)

            GlueCatalogHandler().generate_manifest(spark, glue_table)
            GlueCatalogHandler().msck_repair_partitions(spark, glue_table)


            print("######### Clean Layer Spark Process successfully completed for SCD-2 run for dataset")
            target_records = spark.sql(f"""select * from {glue_table}""").count()

            # Adding success entry in audit table
            audit_obj.update_audit_info(dynamoDB_table, job_key, is_cross_account,
                                                 cross_account_role_arn, source_records,
                                             target_records, job_status="SUCCEEDED",
                                             error_message="null")
        except Exception as e:
            print("####### - Failed while performing incremental load " + str(e))
            audit_obj.update_audit_info(dynamoDB_table, job_key, is_cross_account, cross_account_role_arn, source_records=0, target_records=0,
                                             job_status="FAILED", error_message=str(e))

            raise e
