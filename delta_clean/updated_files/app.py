import logging
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import lit
from delta.tables import *
from datetime import datetime


def get_source_df_with_custom_schema(file_format, raw_file_location, custom_schema):
    logger.info("Checking the file extension and reading as dataframe")
    if file_format == 'csv' or file_format == 'txt':
        logger.info("Reading csv/txt file")
        raw_df = spark.read.csv(raw_file_location, header=True, sep=',', schema=custom_schema)
    elif file_format == 'parquet':
        logger.info("Reading parquet file")
        raw_df = spark.read.format("parquet").load(raw_file_location)
    elif file_format == 'json':
        logger.info("Reading json file")
        raw_df = spark.read.json(raw_file_location, schema=custom_schema)
    else:
        logger.info("File format not supported")
    return raw_df


def delta_save(dataset):
    
    clean_table = run_constants[Env][dataset]['glue_table']
    custom_schema = run_constants[Env][dataset]['custom_schema']
    partition_key_list = run_constants[Env][dataset]['partition_key']
    scd_type = run_constants[Env][dataset]['scd_type']
    pk = run_constants[Env][dataset]['primary_key']
    file_format = run_constants[Env][dataset]['file_format']
    raw_file_location =  raw_path + "/" + dataset + "/"
    scd_type2_key = run_constants[Env][dataset]['scd_type2_key']

    rawDF = get_source_df_with_custom_schema(file_format, raw_file_location, custom_schema)
    
    logger.info("Checking the SCD Type given")
    if scd_type == 'scd0':
        (rawDF.write.mode("overwrite").option("overwriteSchema", "true").format("delta").partitionBy(partition_key_list).saveAsTable(f"{clean_table}"))
    elif scd_type == 'scd1':
        logger.info("Executing scd type 1 ")
        stg_table = (f"{clean_table}_stg")
        rawDF.createOrReplaceTempView(f"{stg_table}")
        spark.sql(f""" 
                      MERGE INTO {clean_table}
                      USING {stg_table}
                      on {clean_table}.{pk} = {stg_table}.{pk}
                      WHEN MATCHED THEN 
                          UPDATE SET *
                      WHEN NOT MATCHED 
                          THEN INSERT *  
                    """)
    elif scd_type =='scd2':
        logger.info("Executing scd type 2 ")
        stg_table = (f"{clean_table}_stg")
        rawDF = rawDF.withColumn("record_active_flag", lit("true"))
        rawDF.createOrReplaceTempView(f"{stg_table}")
        spark.sql(f""" MERGE INTO {clean_table}
                       USING (
                       SELECT {stg_table}.{pk} as mergeKey, {stg_table}.*
                       FROM {stg_table}
                       
                       UNION ALL
                           SELECT NULL as mergeKey, {stg_table}.*
                           FROM {stg_table} JOIN {clean_table}
                           ON {stg_table}.{pk} = {clean_table}.{pk} 
                           WHERE {clean_table}.record_active_flag = 'true' AND {stg_table}.{scd_type2_key} <> {clean_table}.{scd_type2_key} 
                       
                       ) staged_updates
                       ON {clean_table}.{pk} = staged_updates.mergeKey
                       WHEN MATCHED AND {clean_table}.record_active_flag = 'true' AND {clean_table}.{scd_type2_key} <> staged_updates.{scd_type2_key} THEN  
                       UPDATE SET record_active_flag = 'false'
                       WHEN NOT MATCHED THEN 
                       INSERT *      
                                   """)

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--env')
    parser.add_argument('--raw_path')
    parser.add_argument('--clean_path')
    parser.add_argument('--config_file_path')
    args = parser.parse_args()

    Env = args.env
    raw_path = args.raw_path
    clean_path = args.clean_path

    logging.info("Creating Spark Session")
    spark = (SparkSession.builder.appName("Cleansed Data Creation app").config("spark.sql.warehouse.dir", clean_path) \
    .enableHiveSupport().getOrCreate())

    logger.info("Reading configuration file")    
    config_json = spark.read.option("multiline", "true").json(args.config_file_path)
    run_constants = list(map(lambda row: row.asDict(),config_json.collect()))[0]

    logger.info("Reading constants from configuration file") 
    datasets = run_constants[Env]['dataset']

    glue_database = run_constants[Env]['glue_database']
    spark.sql(f"USE {glue_database}")

    list(map(delta_save, datasets))

    