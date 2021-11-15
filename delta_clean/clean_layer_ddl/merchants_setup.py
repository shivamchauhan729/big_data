import logging
from pyspark.sql import SparkSession
import argparse


def create_refresh_external_table():
    manifest_location = clean_path + glue_database + ".db/" + clean_table + "/_symlink_format_manifest/"
    clean_location = clean_path + glue_database + ".db/" + clean_table + '/'

    spark.sql(f"""
            CREATE OR REPLACE TABLE merchants_cln (ecomm_provider_id String, merchant_id String, plan_code String, owning_user_id int, merchant_create_utc_ts String, plan_status_type_desc String, plan_subscribed_utc_ts String, cancel_utc_ts String)
            USING DELTA
            PARTITIONED BY (plan_status_type_desc)
            LOCATION '{clean_location}'
            """)

    spark.sql(f"""GENERATE symlink_format_manifest FOR TABLE merchants_cln""")

    spark.sql(f""" ALTER TABLE merchants_cln SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)""")
            

    spark.sql(f""" CREATE EXTERNAL TABLE IF NOT EXISTS merchants_cln_external (ecomm_provider_id String, merchant_id String, plan_code String, owning_user_id int, merchant_create_utc_ts String, plan_subscribed_utc_ts String, cancel_utc_ts String)
    PARTITIONED BY (plan_status_type_desc STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '{manifest_location}'
    """)


def create_refresh_external_table_scd2():
    spark.sql(f"""
            ALTER TABLE merchants_cln ADD columns (record_active_flag string)
          """)

    spark.sql(f"""
            UPDATE merchants_cln SET record_active_flag = 'true'
            """)

    spark.sql(f"""
            ALTER TABLE merchants_cln_external ADD columns (record_active_flag string)
          """)




if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--env')
    parser.add_argument('--clean_path')
    parser.add_argument('--config_file_path')
    args = parser.parse_args()

    Env = args.env
    clean_path = args.clean_path

    logging.info("Creating Spark Session")
    spark = (SparkSession.builder.appName("Cleansed Data Creation app").config("spark.sql.warehouse.dir", clean_path) \
    .enableHiveSupport().getOrCreate())
    from delta.tables import *

    logger.info("Reading configuration file")    
    config_json = spark.read.option("multiline", "true").json(args.config_file_path)
    run_constants = list(map(lambda row: row.asDict(),config_json.collect()))[0]

    dataset = run_constants[Env]['dataset']
    clean_table = run_constants[Env][dataset]['glue_table']
    custom_schema = run_constants[Env][dataset]['custom_schema']
    partition_key_list = run_constants[Env][dataset]['partition_key']
    glue_database = run_constants[Env]['glue_database']
    scd_type = run_constants[Env][dataset]['scd_type']

    #spark.sql(f"DROP TABLE IF EXISTS {glue_database}.{clean_table}")
    #spark.sql(f"DROP TABLE IF EXISTS {glue_database}.{clean_table}_external")
    #spark.sql(f"DROP DATABASE IF EXISTS {glue_database}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {glue_database}")
    spark.sql(f"USE {glue_database}")

    ## creating external table
    if scd_type == 'scd0' or scd_type == 'scd1':
        create_refresh_external_table()
    elif scd_type == 'scd2':
        create_refresh_external_table()
        create_refresh_external_table_scd2()


    