import logging
from pyspark.sql import SparkSession
import argparse


def create_refresh_external_table():
    manifest_location = clean_path + glue_database + ".db/" + clean_table + "/_symlink_format_manifest/"
    clean_location = "clean_path" + glue_database + ".db/" + clean_table + '/'

    spark.sql(f"""
            CREATE OR REPLACE TABLE users_cln (user_id String, sign_in_count String, current_sign_in_utc_ts String, last_sign_in_utc_ts String, failed_attempts String, unlock_token String, locked_utc_ts String, merchant_id String, create_utc_ts String, update_utc_ts String, invitation_token String, invitation_create_utc_ts String,invitation_sent_utc_ts String, invitation_accept_utc_ts String, invitation_limit_num String, invited_by_id String, invited_by_type String, invitations_cnt String, uuid String, utm_src String, utm_campaign String,utm_medium String, utm_term String, utm_content String, tutorial_completed_create_listing_flag String, tutorial_completed_enable_inventory_sync_flag String, tutorial_completed_fulfill_order_flag String, time_zone String)
            USING DELTA
            PARTITIONED BY (sign_in_count, failed_attempts)
            LOCATION '{clean_location}'
            """)

    spark.sql(f"""GENERATE symlink_format_manifest FOR TABLE users_cln""")

    spark.sql(f""" ALTER TABLE users_cln SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)""")
            

    spark.sql(f""" CREATE EXTERNAL TABLE IF NOT EXISTS users_cln_external (user_id String, current_sign_in_utc_ts String, last_sign_in_utc_ts String, unlock_token String, locked_utc_ts String, merchant_id String, create_utc_ts String, update_utc_ts String, invitation_token String, invitation_create_utc_ts String,invitation_sent_utc_ts String, invitation_accept_utc_ts String, invitation_limit_num String, invited_by_id String, invited_by_type String, invitations_cnt String, uuid String, utm_src String, utm_campaign String,utm_medium String, utm_term String, utm_content String, tutorial_completed_create_listing_flag String, tutorial_completed_enable_inventory_sync_flag String, tutorial_completed_fulfill_order_flag String, time_zone String)
    PARTITIONED BY (sign_in_count STRING, failed_attempts STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '{manifest_location}'
    """)


def create_refresh_external_table_scd2():
    spark.sql(f"""
            ALTER TABLE users_cln ADD columns (record_active_flag string)
          """)

    spark.sql(f"""
            UPDATE users_cln SET record_active_flag = 'true'
            """)

    spark.sql(f"""
            ALTER TABLE users_cln_external ADD columns (record_active_flag string)
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
        create_refresh_external_table_scd2()



    