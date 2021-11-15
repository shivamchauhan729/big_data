CREATE TABLE merchant_cln (
ecomm_provider_id int, merchant_id int, owning_user_id int, merchant_create_utc_ts timestamp, plan_code String, plan_status_type_desc String, plan_subscribed_utc_ts timestamp, cancel_utc_ts timestamp, record_active_flag string)
USING DELTA
PARTITIONED BY (ecomm_provider_id)
LOCATION 's3://aws-poc-serverless-analytics/delta_lake_demo/clean/delta_lake_demo.db/merchant_cln/'

GENERATE symlink_format_manifest FOR TABLE merchant_cln

ALTER TABLE merchants_cln SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)

CREATE EXTERNAL TABLE IF NOT EXISTS merchant_cln_external (
merchant_id int, owning_user_id int, merchant_create_utc_ts timestamp, plan_code String, plan_status_type_desc String, plan_subscribed_utc_ts timestamp, cancel_utc_ts timestamp, record_active_flag string)
PARTITIONED BY (ecomm_provider_id int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://aws-poc-serverless-analytics/delta_lake_demo/clean/delta_lake_demo.db/merchant_cln/_symlink_format_manifest/'
