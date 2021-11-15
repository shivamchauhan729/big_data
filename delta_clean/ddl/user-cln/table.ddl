CREATE TABLE user_cln(
user_id int,
sign_in_cnt int,
current_sign_in_utc_ts timestamp,
last_sign_in_utc_ts timestamp,
failed_attempts int,
unlock_token string,
locked_utc_ts timestamp,
merchant_id int,
create_utc_ts timestamp,
update_utc_ts timestamp,
invitation_token string,
invitation_create_utc_ts timestamp,
invitation_sent_utc_ts timestamp,
invitation_accept_utc_ts timestamp,
invitation_limit_num int,
invited_by_id int,
invited_by_type string,
invitations_cnt int,
uuid string,
utm_src string,
utm_campaign string,
utm_medium string,
utm_term string,
utm_content string,
tutorial_completed_create_listing_flag boolean,
tutorial_completed_enable_inventory_sync_flag boolean,
tutorial_completed_fulfill_order_flag boolean,
time_zone string,
cln_insert_utc_ts timestamp,
cln_update_utc_ts timestamp)
USING DELTA
PARTITIONED BY (failed_attempts)
LOCATION 's3://aws-poc-serverless-analytics/delta_lake_demo/clean/delta_lake_demo.db/user_cln/'


GENERATE symlink_format_manifest FOR TABLE user_cln

ALTER TABLE user_cln SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)

CREATE EXTERNAL TABLE IF NOT EXISTS user_cln_external(
user_id int,
sign_in_cnt int,
current_sign_in_utc_ts timestamp,
last_sign_in_utc_ts timestamp,
unlock_token string,
locked_utc_ts timestamp,
merchant_id int,
create_utc_ts timestamp,
update_utc_ts timestamp,
invitation_token string,
invitation_create_utc_ts timestamp,
invitation_sent_utc_ts timestamp,
invitation_accept_utc_ts timestamp,
invitation_limit_num int,
invited_by_id int,
invited_by_type string,
invitations_cnt int,
uuid string,
utm_src string,
utm_campaign string,
utm_medium string,
utm_term string,
utm_content string,
tutorial_completed_create_listing_flag boolean,
tutorial_completed_enable_inventory_sync_flag boolean,
tutorial_completed_fulfill_order_flag boolean,
time_zone string,
cln_insert_utc_ts timestamp,
cln_update_utc_ts timestamp)
PARTITIONED BY (failed_attempts int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://aws-poc-serverless-analytics/delta_lake_demo/clean/delta_lake_demo.db/user_cln/_symlink_format_manifest/'