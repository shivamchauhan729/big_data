from pyspark.sql import SparkSession
from pyspark_app.clean import CleansedDataCreation
from python.utils.audit_handler import AuditHandler
from datetime import datetime
import sys

start_datetime = datetime.now()
start_ts_str = start_datetime.strftime("%Y%m%d%H%M%S")

if __name__ == "__main__":

    dw_dir  = sys.argv[1]
    print("########################### datawarehouse directory - ",dw_dir)

    spark = SparkSession.builder.appName("Cleansed Data Creation app") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.speculation", "true") \
        .config("spark.sql.codegen.wholeStage", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.warehouse.dir", dw_dir) \
        .enableHiveSupport() \
        .getOrCreate()

    Env = 'dev'
    config_json = spark.read.option("multiline", "true").json("s3://aws-poc-serverless-analytics/emr/delta_lake/code/config.json")
    run_constants = list(map(lambda row: row.asDict(),config_json.collect()))[0]
    dataset = run_constants[Env]['dataset']    
    bucket_name = run_constants[Env]['bucket_name']
    raw_path_input = run_constants[Env]['raw_path_input']
    dynamoDB_table = run_constants[Env]['dynamoDB_table']
    AUDIT_PREFIX = run_constants[Env]['audit_prefix']
    source = run_constants[Env]['source']
    dataset_info_table = run_constants[Env]['dataset_info_table']
    is_cross_account = eval(run_constants[Env]['is_cross_account'])
    cross_account_role_arn = run_constants[Env]['cross_account_role_arn']
    layer_name = run_constants[Env]['layer_name']
    sub_directory_path = run_constants[Env]['sub_directory_path']
    glue_database = run_constants[Env]['glue_database']

    file_extension = run_constants[Env][dataset]['file_format']
    delimiter = run_constants[Env][dataset]['delimiter']
    scd = run_constants[Env][dataset]['scd']
    glue_table = run_constants[Env][dataset]['glue_table']

    raw_staging_path = 's3://' + bucket_name + '/' + Env + '/flattened_raw/' + raw_path_input
    #clean_output_location = 's3://' + bucket_name + '/' + Env + '/clean/' + dataset + '_cln_no_col_map'
    job_key = AUDIT_PREFIX + '_' + dataset + "_" + start_ts_str
    dataset_key = AUDIT_PREFIX + '_' + dataset
    job_name = AUDIT_PREFIX + '_' + dataset
    job_run_date = datetime.now().strftime("%Y-%m-%d")
    applicationName = "CleanDataCreation for " + dataset


    audit_obj = AuditHandler()
    dataset_id = audit_obj.get_dataset_dataset_id(dataset_info_table, dataset_key, is_cross_account, cross_account_role_arn)
    audit_item_json = audit_obj.audit_input_json_generator(dataset, start_datetime, job_key, dataset_id, job_name, source,
                                                                layer_name, job_run_date)

    response, temp_key = audit_obj.insert_audit_info(dynamoDB_table, start_datetime, audit_item_json,
                                                          is_cross_account, cross_account_role_arn)

    print("####### - Starting the process for Cleansed run")
    cleansed_layer_run = CleansedDataCreation()
    cleansed_layer_run.s3_to_emr_read(spark, dataset, raw_staging_path, sub_directory_path, dw_dir, file_extension, scd, delimiter,
                                            audit_obj, dynamoDB_table, is_cross_account, cross_account_role_arn, job_key, run_constants, Env, glue_database, glue_table)
