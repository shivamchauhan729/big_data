class GlueCatalogHandler(object):
    
    def __init__(self):
        pass

    def create_external_table(self, spark, target_table, partition_key_list, manifest_location):
        col_schema_list = spark.sql(f"""select * from {target_table}""").dtypes
        partition_key_schema = ''
        for pk in partition_key_list:
            for schema in col_schema_list:
                if schema[0] == pk:
                    partition_key_schema+= pk + ' ' + schema[1] + ','
                    col_schema_list.remove(schema)

        col_type_schema = ''
        for schema in col_schema_list:
            col_type_schema += schema[0] + ' ' + schema[1] + ','

        spark.sql(f""" CREATE EXTERNAL TABLE IF NOT EXISTS {target_table}_external ({col_type_schema[:-1]})
        PARTITIONED BY ({partition_key_schema[:-1]})
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION '{manifest_location}' 
        """)

    def generate_manifest(self, spark, target_table):
        spark.sql(f"""GENERATE symlink_format_manifest FOR TABLE {target_table}""")

    def msck_repair_partitions(self, spark, target_table):
        spark.sql(f"""MSCK REPAIR TABLE {target_table}_external""")
