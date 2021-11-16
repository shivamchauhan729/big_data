#!/usr/bin/python
# -*- coding: utf-8 -*-

#######################################Module Information####################################
#  Module Name         : GlueCatalogHandler                                                 #
#  Purpose             : To update the glue location and column                             #
#  Pre-requisites      : Table should already available in glue                             #
#############################################################################################

# Library and external modules declaration
import copy
import json, time
import boto3

client = boto3.client('glue')
athena = boto3.client('athena')


class GlueCatalogHandler(object):
    """
        Class contains all the function to update the glue location and column.
    """

    def create_glue_table(self, df, clean_output_path, table_name, glue_database, partition_key_list):
        """
            This function is used for creating glue table
        """
        # Reading dataframe column name and datatypes
        input_list = df.dtypes
        glue_column_list = []

        # Making column list for glue api call
        for each in input_list:
            column_dict = {}
            column_dict["Type"] = each[1].replace("integer", "int").replace("long", "bigint")
            column_dict["Name"] = each[0]
            glue_column_list.append(column_dict.copy())

        partition_key = []
        for i in partition_key_list:
            for each in glue_column_list:
                if each["Name"] == i:
                    partition_key.append(each)
                    glue_column_list.remove(each)

        glue_database_name = glue_database
        glue_table_name = table_name
        glue_table_location = clean_output_path

        response = client.create_table(
            DatabaseName=glue_database_name,
            TableInput={"Name": glue_table_name,
                        "StorageDescriptor": {
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            "SortColumns": [],
                            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            "SerdeInfo": {
                                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                "Parameters": {
                                    "serialization.format": "1"
                                }
                            },
                            "BucketColumns": [],
                            "Parameters": {
                                "CrawlerSchemaDeserializerVersion": "1.0",
                                "compressionType": "none",
                                "classification": "parquet",
                                "typeOfData": "file",
                                "CrawlerSchemaSerializerVersion": "1.0"
                            },
                            "Location": glue_table_location,
                            "NumberOfBuckets": -1,
                            "StoredAsSubDirectories": False,
                            "Columns": glue_column_list,
                            "Compressed": False
                        },
                        "PartitionKeys": partition_key,
                        "Parameters": {
                            "CrawlerSchemaDeserializerVersion": "1.0",
                            "compressionType": "none",
                            "classification": "parquet",
                            "typeOfData": "file",
                            "CrawlerSchemaSerializerVersion": "1.0"
                        },
                        "Owner": "owner",
                        "TableType": "EXTERNAL_TABLE"
                        }
        )

    def update_glue_catalog(self, existing_table_schema_json, new_table_json, dataset, partition_key_list,
                            clean_output_path):
        """
        This function is used for updating the glue location
        """
        # Load the schema json into json object
        schema_json = existing_table_schema_json
        glue_json = json.loads(new_table_json)
        new_s3_location = clean_output_path

        # Prepare Column List from schema json by removing metadata key
        temp_column_list = [{k: v for k, v in d.items() if k not in ['metadata', 'nullable']} for d in
                            glue_json["fields"]]
        column_list = []
        for temp_col in temp_column_list:
            t_dict = {}
            for k, v in temp_col.items():
                if type(v) == str:
                    t_dict.update({k.title(): v.replace("integer", "int").replace("long", "bigint")})
                elif type(v) == dict:
                    if v["elementType"] == "string":
                        t_dict.update({k.title(): v["type"].replace("array", "array<string>")})
                    elif v["elementType"] == "integer":
                        t_dict.update({k.title(): v["type"].replace("array", "array<int>")})
                    elif v["elementType"] == "long":
                        t_dict.update({k.title(): v["type"].replace("array", "array<bigint>")})
            column_list.append(t_dict)

        # Remove the partition key from the column list
        new_column_list = []
        for each in column_list:
            if each["Name"] not in partition_key_list:
                new_column_list.append(each)

        # Get required keys from the schema_json to avoid newly added keys from glue
        main_req = ["StorageDescriptor", "PartitionKeys", "Name", "Owner", "TableType", "Retention"]
        seperate_list = ["OutputFormat", "SortColumns", "InputFormat", "SerdeInfo", "BucketColumns", "Parameters",
                         "Location", "NumberOfBuckets", "StoredAsSubDirectories", "Compressed", "Columns"]

        updated_schema_json = dict((k, schema_json["Table"][k]) for k in main_req if k in schema_json['Table'].keys())
        selected_schema_json = dict((k, updated_schema_json['StorageDescriptor'][k]) for k in seperate_list if
                                    k in updated_schema_json['StorageDescriptor'].keys())
        updated_schema_json["StorageDescriptor"] = selected_schema_json

        # Adding new column list
        updated_schema_json["StorageDescriptor"]["Columns"] = new_column_list

        # changing the location
        updated_schema_json["StorageDescriptor"]["Location"] = new_s3_location
        print("####### - changing glue table location to " + new_s3_location)

        # Write the temp json into temp json file
        temp_json_file = '/home/hadoop/clean/shell-scripts/' + dataset + '_temp_glue.json'
        with open(temp_json_file, 'w') as json_file:
            json.dump(updated_schema_json, json_file)

    def run_msck_repair(self, database_name, table_name, output_loc):
        query_string = "MSCK REPAIR TABLE {0}.{1}".format(database_name, table_name)
        query_start = athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': database_name
            },
            ResultConfiguration={
                'OutputLocation': output_loc,
            }
        )
        qe = athena.get_query_execution(QueryExecutionId=query_start["QueryExecutionId"])
        print(qe["QueryExecution"]['Status']["State"])
        flag_val = True
        while flag_val:
            query_execution = athena.get_query_execution(QueryExecutionId=query_start["QueryExecutionId"])
            print(query_execution["QueryExecution"]['Status']["State"])
            if query_execution["QueryExecution"]['Status']["State"] == "SUCCEEDED":
                flag_val = False
                return "SUCCEEDED"
            elif query_execution["QueryExecution"]['Status']["State"] == "FAILED":
                raise Exception("####### - Failed to perform MSCK repair operation")
            time.sleep(300)

    def run_alter_table_partition(self, glue_database, table_name, partition_list, s3_parent_loc,
                                  query_output_loc):
        print("####### - Running alter table commands for partitions")
        for p_key in partition_list:
            if s3_parent_loc.endswith("/"):
                s3_parent_loc = s3_parent_loc
            else:
                s3_parent_loc = s3_parent_loc + "/"
            query = f"ALTER TABLE {table_name} PARTITION "
            temp, ql = "(", ""
            for key, val in p_key.items():
                temp += key + "='" + val + "', "
                ql += key + "=" + val + "/"
            temp = temp.rstrip(', ')
            ql = ql.rstrip('/')
            s3_loc = "'" + s3_parent_loc + ql + "'"
            temp += ')'
            query += temp
            query = query + " SET LOCATION " + s3_loc
            print(query)
            query_start = athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': glue_database
                },
                ResultConfiguration={
                    'OutputLocation': query_output_loc,
                }
            )
            flag_val = True
            while flag_val:
                qe = athena.get_query_execution(QueryExecutionId=query_start["QueryExecutionId"])
                print(qe["QueryExecution"]['Status']["State"])
                if qe["QueryExecution"]['Status']["State"] == "SUCCEEDED":
                    flag_val = False
                    print("####### - Successfully Updated Partition " + str(p_key) + " for " + table_name)
                elif qe["QueryExecution"]['Status']["State"] == "FAILED":
                    print("####### - Failed to  update Partition " + str(p_key) + " for " + table_name)
                    raise Exception("####### - Failed to  update Partition " + str(p_key) + " for " + table_name)
                time.sleep(10)

    def table_exist_check(self, glue_database, glue_table_name):
        """
            This function is used to check if table is already present on glue or not
        """
        paginator = client.get_paginator('get_tables')
        page_iterator = paginator.paginate(
            DatabaseName=glue_database
        )

        for page in page_iterator:
            table_info = page['TableList']

        table_names = []
        for table in table_info:
            table_names.append(table['Name'])

        if glue_table_name in table_names:
            return True
        else:
            return False

    def delete_table(self, glue_database, glue_table_name):
        """
            This function is used to delete the glue table from given database
        """
        response = client.delete_table(
            DatabaseName=glue_database,
            Name=glue_table_name
        )
