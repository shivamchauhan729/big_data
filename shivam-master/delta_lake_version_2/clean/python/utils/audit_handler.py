#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Godaddy'

#######################################Module Information####################################
#  Module Name         : AuditHandler                                                       #
#  Purpose             : To store audit all the information                                 #
#  Input Parameters    : audit_table name ,start_datetime, job_key, job_name                #
#  Output Value        : Response will update in DynamoDB table                             #
#############################################################################################


# Library and external modules declaration
from datetime import datetime
import boto3


class AuditHandler(object):
    """
        This class contains functions for inserting and updating the dynamodb audit table.
        To re-use this module, audit_table name ,start_datetime, job_key, job_name should be passed
        from parent program(wherever we call this function).
    """
    def get_dynamodb_resource(self, is_cross_account, cross_account_role_arn):
        if is_cross_account:
            client = boto3.client('sts')
            sts_response = client.assume_role(RoleArn=cross_account_role_arn,
                                              RoleSessionName='AssumePocRole',
                                              DurationSeconds=900)
            return boto3.resource(
                service_name='dynamodb',
                region_name='us-west-2',
                aws_access_key_id=sts_response['Credentials']['AccessKeyId'],
                aws_secret_access_key=sts_response['Credentials']['SecretAccessKey'],
                aws_session_token=sts_response['Credentials']['SessionToken'])

        else:
            return boto3.resource(service_name='dynamodb',
                                  region_name='us-west-2',
                                  endpoint_url="https://dynamodb.us-west-2.amazonaws.com")

    def get_dataset_dataset_id(self, dataset_info_table, dataset_key,
                           is_cross_account, cross_account_role_arn):
        try:
            dynamodb = self.get_dynamodb_resource(is_cross_account,
                                                  cross_account_role_arn)
            dynamo_table = dynamodb.Table(dataset_info_table)
            response = dynamo_table.get_item(
                Key={
                    'dataset_key': dataset_key
                }
            )

            print(response)
            return response['Item']['dataset_id']

        except Exception as e:
            print(f"####### - Exception Occurred while reading getting dataset_id dataset info from DynamoDB :  {str(e)}")

            raise e


    def audit_input_json_generator(self, dataset, c_date, job_key,job_id, audit_job_name, source, layer_name,
                                   job_run_date):
        item_json = {}
        item_json['job_key'] = job_key
       # item_json['job_id'] = job_id
        item_json['job_name'] = audit_job_name
        item_json['dataset'] = dataset
        item_json['source'] = source
        item_json['layer_name'] = layer_name
        item_json['job_run_date'] = job_run_date
        item_json['end_ts'] = ''
        item_json['error_message'] = ''
        item_json['job_status'] = 'IN PROGRESS'
        return item_json

    def insert_audit_info(self, audit_table, c_date, item_json,
                          is_cross_account, cross_account_role_arn):
        """
        This function is used for inserting the record in dynamodb  table when the cleansed process starts for any dataset.
        """
        try:

            start_ts = c_date.strftime("%Y-%m-%d %H:%M:%S")
            item_json.update({"start_ts": start_ts})
            dynamodb = self.get_dynamodb_resource(is_cross_account,
                                                  cross_account_role_arn)
            dynamo_table = dynamodb.Table(audit_table)

            response = dynamo_table.put_item(Item=item_json)
            print("####### - Record inserted successfully in Audit table.")
            return response, item_json["job_key"]

        except Exception as e:
            print("####### - Exception Occurred while inserting a record in DynamoDB : " + str(e))
            raise e

    def update_audit_info(self, audit_table, job_key, is_cross_account, cross_account_role_arn, source_records,
                          target_records, job_status, error_message):
        """
        This function is used for update the record in Audit table once cleansed process completes.
        """
        try:
            current_time = datetime.now()
            end_ts = current_time.strftime("%Y-%m-%d %H:%M:%S")
            dynamodb = self.get_dynamodb_resource(is_cross_account,
                                                  cross_account_role_arn)
            dynamo_table = dynamodb.Table(audit_table)
            response = dynamo_table.update_item(
                Key={
                    'job_key': job_key
                },
                UpdateExpression="set end_ts =:et, source_records =:drc, target_records =:crc, job_status =:st ,error_message =:em",
                ExpressionAttributeValues={
                    ':et': end_ts,
                    ':drc': source_records,
                    ':crc': target_records,
                    ':st': job_status,
                    ':em': error_message
                },
                ReturnValues="UPDATED_NEW"
            )
            print("####### - DynamoDB Update record response : " + str(response))

        except Exception as e:
            print("####### - Exception Occurred while updating latest timestamp in DynamoDB : " + str(e))
            raise e
