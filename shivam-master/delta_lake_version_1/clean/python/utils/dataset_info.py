# ######################################Module Information####################################
#  Module Name         : dataset_info                                                        #
#  Purpose             : For getting Cut-off date for hitting API                            #
#  Input Parameters    : dynamodb table, cut-off start and end date, job_key, layer, source  #
#  Output Value        : Response will update in DynamoDB table                              #
# ############################################################################################

# Library and external modules declaration
from datetime import datetime
import boto3


class DatasetInfo(object):
    """
        This class contains functions for inserting and updating the dynamodb table
        for handling cutoff date for rest apis.
        To re-use this module, dataset_info_table name ,start_datetime, dataset_key,
        job_name,sla, run_mode,is_cross_account, cross_account_role_arn should be passed
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

    def get_dataset(self, dataset_info_table, dataset_key,
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
            return response

        except Exception as e:
            print(f"####### - Exception Occurred while reading getting "
                  f"dataset info from DynamoDB :  {str(e)}")

            raise e

    def update_dataset(self, dataset_info_table, dataset_key, run_start_ts,
                       next_run_watermark_ts, is_cross_account, cross_account_role_arn, sla, run_mode, data_location):
        """
        This function is used for update the record in dynamodb table
        once cleansed process completes.
        """
        try:
            current_time = datetime.now()
            last_update_ts = current_time.strftime("%Y-%m-%d %H:%M:%S")

            dynamodb = self.get_dynamodb_resource(is_cross_account,
                                                  cross_account_role_arn)

            dynamo_table = dynamodb.Table(dataset_info_table)
            response = dynamo_table.update_item(
                Key={
                    'dataset_key': dataset_key
                },
                UpdateExpression="set run_start_ts =:csts, run_end_ts =:cets , \
                                  watermark_ts =:wts, sla =:sl, last_update_ts =:uts, \
                                  run_mode =:rm , data_location =:dl",
                ExpressionAttributeValues={
                    ':csts': run_start_ts,
                    ':cets': next_run_watermark_ts,
                    ':wts': next_run_watermark_ts,
                    ':sl': sla,
                    ':uts': last_update_ts,
                    ':rm': run_mode,
                    ':dl': data_location
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"DynamoDB Update record response : {str(response)}")
            return response
        except Exception as e:
            print(f"Exception Occurred while updating latest "
                  f"timestamp in DynamoDB : {str(e)}")
            raise e
