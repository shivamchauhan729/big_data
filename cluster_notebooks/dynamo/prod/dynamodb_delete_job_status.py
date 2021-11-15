import logging
import boto3
from botocore.exceptions import ClientError

if __name__== '__main__':
    logging.getLogger("").setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

    TABLE_NAME = "dataset_run_execution"

    dynamodb = boto3.resource('dynamodb', region_name="us-west-2")
    table = dynamodb.Table(TABLE_NAME)
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = \
            table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])


    for i in range(len(data)):
        pk = data[i]['job_key']
        try:
            response = table.delete_item(
                        Key={
                            'job_key': pk
                        },
                        ConditionExpression="job_status = :job_status",
                        ExpressionAttributeValues={
                            ":job_status": "IN PROGRESS"
                        }
                    )
            
        except ClientError as e:  
            if e.response['Error']['Code']=='ConditionalCheckFailedException':  
                print(e.response['Error']) 

    logging.info("Processing Complete")
    logging.shutdown()