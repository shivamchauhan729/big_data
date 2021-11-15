import logging
import boto3

if __name__== '__main__':
    logging.getLogger("").setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

    TABLE_NAME = "dataset_info_dev"

    dynamodb = boto3.resource('dynamodb', region_name="us-west-2")
    table = dynamodb.Table(TABLE_NAME)
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = \
            table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    for i in range(len(data)):
        pk = data[i]['dataset_key']
        response = table.update_item(
                Key={
                    'dataset_key' : pk
                },
                UpdateExpression="set data_tier=:data_tier",
                ExpressionAttributeValues={
                    ':data_tier': 'Tier 3'
                },
                ReturnValues="UPDATED_NEW"
            )

    logging.info("Processing Complete")
    logging.shutdown()