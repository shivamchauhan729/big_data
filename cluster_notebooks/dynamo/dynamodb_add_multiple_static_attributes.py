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


    row_index = 0
    for i in range(len(data)):
        pk = data[row_index]['dataset_key']
        print(pk)
        response = table.update_item(
            Key={
                'dataset_key':pk
            },
            UpdateExpression="set data_tier=:data_tier, aws_account_name=:aws_account_name, database_name=:database_name, table_name=:table_name, data_producer=:data_producer", 
            ExpressionAttributeValues={
                ':data_tier': 3,
                ':aws_account_name': 'Acquisitions Data Lab',
                ':database_name' : "null",
                ':table_name' : "null",
                ':data_producer': "GDE"
            },
            ReturnValues="UPDATED_NEW")
        row_index+=1
        print(row_index)

    logging.info("Processing Complete")
    logging.shutdown()

