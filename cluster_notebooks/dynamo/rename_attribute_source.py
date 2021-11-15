import boto3

TABLE_NAME = "dataset_run_execution"

dynamodb = boto3.resource('dynamodb', region_name="us-west-2")
table = dynamodb.Table(TABLE_NAME)
response = table.scan()
data = response['Items']

row_index = 0
for i in range(len(data)):
    pk = data[row_index]['job_key']
    print(pk)
    old_source = data[row_index]['source']
    response = table.update_item(
        Key={
            'job_key':pk
        },
        UpdateExpression="set sources=:sources", 
        ExpressionAttributeValues={
            ':sources': old_source
        },
        ReturnValues="UPDATED_NEW")
    row_index+=1
    print(row_index)



dynamodb = boto3.resource('dynamodb', region_name="us-west-2")
table = dynamodb.Table(TABLE_NAME)
response = table.scan()
data = response['Items']

for i in range(len(data)):
    pk = data[i]['job_key']
    table.update_item(
        Key={
            'job_key': pk
        },
        UpdateExpression='REMOVE sources'
    )