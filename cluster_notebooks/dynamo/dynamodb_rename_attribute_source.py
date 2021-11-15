#!/usr/bin/env python3

""" Rename an AWS dynamodb table attribute with an exponential timing back-off
"""

import logging
from time import sleep

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

AWS_RESOURCE = 'dynamodb'
AWS_REGION = 'us-west-2'


def _replace_attribute(item, attribute_from, attribute_to):
    """Replace a single item attribute"""

    item[attribute_to] = item[attribute_from]
    del item[attribute_from]


def _rename_attribute(dyn_table, attribute_from, attribute_to, pause_time):
    """Rename dyn_table.attribute_from to dyn_table.attribute_to"""

    batch = dyn_table.batch_writer()

    scan_filter = Attr(attribute_from).exists()

    response = dyn_table.scan(
        FilterExpression=scan_filter
    )

    sleep(pause_time)

    for item in response['Items']:
        _replace_attribute(item, attribute_from, attribute_to)
        batch.put_item(Item=item)

    while 'LastEvaluatedKey' in response:

        response = dyn_table.scan(
            FilterExpression=scan_filter,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )

        sleep(pause_time)

        for item in response['Items']:
            _replace_attribute(item, attribute_from, attribute_to)
            batch.put_item(Item=item)


def table_rename_attribute(dyn_table, attribute_from, attribute_to):
    """ Rename dyn_table.attribute_from to dyn_table.attribute_to
        Catch quota exceptions, backoff, and retry as required
    """

    retry_exceptions = ('ProvisionedThroughputExceededException',
                        'ThrottlingException')
    retries = 0
    pause_time = 0

    while True:
        try:
            _rename_attribute(dyn_table, attribute_from, attribute_to, pause_time)
            break
        except ClientError as err:
            if err.response['Error']['Code'] not in retry_exceptions:
                raise
            pause_time = (2 ** retries)
            logging.info('Back-off set to %d seconds', pause_time)
            retries += 1


if __name__ == "__main__":

    logging.getLogger("").setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

    DB = boto3.resource(AWS_RESOURCE, region_name=AWS_REGION)

    table_rename_attribute(DB.Table('dataset_run_execution'), 'source', 'sources')

    logging.info("Processing Complete")
    logging.shutdown()