#This GIST removes all elements from a DynamoDB table in a batch format (25 items to remove per batch as its the max batch size allowed by DynamoDB).
import boto3
from boto3.dynamodb.conditions import Key


def truncate_table(profile_name, table_name):
    session = boto3.Session(profile_name=profile_name, region_name='eu-central-1')
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)

    def delete_items(items):
        with table.batch_writer() as batch:
            for item in items:
                key = {'pk': item['pk'], 'sk': item['sk']}  # Replace 'pk' and 'sk' with your partition and sort key attribute names
                batch.delete_item(Key=key)

    # Scan the table to get all items and handle pagination
    scan_kwargs = {}
    done = False

    while not done:
        response = table.scan(**scan_kwargs)
        data = response['Items']
        
        if 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            done = True

        # Delete items in batches
        batch_size = 25  # DynamoDB batch_write supports a maximum of 25 items per request
        for i in range(0, len(data), batch_size):
            print(f"Removing batch {i // batch_size + 1}/{(len(data) + batch_size - 1) // batch_size}")
            delete_items(data[i:i+batch_size])
