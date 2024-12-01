#This GIST reads data from a DynamoDB tables and changes it's status. It applies these updates using batches (of 25 items) to decrease the number of requests performed to DynamoDB and to save money (25 is the max number of items in a batch).
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime

def update_dynamodb_table(profile_name, table_name, index_name, priority_value, sts_prefix, new_status):
    session = boto3.Session(profile_name=profile_name, region_name='eu-central-1')
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    items = read_dynamodb_table(profile_name, table_name, index_name, priority_value, sts_prefix)
    updated_items = []
    
    for item in items:
        current_status = item.get('sts', '').split('#')[0]
        if current_status.upper() != 'UPDATED':
            current_timestamp = int(datetime.now().timestamp() * 1000)
            new_status_with_timestamp = f'{new_status}#{current_timestamp}'
            updated_items.append({
                'pk': item['pk'],
                'sk': item['sk'],
                'new_status_with_timestamp': new_status_with_timestamp
            })
    return updated_items

def batch_update(items, table_name, dynamodb, batch_size=25):
    """
    Updates items in DynamoDB in batches.
    """
    table = dynamodb.Table(table_name)

    for i in range(0, len(items), batch_size):
        print(f"Updating batch {i}/{len(items)}")
        batch = items[i:i + batch_size]
        
        with table.batch_writer() as batch_writer:
            for item in batch:
                try:
                    table.update_item(
                        Key={
                            'pk': item['pk'],
                            'sk': item['sk']
                        },
                        UpdateExpression='SET sts = :new_status, details = :details',
                        ExpressionAttributeValues={
                            ':new_status': item['new_status_with_timestamp'],
                            ':details': {
                                'days': 60,
                                'maxPosts': 150
                            }
                        }
                    )
                except Exception as e:
                    print(f"Failed to update item {item['pk']}: {e}")
