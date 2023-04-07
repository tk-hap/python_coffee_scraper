from  hashlib import blake2b
import requests
import boto3
from boto3.dynamodb.conditions import Key
import os
import json

os.environ['AWS_PROFILE'] = "tk-personal"
os.environ['AWS_DEFAULT_REGION'] = "ap-southeast-2"


dynamodb_resource = boto3.resource('dynamodb', region_name='ap-southeast-2') 
table = dynamodb_resource.Table('coffee_table')
sqs = boto3.client('sqs')

def get_vendors():
    # Get Roaster records from dynamodb
    all_vendors = table.query(KeyConditionExpression=Key('pk').eq('ROASTER'))['Items']
    return all_vendors

def get_hash(url):
    #Initialize hashing
    hashing = blake2b(digest_size=10)
    # Get data from url using requests
    url_content = requests.get(url)
    #Create hash from url data
    hashing.update(url_content.content)
    webpage_hashed = hashing.hexdigest()
    return webpage_hashed

# Loop through all roasters, if stored hash doesn't match the new hash, delete all existing items and write a message to an sqs queue

for vendor in get_vendors():
    new_hash = get_hash(vendor['url']+"/products.json")
    if vendor['page_hash'] == new_hash:
        continue
    else:
        partition_key_value = vendor['sk']
        response_vendor = table.query(
        KeyConditionExpression='pk = :val',
        ExpressionAttributeValues={
        ':val': partition_key_value
            }
        )
        with table.batch_writer() as batch:
            for item in response_vendor['Items']:
                batch.delete_item(Key={'pk': item['pk'], 'sk': item['sk']})   
        print(f'All items with partition key {partition_key_value} deleted.')
        
        message = { 'url': vendor['url']+"/products.json", 'roaster': vendor['sk']},
        sqs.send_message(
            QueueUrl = "sqs queue",
            MessageBody = json.dumps(message)
        )       
        
        response_update = table.update_item(
            Key={'pk': vendor['pk'], 'sk': vendor['sk']},
            UpdateExpression='SET page_hash = :val',
            ExpressionAttributeValues={
                ':val': new_hash
            }
        )


