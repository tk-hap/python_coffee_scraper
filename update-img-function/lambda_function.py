import boto3
import json

table = boto3.resource('dynamodb').Table('coffee_table')

def lambda_handler(event, context):
    for record in event['Records']:

        payload = record["body"]
        print(payload)
        payload_json = json.loads(payload)
        print(payload_json)
        vendor = payload_json[0]["vendor"]
        id = payload_json[0]["id"]
        image = payload_json[0]["image"]
        
        table.update_item(
            Key={'vendor': vendor, 'id': id},
            UpdateExpression='SET images = :newImage',
            ExpressionAttributeValues={
                ':newImage': image,
                
            }
            )
