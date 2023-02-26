#THIS LAMBDA NEEDS TO BE PACKAGED INTO A ZIP LOCALLY, REQUIRES REQUESTS PACKAGE

import json
import requests
import boto3

session = boto3.Session()
s3 = session.resource('s3')
bucket_name = 'python-coffee-img'
bucket = s3.Bucket(bucket_name)
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    for record in event["Records"]:
        if record['eventName'] == 'INSERT':
            print(f"Record = {record}")
            image_url = record['dynamodb']['NewImage']['images']['S']
            roaster = record['dynamodb']['NewImage']['vendor']['S']
            id = record['dynamodb']['NewImage']['id']['S']
            key = roaster + "/" + id
            r = requests.get(image_url, stream=True)
            bucket.upload_fileobj(r.raw, key)
            
            message = { 'id': id, 'vendor': roaster, 'image': "https://python-coffee-img.s3.amazonaws.com/" + key},
            sqs.send_message(
                QueueUrl = "https://sqs.ap-southeast-2.amazonaws.com/373205127336/coffee-img-queue",
                MessageBody = json.dumps(message)
            )
            print(f"{image_url} was downloaded")
  