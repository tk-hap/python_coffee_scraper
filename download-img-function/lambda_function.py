import json
import requests
import boto3

session = boto3.Session()
s3 = session.resource('s3')
bucket_name = 'python-coffee-img'
bucket = s3.Bucket(bucket_name)

def lambda_handler(event, context):
    for record in event["Records"]:
        if record['eventName'] == 'INSERT':
            image_url = record['dynamodb']['NewImage']['image']['S']
            roaster = record['dynamodb']['NewImage']['vendor']['S']
            id = record['dynamodb']['NewImage']['id']['N']
            key = roaster + "/" + id
            r = requests.get(image_url, stream=True)
            
            bucket.upload_fileobj(r.raw, key)
            print(f"{image_url} was downloaded")

# TO DO: Create event filter for "INSERT" events only. Put new image location into sqs for a lambda to update dynamo db. Connect to dynamodb streams Document better :)