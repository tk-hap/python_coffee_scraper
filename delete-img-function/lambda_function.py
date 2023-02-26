import boto3

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    for record in event["Records"]:
        print(f"Record = {record}")
        image_url = record['dynamodb']['OldImage']['images']['S']
        s3_key = image_url.split('.com/')[1]
        s3.Object('python-coffee-img', s3_key).delete()
        
        print(f"{s3_key} was deleted")