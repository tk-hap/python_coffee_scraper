import boto3
from flask import Flask, render_template
from werkzeug.exceptions import abort
from boto3.dynamodb.conditions import Key
dynam_sess = boto3.Session(profile_name='default')
dynamodb_resource = dynam_sess.resource('dynamodb')
table = dynamodb_resource.Table('coffee_table')




pk_values = ["Red Rabbit Coffee Co.", "Grey Roasting Co"]
items = []

all_vendors = table.query(KeyConditionExpression=Key('pk').eq('ROASTER'))['Items']



roaster_values = [x['sk'] for x in all_vendors]
latest_products = []
for roaster in roaster_values:
        response_batch = table.query(
            IndexName='latest-index',
            KeyConditionExpression=Key('pk').eq(roaster),
            Limit= 1,
            ScanIndexForward = False
            
        )
    
        latest_products.append(response_batch['Items'])
print(latest_products)