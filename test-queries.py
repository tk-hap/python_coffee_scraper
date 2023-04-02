import boto3
from flask import Flask, render_template
from werkzeug.exceptions import abort
from boto3.dynamodb.conditions import Key
dynam_sess = boto3.Session(profile_name='tk-personal')
dynamodb_resource = dynam_sess.resource('dynamodb')
table = dynamodb_resource.Table('coffee_table')

product_vendor = "ROASTER#Red Rabbit Coffee Co."
product_id = "ID#6543747252322"
    

# product = table.query(KeyConditionExpression=Key('pk').eq(product_vendor) & Key('sk').eq(product_id))['Items']
response = table.query(
    IndexName='latest-index',
    KeyConditionExpression=Key('pk').eq({"SS":["ROASTER#Red Rabbit Coffee Co.", "Grey Roasting Co"]}),
    Limit=1,
    ScanIndexForward= False
    )['Items']




print(response)
