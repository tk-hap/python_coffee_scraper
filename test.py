import boto3
from boto3.dynamodb.conditions import Key

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table('coffee_table_test')

#Get all products by vendor
all_vendor_products = table.query(KeyConditionExpression=Key('PK').eq('Red Rabbit Co.') & Key('SK').begins_with('#ID#'))

#Get all roasters
all_roasters = table.query(KeyConditionExpression=Key('PK').eq('#ROASTER'))

print(all_vendor_products)

# Need Get all products by region, Get all Products
