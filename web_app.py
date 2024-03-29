# POWERSHELL TO RUN FLASK APP
#(venv) $env:FLASK_APP = 'web_app.py'
#(venv) $env:FLASK_ENV = 'development'
#(venv)  flask run
import boto3
from flask import Flask, render_template
from werkzeug.exceptions import abort
from boto3.dynamodb.conditions import Key
from zappa.handler import lambda_handler
import epsagon

epsagon.init(
  token='2f3130c9-52a3-41ba-b6d4-a2d6fb05456d',
  app_name='CommunityGrounds - Coffee Scraper',
  metadata_only=False,
)
epsagon_handler = epsagon.lambda_wrapper(lambda_handler)

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table('coffee_table')

app = Flask(__name__)

def get_latest_products(roaster_values):
    roasters_list = [x['sk'] for x in roaster_values]
    latest_products = []
    for roaster in roasters_list:
        response_batch = table.query(
            IndexName='latest-index',
            KeyConditionExpression=Key('pk').eq(roaster),   
            Limit= 1,
            ScanIndexForward = False
            
        )
    
        latest_products.extend(response_batch['Items'])
    return latest_products


def get_product(product_vendor, product_id):
    product = table.query(KeyConditionExpression=Key('pk').eq(product_vendor) & Key('sk').eq(product_id))['Items'][0]
    if product is None:
        abort(404)
    return product

def get_vendors():
    all_vendors = table.query(KeyConditionExpression=Key('pk').eq('ROASTER'))['Items']
    if all_vendors is None:
        abort(404)
    return all_vendors

def get_regions():
    all_regions = table.query(
    IndexName='region-index',
    Select='SPECIFIC_ATTRIBUTES',
    ProjectionExpression='#coffee_region',
    ExpressionAttributeNames={ "#coffee_region": "region" },
    KeyConditionExpression=Key('region_tag').eq('REGION')
    )['Items']
    # Remove duplicates
    all_regions = list({ item['region'] : item for item in all_regions}.values())

    if all_regions is None:
        abort(404)
    return all_regions

def get_vendor_products(product_vendor):
    all_products = table.query(KeyConditionExpression=Key('pk').eq(product_vendor))
    if all_products is None:
        abort(404)
    return all_products

def get_region_products(product_region):
    all_region_products = table.query(IndexName='region-index', KeyConditionExpression=Key('region_tag').eq('REGION') & Key('region').eq(product_region))['Items']
    if all_region_products is None:
        abort(404)
    return all_region_products


@app.route('/')
def index():
    latest_products = get_latest_products(get_vendors())
    return render_template('index.html', coffee_products=latest_products)

#Display all vendors (roasters)
@app.route('/roasters')
def vendors():
    all_roasters = get_vendors()
    return render_template('vendors.html', all_roasters=all_roasters)

#Displays all regions
@app.route('/regions')
def regions():
    all_regions = get_regions()
    return render_template('regions.html', all_regions=all_regions)


#Displays a single product
@app.route('/<product_vendor>/<product_id>')
def single_product(product_vendor, product_id):
    product = get_product(product_vendor, product_id)
    return render_template('product.html', product=product)

#Displays all products of a vendor
@app.route('/roasters/<product_vendor>')
def vendor_products(product_vendor):
    all_products = get_vendor_products(product_vendor)
    return render_template('vendor_products.html', vendor_products=all_products)

#Displays all products of a region
@app.route('/regions/<product_region>')
def region_products(product_region):
    all_products = get_region_products(product_region)
    return render_template('region_products.html', region_products=all_products)