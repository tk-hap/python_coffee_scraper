# POWERSHELL TO RUN FLASK APP
#(venv) $env:FLASK_APP = 'web_app.py'
#(venv) $env:FLASK_ENV = 'development'
#(venv)  flask run
import boto3
from flask import Flask, render_template
from werkzeug.exceptions import abort
from boto3.dynamodb.conditions import Key

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table('coffee_table')

app = Flask(__name__)
 
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
    coffee_products = table.scan()
    return render_template('index.html', coffee_products=coffee_products)

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

@app.route('/regions/<product_region>')
def region_products(product_region):
    all_products = get_region_products(product_region)
    return render_template('region_products.html', region_products=all_products)