# POWERSHELL TO RUN FLASK APP
#(venv) $env:FLASK_APP = 'web_app.py'
#(venv) $env:FLASK_ENV = 'development'
#(venv)  flask run
import boto3
from flask import Flask, render_template
from werkzeug.exceptions import abort

dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table('coffee_table')

app = Flask(__name__)
 
def get_product(product_vendor, product_id):
    product = table.get_item(Key={'id':product_id, 'vendor':product_vendor})['Item']
    if product is None:
        abort(404)
    return product


@app.route('/')
def index():
    coffee_products = table.scan()
    return render_template('index.html', coffee_products=coffee_products)

@app.route('/<product_vendor>/<int:product_id>')
def single_product(product_vendor, product_id):
    product = get_product(product_vendor, product_id)
    return render_template('product.html', product=product)
