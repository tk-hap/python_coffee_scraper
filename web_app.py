# POWERSHELL TO RUN FLASK APP
#(venv) $env:FLASK_APP = 'web_app.py'
#(venv) $env:FLASK_ENV = 'development'
#(venv)  flask run

import sqlite3
from flask import Flask, render_template
from werkzeug.exceptions import abort

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('test_database')
    conn.row_factory = sqlite3.Row
    return conn

def get_product(product_id):
    conn = get_db_connection()
    product = conn.execute('SELECT * FROM coffee WHERE id = ?',(product_id,)).fetchone()
    conn.close()
    if product is None:
        abort(404)
    return product


@app.route('/')
def index():
    conn = get_db_connection()
    coffee_products = conn.execute('SELECT * FROM coffee').fetchall()
    conn.close()
    return render_template('index.html', coffee_products=coffee_products)

@app.route('/<int:product_id>')
def single_product(product_id):
    product = get_product(product_id)
    return render_template('product.html', product=product)