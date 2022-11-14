# POWERSHELL TO RUN FLASK APP
#(venv) $env:FLASK_APP = 'web_app.py'
#(venv) $env:FLASK_ENV = 'development'
#(venv)  flask run
import psycopg2
import psycopg2.extras
from flask import Flask, render_template
from werkzeug.exceptions import abort


app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host='web-db-prod-01.cqqsz59wf4te.ap-southeast-2.rds.amazonaws.com',
        dbname='postgres',
        user='postgres',
        password='5a$78Fs#JFt7eG'
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cur

def get_product(product_id):
    cur = get_db_connection()
    cur.execute('SELECT * FROM coffee WHERE id = %s',(product_id,))
    product = cur.fetchone()
    cur.close()
    if product is None:
        abort(404)
    return product


@app.route('/')
def index():
    cur = get_db_connection()
    cur.execute('SELECT * FROM coffee;')
    coffee_products = cur.fetchall()
    cur.close()
    return render_template('index.html', coffee_products=coffee_products)

@app.route('/<int:product_id>')
def single_product(product_id):
    product = get_product(product_id)
    return render_template('product.html', product=product)