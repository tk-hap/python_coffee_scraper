from turtle import st
from numpy import int64
import requests
import pandas as pd
import sqlite3
import re

COFFEE_SITES = ['https://folkbrewers.co.nz/products.json', 'https://greyroastingco.com/products.json', 'https://redrabbitcoffee.co.nz/products.json']
products_json = {}
products_all = pd.DataFrame()

db = sqlite3.connect('test_database')
c = db.cursor()
db.commit()

def get_json(url):
    """ 
    Takes URL and returns JSON data from Shopify products page.

    Parameters:
    url (str): A website URL ie. https://thisisaurl.com/products.json

    Returns:
    products_json (dict): Dictionary containing JSON data from the URL
    """
    # Get data from url using requests
    url_content = requests.get(url)
    # Turn request into json
    products_json = url_content.json()
    return products_json

def create_df(products_json):
    """
    Converts JSON dictionary to a pandas dataframe

    Parameters:
    products_json (dict): Dictionary containing JSON Shopify products data

    Returns:
    products_df (dataframe): Pandas dataframe containing the JSON Shopify products 
    """
    products_df = pd.DataFrame.from_dict(products_json['products'])
    return products_df

def filter_df(products_all):
    """
    Filters products dataframe to only include individual coffee products

    Parameters:
    products_all (dataframe): Unfiltered dataframe containing all Shopify products

    Returns:
    coffee_df (dataframe): Filtered dataframe
    
    """
    # Conditions to filter data from dataframes
    cond_coffee = products_all['product_type'] == 'Coffee'
    cond_notSub = products_all['title'].str.contains('Subscription') == False
    coffee_df = products_all[cond_coffee & cond_notSub]
    return coffee_df


for url in COFFEE_SITES:
    products_json = get_json(url)
    products_df = create_df(products_json)
    products_all = pd.concat([products_all, products_df], ignore_index=True)
    

coffee_df = filter_df(products_all)
# Converts total dataframe to str
coffee_df = coffee_df.astype(str)

# Converts the ID field back to an int
coffee_df['id'] = coffee_df['id'].astype(int64)

coffee_df.to_sql('coffee', db, if_exists='replace', index=False)
pd.read_sql('select * from coffee', db)


db.close()



