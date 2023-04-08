#from turtle import st
from numpy import int64
import requests
import pandas as pd
import awswrangler as wr
import boto3
import pycountry
import datetime
import json

products_json = {}
products_all = pd.DataFrame()

def timestamp_to_epoch(timestamp):
    dt = datetime.datetime.fromisoformat(timestamp)
    return int(dt.timestamp())

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
    Filters products dataframe to only include individual coffee products and remove columns

    Parameters:
    products_all (dataframe): Unfiltered dataframe containing all Shopify products

    Returns:
    coffee_df (dataframe): Filtered dataframe
    
    """
    # Conditions to filter data from dataframes
    cond_coffee = products_all['product_type'] == 'Coffee'
    cond_notSub = products_all['title'].str.contains('Subscription') == False
    coffee_df = products_all[cond_coffee & cond_notSub]

    # Drop columns
    coffee_df.drop(['tags', 'options', 'product_type', 'created_at'], axis=1, inplace=True)

    # Rename columns
    coffee_df.rename(columns = {'vendor':'pk', 'id':'sk'}, inplace=True)

    # Add prefix to keys
    coffee_df['sk'] = 'ID#' + coffee_df['sk'].astype(str)
    coffee_df['pk'] = 'ROASTER#' + coffee_df['pk'].astype(str)

    # Convert published_at timestamp to epoch
    coffee_df['published_at'] = coffee_df['published_at'].apply(timestamp_to_epoch)

    return coffee_df

def get_region(row):
    for country in pycountry.countries:
        if country.name.lower() in row['handle'].lower():
            return country.name

def lambda_handler(event, context):
    for record in event['Records']:
        global products_all
        #Get URL
        payload = record["body"]
        payload_json = json.loads(payload)
        url = payload_json[0]["url"]

        products_json = get_json(url)
        products_df = create_df(products_json)
        products_all = pd.concat([products_all, products_df], ignore_index=True)
        

    coffee_df = filter_df(products_all)
    # Add region to DF
    coffee_df['region'] = coffee_df.apply(get_region, axis=1)
    coffee_df['region_tag'] = 'REGION'
    # Converts total dataframe to str
    coffee_df = coffee_df.astype(str)

    # Converts the Published_at field back to an int
    coffee_df['published_at'] = coffee_df['published_at'].astype(int64)

    #extract image url
    coffee_df['images'] = coffee_df['images'].str.extract(r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))", expand=True)[0]
    print(coffee_df.columns)
    # coffee_df.to_csv("test.csv")
    wr.dynamodb.put_df(df=coffee_df, table_name="coffee_table")
