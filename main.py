#from turtle import st
from numpy import int64
import requests
import pandas as pd
import awswrangler as wr
import boto3
import pycountry
from  hashlib import blake2b

COFFEE_SITES = ['https://folkbrewers.co.nz/products.json', 'https://greyroastingco.com/products.json', 'https://redrabbitcoffee.co.nz/products.json']
products_json = {}
products_all = pd.DataFrame()
dynam_sess = boto3.Session(profile_name='tk-personal')

def get_json(url):
    """ 
    Takes URL and returns JSON data from Shopify products page.
    Additionally hashes the current data to be added to the dataframe later

    Parameters:
    url (str): A website URL ie. https://thisisaurl.com/products.json

    Returns:
    products_json (dict): Dictionary containing JSON data from the URL
    webpage_hashed (str): A hash created from the webpage, used for comparison
    """
    #Initialize hashing
    hashing = blake2b(digest_size=10)
    # Get data from url using requests
    url_content = requests.get(url)
    #Create hash from url data
    hashing.update(url_content.content)
    webpage_hashed = hashing.hexdigest()
    # Turn request into json
    products_json = url_content.json()
    return products_json, webpage_hashed

def create_df(products_json, webpage_hashed):
    """
    Converts JSON dictionary to a pandas dataframe

    Parameters:
    products_json (dict): Dictionary containing JSON Shopify products data

    Returns:
    products_df (dataframe): Pandas dataframe containing the JSON Shopify products 
    """
    products_df = pd.DataFrame.from_dict(products_json['products'])
    products_df.insert(2, "page_hash", webpage_hashed, True)
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
    return coffee_df

def get_region(row):
    for country in pycountry.countries:
        if country.name.lower() in row['handle'].lower():
            return country.name


for url in COFFEE_SITES:
    products_json, webpage_hashed = get_json(url)
    products_df = create_df(products_json, webpage_hashed)
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
wr.dynamodb.put_df(df=coffee_df, table_name="coffee_table", boto3_session=dynam_sess)




