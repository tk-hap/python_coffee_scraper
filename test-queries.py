from numpy import int64
import requests
import pandas as pd
import awswrangler as wr
import boto3
import pycountry
import datetime
import json
import ast
# ='Subscription'
#Create a class called roaster website
class RoasterWebsite:
    def __init__(self, url, vendor, product_section, convert_timestamp, category_column, category_conditions, country_column, title_column, title_conditions, columns):
        self.url = url
        self.vendor = vendor
        self.product_section = product_section
        self.convert_timestamp = convert_timestamp
        self.category_conditions = ast.literal_eval(category_conditions)
        self.country_column = country_column
        self.category_column = category_column
        self.title_column = title_column
        self.title_conditions = title_conditions
        self.columns = columns
        # self.product_type = product_type
        # self.image_url = image_url
        # self.region_txt = region_txt

    def timestamp_to_epoch(timestamp):
        dt = datetime.datetime.fromisoformat(timestamp)
        return int(dt.timestamp())
    
    def get_region(self, row):
        found = False
        for country in pycountry.countries:
            if country.name.lower() in row[self.country_column].lower():
                found = True
                return country.name
        if not found:
            return 'Other'
            # elif country.name.lower() not in row[self.country_column].lower():
            #     return "Other"
        
    def json(self):
        """ 
            Takes URL and returns JSON data from Shopify products page.


            Parameters:
            url (str): A website URL ie. https://thisisaurl.com/products.json

            Returns:
            products_json (dict): Dictionary containing JSON data from the URL

            """
        # Get data from url using requests
        url_content = requests.get(self.url)

        # Turn request into json
        products_json = url_content.json()
        return products_json
    
    def products_df(self):
        """
        Converts JSON dictionary to a pandas dataframe

        Parameters:
        products_json (dict): Dictionary containing JSON Shopify products data

        Returns:
        products_df (dataframe): Pandas dataframe containing the JSON Shopify products 
        """
        products_df = pd.DataFrame.from_dict(self.json()[self.product_section])
        return products_df
    
    def filtered_products_df(self):
        """
        Filters products dataframe to only include individual coffee products and remove columns

        Parameters:
        products_all (dataframe): Unfiltered dataframe containing all Shopify products

        Returns:
        coffee_df (dataframe): Filtered dataframe
        
        """
        product_dataframe = self.products_df()
        # Conditions to filter data from dataframes
        cond_coffee = product_dataframe[self.category_column].isin(self.category_conditions)
        cond_not_sub = product_dataframe[self.title_column].str.contains(self.title_conditions) == False
        coffee_df = product_dataframe[cond_coffee & cond_not_sub]

        # Rename columns
        coffee_df.rename(columns = self.columns, inplace=True)

        # Add prefix to keys
        coffee_df['sk'] = 'ID#' + coffee_df['sk'].astype(str)
        coffee_df['pk'] = 'ROASTER#' + self.vendor

        coffee_df = coffee_df[self.columns.values()]
        # Add region to DF
        coffee_df['region'] = coffee_df.apply(self.get_region, axis=1)
        coffee_df['region_tag'] = 'REGION'

        # Extract image url
        coffee_df['images'] = coffee_df['images'].str.extract(r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))", expand=True)[0]
        if self.convert_timestamp == True:
             # Convert published_at timestamp to epoch
            coffee_df['published_at'] = coffee_df['published_at'].apply(self.timestamp_to_epoch)
        
        return coffee_df

# Connect to DynamoDBtk-personal
session = boto3.Session(profile_name='tk-personal')

dynamodb = session.resource('dynamodb', region_name='ap-southeast-2')
table = dynamodb.Table('coffee_table')

# Retrieve the parameters dictionary from DynamoDB
response = table.get_item(Key={'pk':'TEST', 'sk':'ROASTER#Little Drum Coffee'})
item = response['Item']
retrieved_params = item['params']

print(retrieved_params['category_conditions'])

website = RoasterWebsite(
                        url                 =   retrieved_params['url'],
                        vendor              =   retrieved_params['vendor'],
                        product_section     =   retrieved_params['product_section'],
                        convert_timestamp   =   retrieved_params['convert_timestamp'],
                        category_column     =   retrieved_params['category_column'],
                        category_conditions =   retrieved_params['category_conditions'],
                        country_column      =   retrieved_params['country_column'],
                        title_column        =   retrieved_params['title_column'],
                        title_conditions    =   retrieved_params['title_conditions'],
                        columns             =   retrieved_params['columns']
                        )   

        

print(website.filtered_products_df().to_csv('test.csv'))

        
# # Converts total dataframe to str
# coffee_df = coffee_df.astype(str)# Converts the Published_at field back to an int
# coffee_df['published_at'] = coffee_df['published_at'].astype(int64)#extract image url
# coffee_df['images'] = coffee_df['images'].str.extract(r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))", expand=True)[0]
# print(coffee_df.columns)
# coffee_df.to_csv("test.csv")
# wr.dynamodb.put_df(df=coffee_df, table_name="coffee_table")