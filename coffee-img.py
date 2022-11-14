import boto3
import requests

url = "https://cdn.shopify.com/s/files/1/0289/0760/1998/products/wp4209745-ethiopia-flag-wallpapers.jpg?v=1646093671"
r = requests.get(url, stream=True)

session = boto3.session()
s3 = session.resource('s3')

bucket_name = 'python-coffee-img'
key = 'test-file'

bucket = s3.Bucket(bucket_name)
bucket.upload_fileobj(r.raw, key)
