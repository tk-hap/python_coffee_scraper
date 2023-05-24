import requests
import pandas as pd

url = "https://slowcoffeeroasters.org/store-1?format=json-pretty"
url_content = requests.get(url)
# Turn request into json
products_json = url_content.json()


products_df = pd.DataFrame.from_dict(products_json['items'])


products_df.to_csv('test-slow.csv')