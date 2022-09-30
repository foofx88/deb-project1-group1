import requests
import pandas as pd

url = "https://countriesnow.space/api/v0.1/countries/population/cities/filter"

payload = {"limit": 20, "order":"dsc","orderBy": "populationCounts","country": "australia"}
headers = {}

response = requests.request("POST", url, headers=headers, data=payload)

respond = response.json()

df_cities_resp = pd.json_normalize(respond['data'], 'populationCounts',['city', 'country'])

df_cities_unclean = df_cities_resp[~df_cities_resp['city'].str.contains('Greater')]
df_cities_unclean['city'].replace(' ', '-', regex=True, inplace=True)

df_cities = df_cities_unclean[df_cities_unclean['year'].str.contains(df_cities_unclean['year'].max())]

df_cities.reset_index(drop=True, inplace=True)

#to include load to raw_cities