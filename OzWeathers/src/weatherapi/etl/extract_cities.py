import requests
import pandas as pd

pd.options.mode.chained_assignment = None

def extract_cities():

    url = "https://countriesnow.space/api/v0.1/countries/population/cities/filter"

    payload = {"limit": 30, "order":"dsc","orderBy": "populationCounts","country": "australia"}
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code== 200:
        respond = response.json()

        df_cities_resp = pd.json_normalize(respond['data'], 'populationCounts',['city', 'country'])

        df_cities_unclean = df_cities_resp[~df_cities_resp['city'].str.contains(' |-')]

        df_cities = df_cities_unclean[df_cities_unclean['year'].str.contains(df_cities_unclean['year'].max())]

        df_cities.reset_index(drop=True, inplace=True)
        
        return df_cities.head(10)
    
    else: 
        print(response)
        


