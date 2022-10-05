import requests
import pandas as pd

import pandas as pd
import jinja2 as j2 

# import libraries for sql 
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


response_data = []

Cities = ["Perth", "Sydney"]

df = None

for city in Cities:
    params = {
        "days": "7",
        "q":city
    }

    base_url = "http://api.weatherapi.com/v1/forecast.json?aqi=no&alerts=no"

    headers = {
                "key": "2e6f2448e0854266b1d123656222709"
              }
    response = requests.get(base_url, params=params, headers=headers)
    
    df_city = pd.json_normalize(pd.json_normalize(response.json())["forecast.forecastday"][0])
    df_city['City_Name']=city
    
    if df is None: df = df_city
    else: df = df.append(df_city)


# create connection to the source database 
source_connection_url = URL.create(
    drivername = "postgresql+pg8000", 
    username = "PostgresAdmin",
    password = "v3h9vJ8fG&AqOMNi",
    host = "weather.cgtcb5sorecc.ap-southeast-2.rds.amazonaws.com", 
    port = 5432,
    database = "weather", 
)

source_engine = create_engine(source_connection_url)

df.to_sql(name="raw_forecast", con=source_engine, if_exists="replace", index=False)

## Pulling Historic

response_data = []

Cities = ["Perth", "Sydney"]

df = None

for i in range(100):
    
    a = datetime.datetime.now().date() - datetime.timedelta(days = i)

    date = a.strftime("%Y-%m-%d")

    for city in Cities:
        params = {
            "dt": date,
            "q":city
        }

        base_url = "http://api.weatherapi.com/v1/history.json"

        headers = {
                    "key": "2e6f2448e0854266b1d123656222709"
                  }
        print(date)
        response = requests.get(base_url, params=params, headers=headers)

        df_city = pd.json_normalize(pd.json_normalize(response.json())["forecast.forecastday"][0])
        df_city['City_Name']=city

        if df is None: df = df_city
        else: df = df.append(df_city)
