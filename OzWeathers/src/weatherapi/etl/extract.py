from datetime import timedelta, datetime as dt
from boto3 import client
import pandas as pd
import requests
import logging
import time

from yaml import load

pd.options.mode.chained_assignment = None

class Extract():

    def __init__(self, table_name:str, cities_api_url:str=None, cities_api_country:str=None, weather_api_url:str=None, weather_api_key:str=None, aws_bucket:str=None, aws_access_id:str=None, aws_secrect_key:str=None, aws_region:str=None, aws_log_file:str=None):
        self.table_name = table_name        
        self.cities_api_url = cities_api_url
        self.cities_api_country = cities_api_country
        self.weatehr_api_url = weather_api_url
        self.weather_api_key = weather_api_key   
        self.aws_bucket = aws_bucket
        self.aws_access_id = aws_access_id
        self.aws_secret_key = aws_secrect_key
        self.aws_region = aws_region
        self.aws_log_file = aws_log_file
        
    def _extract_cities(self, url, country):
        
        url = url

        payload = {"limit":30, "order":"dsc", "orderBy":"populationCounts", "country":country}
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
            logging.error(response)

    def _extract_weather_historic(self, cities_api_url, country, weather_api_url, weather_api_key, loaddate)->pd.DataFrame:

        df_historic = pd.DataFrame()
        get_cities = self._extract_cities(url=cities_api_url, country=country)
        cities = get_cities['city'].tolist()
        today_date = dt.now()
        # today_date = dt.strptime('2022-10-14', "%Y-%m-%d")
        if loaddate is None:
            set_loaddate = today_date - timedelta(days = 2)
            logging.info(f"Performing full extract from {set_loaddate}")
        else:
            set_loaddate = dt.strptime(loaddate, "%Y-%m-%d") + timedelta(days = 1)
            logging.info(f"Performing extract from {set_loaddate}")

        while set_loaddate.date() < today_date.date():

            date_string = set_loaddate.strftime("%Y-%m-%d")
            for city in cities:
                params = {
                    "dt": date_string,
                    "q":city
                }

                base_url = weather_api_url

                headers = {
                            "key": weather_api_key
                        }

                response = requests.get(base_url, params=params, headers=headers)
                time.sleep(0.1)      

                df_city = pd.json_normalize(pd.json_normalize(response.json())["forecast.forecastday"][0])
                df_city['City_Name']=city
                if df_historic.empty: 
                    df_historic = df_city
                else: 
                    df_historic = pd.concat((df_historic, df_city), axis = 0)
            
            # Increment date for next api call
            set_loaddate = set_loaddate + timedelta(days = 1)

        return df_historic

    #this extract forecast data for the next 7 days
    def _extract_weather_forecast(self, cities_api_url, country, weather_api_url, weather_api_key):
        
        df_forecast = None
        get_cities = self._extract_cities(url=cities_api_url, country=country)
        cities = get_cities['city'].tolist()
        base_url = weather_api_url
        headers = {
                    "key": weather_api_key
                }
        
        for city in cities:
            params = {
                "days": "7",
                "q":city
            }

            response = requests.get(base_url, params=params, headers=headers)
            df_city = pd.json_normalize(pd.json_normalize(response.json())["forecast.forecastday"][0])
            df_city['City_Name']=city
            if df_forecast is None: 
                df_forecast = df_city

            else: 
                df_forecast = pd.concat((df_forecast, df_city), axis = 0)

        return df_forecast

    """
    We get our last updated and incremental value log files form S3 Bucket. 
    """
    
    def _get_incremental_value(self, aws_bucket, aws_access_id, aws_secret_key, aws_region, aws_log_file):
        
        s3 = client('s3', 
            aws_access_key_id = aws_access_id,
            aws_secret_access_key= aws_secret_key,
            region_name= aws_region   
            )
        response = s3.get_object(Bucket=aws_bucket, Key=aws_log_file)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if status == 200:
            
            logging.info(f"Successfully request and read from S3. Status - {status}")
            get_dates = pd.read_csv(response.get("Body"))
            try:
                latest_date = get_dates[get_dates["incremental_value"]== get_dates["incremental_value"].max()]["incremental_value"].values[0]
                return latest_date
            except:
                return None

        else:
            logging.error(f"Request to get object from S3 has failed. Status - {status}")

    def _upsert_incremental_log(self, aws_bucket, aws_access_id, aws_secret_key, aws_region, aws_log_file, incremental_value)->bool:
        
        s3 = client('s3', 
            aws_access_key_id=aws_access_id,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region   
            )
        response = s3.list_objects_v2(Bucket=aws_bucket)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if status == 200:
            logging.info(f"Successfully request and list objects from S3. Status - {status}")
            for i in range(len(response['Contents'])):
                if aws_log_file == response['Contents'][i]['Key']:
                    try:
                        get_that_file = s3.get_object(Bucket=aws_bucket, Key=aws_log_file)
                        df_existing_log = pd.read_csv(get_that_file.get("Body"))
                        df_incremental_log = pd.DataFrame(data={
                            "log_date":[dt.now().strftime("%Y-%m-%d")], 
                            "incremental_value": [incremental_value]})
                        df_updated_incremental_log = pd.concat([df_existing_log,df_incremental_log])
                        
                        csv_file = aws_log_file.split('/')[1]
                        df_updated_incremental_log.to_csv(csv_file, index=False)                       
                        s3.upload_file(csv_file, Bucket=aws_bucket, Key=aws_log_file)
                        logging.info(f"{aws_log_file} has been updated on S3.")

                    except:
                            logging.info(f"Request to get object from S3 has failed. Status - {status}")
                                
        else:
            logging.error(f"Request to list object from S3 has failed. Status - {status}")

        return True

    def run(self)->pd.DataFrame:
        
        try:
            if self.table_name == "raw_cities":
                df = self._extract_cities(url=self.cities_api_url, country=self.cities_api_country)
                logging.info(f"Successfully extracted table: {self.table_name}, rows extracted: {len(df)}")

            elif self.table_name == "raw_forecast":
                df= self._extract_weather_forecast(cities_api_url=self.cities_api_url, country=self.cities_api_country, weather_api_url=self.weatehr_api_url, weather_api_key=self.weather_api_key)
                logging.info(f"Successfully extracted table: {self.table_name}, rows extracted: {len(df)}")
                
            elif self.table_name =="raw_historic":                
                latest_date = self._get_incremental_value(aws_bucket=self.aws_bucket, aws_access_id=self.aws_access_id, aws_secret_key=self.aws_secret_key, aws_region=self.aws_region, aws_log_file=self.aws_log_file)
                extracted = self._extract_weather_historic(cities_api_url=self.cities_api_url, country=self.cities_api_country, weather_api_url=self.weatehr_api_url, weather_api_key=self.weather_api_key, loaddate=latest_date)
                df = extracted
                
                if len(df) > 0:

                    incremental_date = str(df["date"][df["date"]== df["date"].max()].values[0])
                    self._upsert_incremental_log(aws_bucket=self.aws_bucket, aws_access_id=self.aws_access_id, aws_secret_key=self.aws_secret_key, aws_region=self.aws_region, aws_log_file=self.aws_log_file, incremental_value=incremental_date)
                    logging.info(f"Successfully extracted table: {self.table_name}, rows extracted: {len(df)}")

            else:
                logging.error(f"An error has occured. {self.table_name} not found.")

            return df
        
        except:
            logging.error(f"An error has occured. {self.table_name} not found.")


