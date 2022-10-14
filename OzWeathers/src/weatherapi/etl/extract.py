import requests
from boto3 import client
import pandas as pd
import logging 
from datetime import timedelta, datetime as dt
import time

pd.options.mode.chained_assignment = None

class Extract():

    # def __init__(self,table_name, engine, key):
    #     self.engine = engine
    #     self.table_name = table_name
    #     self.key = key

    # @staticmethod
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

#this extract forecast data for the next 7 days
    def extract_weather_forecast(key):
        df_forecast = None
        get_cities = Extract.extract_cities()
        cities = get_cities['city'].tolist()
        base_url = "http://api.weatherapi.com/v1/forecast.json?aqi=no&alerts=no"
        headers = {
                    "key": key
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

    def extract_weather_historic(key, loaddate):
        df_historic = None
        get_cities = Extract.extract_cities()
        cities = get_cities['city'].tolist()
        if loaddate is None:
            set_loaddate = dt.now().date() - timedelta(days = 2)
            logging.info(f"Performing full extract from {set_loaddate}")
        else:
            set_loaddate = dt.strptime(loaddate, "%Y-%m-%d") + timedelta(days = 1)
            logging.info(f"Performing extract from {loaddate}")

        i = 0
        while set_loaddate < dt.now().date():
            print(set_loaddate)
            print('----------------------')  
            print(dt.now().date())
            print('----------------------')      

            # get_time = set_loaddate + timedelta(days = i)            
            # date = get_time.strftime("%Y-%m-%d")
            date = set_loaddate.strftime("%Y-%m-%d")
            for city in cities:
                params = {
                    "dt": date,
                    "q":city
                }

                base_url = "http://api.weatherapi.com/v1/history.json"

                headers = {
                            "key": key
                        }
                response = requests.get(base_url, params=params, headers=headers)
                time.sleep(0.1)
                

                df_city = pd.json_normalize(pd.json_normalize(response.json())["forecast.forecastday"][0])
                df_city['City_Name']=city
                if df_historic is None: 
                    df_historic = df_city
                else: 
                    df_historic = pd.concat((df_historic, df_city), axis = 0)
            
            i+=1
            set_loaddate = set_loaddate + timedelta(days = i)

        return df_historic

    """
    We get our last updated and incremental value log files form S3 Bucket. 
    """

    def get_incremental_value(bucket, aws_accessid, awssecretkey, regionname, filename):
        bucket=bucket
        file_name=filename
        s3 = client('s3', 
        aws_access_key_id = aws_accessid,
        aws_secret_access_key= awssecretkey,
        region_name= regionname    
        )
        response = s3.get_object(Bucket=bucket, Key=file_name)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if status == 200:
            logging.info(f"Successfully request and read from S3. Status - {status}")
            get_dates = pd.read_csv(response.get("Body"))
            try:
                latest_date = get_dates[get_dates["log_date"]== get_dates["log_date"].max()]['incremental_value'].values[0]
                return latest_date
            except:
                return None

        else:
            logging.error(f"Request to get object from S3 has failed. Status - {status}")

    def upsert_incremental_log(bucket, aws_accessid, awssecretkey, regionname, filename, incremental_value)->bool:
        bucket=bucket
        file_name=filename
        s3 = client('s3', 
        aws_access_key_id = aws_accessid,
        aws_secret_access_key= awssecretkey,
        region_name= regionname    
        )
        response = s3.list_objects_v2(Bucket=bucket)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if status == 200:
            logging.info(f"Successfully request and list objects from S3. Status - {status}")
            for i in range(len(response['Contents'])):
                if file_name == response['Contents'][i]['Key']:
                    try:
                        get_that_file = s3.get_object(Bucket=bucket, Key=file_name)
                        df_existing_log = pd.read_csv(get_that_file.get("Body"))
                        df_incremental_log = pd.DataFrame(data={
                        "log_date":[dt.now().strftime("%Y-%m-%d")], 
                        "incremental_value": [incremental_value]})
                        df_updated_incremental_log = pd.concat([df_existing_log,df_incremental_log])
                        df_updated_incremental_log.to_csv(file_name, index=False)
                        write_into_s3 = s3.upload_file(file_name, Bucket=bucket, Key=file_name)
                        logging.info(f"{file_name} has been updated on S3.")

                    except:
                            logging.info(f"Request to get object from S3 has failed. Status - {status}")
                                
                else: 
                    df_incremental_log = pd.DataFrame(data={
                    "log_date": [dt.now().strftime("%Y-%m-%d")], 
                    "incremental_value": [incremental_value]})
                    df_updated_incremental_log = pd.concat([df_existing_log,df_incremental_log])
                    df_updated_incremental_log.to_csv(file_name, index=False)
                    try:
                        write_into_s3 = s3.upload_file(file_name, Bucket=bucket, Key=file_name)
                    except:
                        print(f"There has been an error creating the Incremental Log. Status - {status}")

        else:
            logging.error(f"Request to list object from S3 has failed. Status - {status}")

        return True

    def extract_from_api (table_name, key, bucket, filename, aws_accessid, awssecretkey, regionname)->pd.DataFrame:
        try:
            if table_name == "raw_cities":
                df = Extract.extract_cities()
                logging.info(f"Successfully extracted table: {table_name}, rows extracted: {len(df)}")
            elif table_name == "raw_forecast":
                df= Extract.extract_weather_forecast(key=key)
                logging.info(f"Successfully extracted table: {table_name}, rows extracted: {len(df)}")
            elif table_name =="raw_historic":
                latest_date = Extract.get_incremental_value(bucket=bucket, aws_accessid=aws_accessid, awssecretkey=awssecretkey, regionname=regionname, filename=filename)
                extracted = Extract.extract_weather_historic(key, latest_date)
                df = extracted
                incremental_date = str(df["date"][df["date"]== df["date"].max()].values[0])
                Extract.upsert_incremental_log(bucket=bucket, aws_accessid=aws_accessid, awssecretkey=awssecretkey,filename=filename, regionname=regionname, incremental_value = incremental_date)
                logging.info(f"Successfully extracted table: {table_name}, rows extracted: {len(df)}")
            else:
                logging.error(f"An error has occured. {table_name} not found.")

            return df
        
        except:
            logging.error(f"An error has occured. {table_name} not found.")


