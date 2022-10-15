from graphlib import TopologicalSorter
from io import StringIO

from attr import NOTHING
from weatherapi.etl.extract_copy import Extract
from weatherapi.etl.load import Load
from weatherapi.etl.transform import Transform
from database.postgres import PostgresDB
from utility.metadata_logging import MetadataLogging
from art import tprint
import datetime as dt 
import os 
import logging
import yaml


def run_pipeline():

    # set up logging 
    run_log = StringIO()
    logging.basicConfig(stream=run_log,level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

    logging.info("Reading yaml config file")
    # get config variables
    with open("weatherapi/config.yaml") as stream:
        config = yaml.safe_load(stream)
    
    logging.info("Getting yaml config variables")
    metadata_log_table = config["meta"]["log_table"]
    path_transform_model = config["transform"]["model_path"]
    aws_bucket = config['extract']['aws_bucket']
    aws_log_file = config['extract']['aws_log_file']
    cities_api_url = config['extract']['cities_api_url'] # Added
    cities_api_country = config['extract']['cities_api_country'] # Added
    weather_api_historic_url = config['extract']['weather_api_historic_url'] # Added
    weather_api_forecast_url = config['extract']['weather_api_forecast_url'] # Added

    logging.info("Getting env variables")       
    db_user = os.environ.get("source_db_user")
    db_password = os.environ.get("source_db_password")
    db_server_name = os.environ.get("source_db_server_name")
    db_database_name = os.environ.get("source_db_database_name") 
    db_user = os.environ.get("target_db_user")
    db_password = os.environ.get("target_db_password")
    db_server_name = os.environ.get("target_db_server_name")
    db_database_name = os.environ.get("target_db_database_name")    
    weather_api_key = os.environ.get('weather_api_key')
    aws_access_id = os.environ.get('aws_access_id')  
    aws_secret_key = os.environ.get('aws_secret_key')
    aws_region = os.environ.get('aws_region')   

    logging.info("Creating source database engine")
    # set up source db engine     
    se = PostgresDB(db_user=db_user, db_password=db_password, db_server_name=db_server_name, db_database_name=db_database_name)
    source_engine = se.create_pg_engine()

    logging.info("Creating target database engine")
    # set up target db engine     
    te = PostgresDB(db_user=db_user, db_password=db_password, db_server_name=db_server_name, db_database_name=db_database_name)
    target_engine = te.create_pg_engine()

    logging.info("Setting up metadata logger")
    # set up metadata logger 
    metadata_logger = MetadataLogging(engine=target_engine)    
    metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)
    
    try:

        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Started",
            run_id=metadata_log_run_id, 
            run_config=config,
            db_table=metadata_log_table
        )              

        logging.info("Running extract to get cities df")
        # Extract historic weather data
        cities_object = Extract(table_name='raw_cities', cities_api_url=cities_api_url, cities_api_country=cities_api_country)
        cities_df = cities_object.run()

        logging.info("Running extract to get historic weather df")
        # Extract historic weather data
        historic_object = Extract(table_name='raw_historic', cities_api_url=cities_api_url, cities_api_country=cities_api_country, weather_api_url=weather_api_historic_url, weather_api_key=weather_api_key, aws_bucket=aws_bucket, aws_access_id=aws_access_id, aws_secrect_key=aws_secret_key, aws_region=aws_region, aws_log_file=aws_log_file)
        historic_df = historic_object.run()

        logging.info("Running extract to get forecast weather df")
        # Extract forecast weather data
        forecast_object = Extract(table_name='raw_forecast', cities_api_url=cities_api_url, cities_api_country=cities_api_country, weather_api_url=weather_api_forecast_url, weather_api_key=weather_api_key)
        forecast_df = forecast_object.run()

        if historic_df.empty:
        # if len(historic_df) == 0:
            tprint('Oh Shit!')
            print('historic_df is empty')
        elif forecast_df.empty:
            tprint('Oh Shit!')
            print('forecast_df is empty')

        logging.info("Creating load cities nodes")
        # Load cities data
        node_load_cities = Load(df=cities_df, engine=target_engine, table_name='raw_cities', load_method='overwrite')

        logging.info("Creating load historic node")
        # Load historic weather data
        node_load_historic = Load(df=historic_df, engine=target_engine, table_name='raw_historic', load_method='upsert', chunksize=5, key_columns=['City_Name', 'date'])

        logging.info("Creating load forecast nodes")
        # Load forecast weather data
        node_load_forecast = Load(df=forecast_df, engine=target_engine, table_name='raw_forecast', load_method='overwrite')
               
        logging.info("Creating transform nodes")
        # transform nodes  
        node_staging_forecast = Transform(model="staging_forecast", engine=target_engine, models_path=path_transform_model)
        node_staging_historic = Transform(model="staging_historic", engine=target_engine, models_path=path_transform_model)
        node_staging_historic_and_forecast = Transform(model="staging_historic_and_forecast", engine=target_engine, models_path=path_transform_model)
        node_serving_activities = Transform(model="serving_activities", source_table = 'staging_historic_and_forecast', engine=target_engine, models_path=path_transform_model)
        node_serving_rank_city_metrics_by_day = Transform(model="serving_rank_city_metrics_by_day", source_table = 'staging_historic_and_forecast', engine=target_engine, models_path=path_transform_model)
        node_serving_rank_city_metrics_by_month = Transform(model="serving_rank_city_metrics_by_month", source_table = 'staging_historic_and_forecast', engine=target_engine, models_path=path_transform_model)
        node_serving_weather_condition_count_by_month = Transform(model="serving_weather_condition_count_by_month", source_table = 'staging_historic', engine=target_engine, models_path=path_transform_model)
        node_serving_weather_condition_count_by_year = Transform(model="serving_weather_condition_count_by_year", source_table = 'staging_historic', engine=target_engine, models_path=path_transform_model)
        
        # build dag
        dag = TopologicalSorter()

        logging.info("Adding dag nodes")
        # adding load nodes 
        if not cities_df.empty or not historic_df.empty and not forecast_df.empty:
            dag.add(node_load_cities)
            dag.add(node_load_historic)
            dag.add(node_load_forecast)
        
        # adding trasform staging nodes 
        dag.add(node_staging_forecast)
        dag.add(node_staging_historic)        
        dag.add(node_staging_historic_and_forecast, node_staging_forecast, node_staging_historic)

        # adding transform serving nodes 
        dag.add(node_serving_activities, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)
        dag.add(node_serving_rank_city_metrics_by_day, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)
        dag.add(node_serving_rank_city_metrics_by_month, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)
        dag.add(node_serving_weather_condition_count_by_month, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)
        dag.add(node_serving_weather_condition_count_by_year, node_staging_forecast, node_staging_historic, node_staging_historic_and_forecast)

        logging.info("Executing DAG")
        # run dag 
        dag_rendered = tuple(dag.static_order())

        for node in dag_rendered: 
            node.run()

        logging.info("Pipeline run successful")
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Completed",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )

    except Exception as e: 
        logging.exception(e)
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="Error",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )

    print(run_log.getvalue())

if __name__ == "__main__":
    run_pipeline()