# from dvd_rental.pipeline.extract_load_pipeline import ExtractLoad
from graphlib import TopologicalSorter
import os 
from database.postgres import PostgresDB
from weatherapi.etl.transform import Transform
import yaml 
from io import StringIO
import logging
from utility.metadata_logging import MetadataLogging
import datetime as dt 


def run_pipeline():

    # set up logging 
    run_log = StringIO()
    logging.basicConfig(stream=run_log,level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")
    
    # set up metadata logger 
    metadata_logger = MetadataLogging(db_target="target")
    
    
    with open("weatherapi/config.yaml") as stream:
        config = yaml.safe_load(stream)
    
    metadata_log_table = config["meta"]["log_table"]
    metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="Started",
        run_id=metadata_log_run_id, 
        run_config=config,
        db_table=metadata_log_table
    )

    try: 
    
        # configure pipeline 
        logging.info("Getting config variables")
        # path_extract_model = config["extract"]["model_path"]
        # path_extract_log = config["extract"]["log_path"]
        path_transform_model = config["transform"]["model_path"]
        # chunksize = config["load"]["chunksize"]
        # set up database 
        source_engine = PostgresDB.create_pg_engine(db_target="source")
        target_engine = PostgresDB.create_pg_engine(db_target="target")
        
        # build dag 
        dag = TopologicalSorter()
        # nodes_extract_load = []
        
        # logging.info("Creating extract and load nodes")
        # extract_load nodes 
        # for file in os.listdir(path_extract_model):
        #     node_extract_load = ExtractLoad(
        #         source_engine=source_engine, 
        #         target_engine=target_engine,
        #         table_name=file.replace(".sql", ""), 
        #         path=path_extract_model, 
        #         path_extract_log=path_extract_log,
        #         chunksize=chunksize
        #     )
        #     dag.add(node_extract_load)
        #     nodes_extract_load.append(node_extract_load)
        
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
        
        # adding nodes to build dag
        dag.add(node_staging_forecast)
        dag.add(node_staging_historic)
        dag.add(node_staging_historic_and_forecast, node_staging_forecast, node_staging_historic)
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