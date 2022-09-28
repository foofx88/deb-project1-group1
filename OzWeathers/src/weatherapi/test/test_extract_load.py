from dvd_rental.pipeline.extract_load_pipeline import ExtractLoad
from database.postgres import PostgresDB
import pandas as pd 
import os 

def test_extract_load_full():
    # -- assemble -- 
    table_name = "mock_table"
    engine = PostgresDB.create_pg_engine(db_target="target")
    df_source_data = pd.DataFrame({
        "id": [1, 2, 3], 
        "amount": [100.50, 75.40, 152.55]
    })
    # create mock source table 
    df_source_data.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

    # create extract sql file 
    path_model_extract = "dvd_rental/models/extract"
    mock_extract_file = f"{table_name}.sql"
    extract_sql_query = """
{% set config = {
    "extract_type": "full"
} %}

select 
    id,
    amount
from 
    {{ source_table }}
    """

    if not os.path.exists(path_model_extract): 
        os.mkdir(path_model_extract) # make directory 
    if mock_extract_file in os.listdir(path_model_extract):
        os.remove(f"{path_model_extract}/{mock_extract_file}")
    
    with open(f"{path_model_extract}/{mock_extract_file}", mode="w") as f: 
        f.write(extract_sql_query)

    # -- act -- 
    node_extract_load = ExtractLoad(source_engine=engine, target_engine=engine, table_name=table_name, path=path_model_extract, path_extract_log="")
    node_extract_load.run()
    df_output = pd.read_sql(sql=table_name, con=engine)

    # clean up first 
    engine.execute(f"drop table {table_name}")
    os.remove(f"{path_model_extract}/{mock_extract_file}")

    # -- assert -- 
    pd.testing.assert_frame_equal(left=df_source_data, right=df_output, check_exact=True)

    
def test_extract_load_incremental():
    # -- assemble -- 
    table_name = "mock_table"
    engine = PostgresDB.create_pg_engine(db_target="target")
    df_source_data = pd.DataFrame({
        "id": [1, 2, 3], 
        "amount": [100.50, 75.40, 152.55], 
        "date": ["2020-01-01", "2020-01-02", "2020-01-03"]
    })
    # create mock source table 
    df_source_data.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

    # create extract sql file 
    path_model_extract = "dvd_rental/models/extract"
    path_extract_log = "dvd_rental/log/extract"
    mock_extract_file = f"{table_name}.sql"
    mock_log_file = f"{table_name}.csv"
    extract_sql_query = """
{% set config = {
    "extract_type": "incremental",
    "incremental_column": "date", 
    "key_columns":["id"]
} %}

select 
    id,
    amount,
    date
from 
    {{ source_table }}
{% if is_incremental %}
    where date > '{{ incremental_value }}'
{% endif %}
    """

    if not os.path.exists(path_model_extract): 
        os.mkdir(path_model_extract) # make directory 
    if mock_extract_file in os.listdir(path_model_extract):
        os.remove(f"{path_model_extract}/{mock_extract_file}")
    
    with open(f"{path_model_extract}/{mock_extract_file}", mode="w") as f: 
        f.write(extract_sql_query)

    # -- act -- 
    node_extract_load = ExtractLoad(source_engine=engine, target_engine=engine, table_name=table_name, path=path_model_extract, path_extract_log=path_extract_log)
    node_extract_load.run()
    df_output = pd.read_sql(sql=table_name, con=engine)

    # clean up first 
    engine.execute(f"drop table {table_name}")
    os.remove(f"{path_model_extract}/{mock_extract_file}")
    os.remove(f"{path_extract_log}/{mock_log_file}")

    # -- assert -- 
    pd.testing.assert_frame_equal(left=df_source_data, right=df_output, check_exact=True)

