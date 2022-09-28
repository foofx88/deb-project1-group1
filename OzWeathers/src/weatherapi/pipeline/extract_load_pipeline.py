from dvd_rental.etl.extract import Extract
from dvd_rental.etl.load import Load

class ExtractLoad():

    def __init__(self, source_engine, target_engine, table_name, path, path_extract_log, chunksize:int=1000):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.table_name = table_name
        self.path = path 
        self.path_extract_log = path_extract_log
        self.chunksize = chunksize
    
    def run(self):
        df = Extract.extract_from_database(table_name=self.table_name, engine=self.source_engine, path=self.path, path_extract_log=self.path_extract_log)
        if Extract.is_incremental(table=self.table_name, path=self.path):
            key_columns = Load.get_key_columns(table=self.table_name, path=self.path)
            Load.upsert_to_database(df=df, table_name=self.table_name, key_columns=key_columns, engine=self.target_engine, chunksize=self.chunksize)
        else: 
            Load.overwrite_to_database(df=df, table_name=self.table_name, engine=self.target_engine)
