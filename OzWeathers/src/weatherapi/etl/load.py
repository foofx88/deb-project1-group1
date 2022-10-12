import logging 
import pandas as pd
import numpy as np 
from sqlalchemy import Table, Column, BigInteger, String, DateTime, Numeric, Boolean, MetaData
from sqlalchemy.dialects import postgresql

#import os  

class Load():

    def __init__(self, df:pd.DataFrame, database_engine:str, database_table_name:str, key_columns:list=None):
        self.df=df
        self.key_columns=key_columns
        self.database_engine=database_engine
        self.database_table_name=database_table_name

    def _get_sqlalchemy_column(column_name:str, source_datatype:str, primary_key:bool=False)->Column:
        """
        A helper function that returns a SQLAlchemy column by mapping a pandas dataframe datatypes to sqlalchemy datatypes 
        """
        dtype_map = {
            "int64": BigInteger, 
            "object": String, 
            "datetime64[ns]": DateTime, 
            "float64": Numeric,
            "bool": Boolean
        }
        column = Column(column_name, dtype_map[source_datatype], primary_key=primary_key) 
        return column

    def _generate_sqlalchemy_schema(df: pd.DataFrame, key_columns:list, table_name:str, meta:MetaData)->Table: 
        """
        A helper function that generates a sqlalchemy table schema that shall be used to create the target table and perform insert/upserts. 
        """
        schema = []
        for column in [{"column_name": col[0], "source_datatype": col[1]} for col in zip(df.columns, [dtype.name for dtype in df.dtypes])]:
            schema.append(self._get_sqlalchemy_column(**column, primary_key=column["column_name"] in key_columns))
        return Table(table_name, meta, *schema)

    def _upsert_in_chunks(df:pd.DataFrame, database_engine, table_schema:Table, key_columns:list, chunksize:int=500)->bool:
        """
        A helper function that performs the upsert with several rows at a time (i.e. a chunk of rows). 
        """
        max_length = len(df)
        df = df.replace({np.nan: None})
        for i in range(0, max_length, chunksize):
            if i + chunksize >= max_length: 
                lower_bound = i
                upper_bound = max_length 
            else: 
                lower_bound = i 
                upper_bound = i + chunksize
            insert_statement = postgresql.insert(table_schema).values(df.iloc[lower_bound:upper_bound].to_dict(orient='records'))
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=key_columns,
                set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
            logging.info(f"Inserting chunk: [{lower_bound}:{upper_bound}] out of index {max_length}")
            result = database_engine.execute(upsert_statement)
        return True 

    def _upsert_all(df:pd.DataFrame, database_engine, table_schema:Table, key_columns:list)->bool:
        """
        A helper function that performs the upsert with all rows at once.
        """
        insert_statement = postgresql.insert(table_schema).values(df.to_dict(orient='records'))
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
        result = database_engine.execute(upsert_statement)
        logging.info(f"Insert/updated rows: {result.rowcount}")
        return True 
    
    def upsert_to_database(self, key_columns:str, chunksize:int=500)->bool: 
        """
        Upsert dataframe to a database table 
        - `key_columns`: name of key columns to be used for upserting 
        - `chunksize`: if chunksize greater than 0 is specified, then the rows will be inserted in the specified chunksize. e.g. 1000 rows at a time. 
        """
        meta = MetaData()
        logging.info(f"Generating table schema: {self.database_table_name}")
        table_schema = self._generate_sqlalchemy_schema(df=self.df, key_columns=key_columns,table_name=self.database_table_name, meta=meta)
        meta.create_all(self.database_engine)
        logging.info(f"Table schema generated: {self.database_table_name}")
        logging.info(f"Writing to table: {self.database_table_name}")
        if chunksize > 0:
            self._upsert_in_chunks(df=self.df, database_engine=self.database_engine, table_schema=table_schema, key_columns=key_columns, chunksize=chunksize)
        else: 
            self._upsert_all(df=self.df, database_engine=self.database_engine, table_schema=table_schema, key_columns=key_columns)
        logging.info(f"Successful write to table: {self.database_table_name}")
        return True 

    def overwrite_to_database(self)->bool: 

        logging.info(f"Writing to table: {self.database_table_name}")
        self.df.to_sql(name=self.database_table_name, con=self.database_engine, if_exists="replace", index=False)
        logging.info(f"Successful write to table: {self.database_table_name}, rows inserted/updated: {len(self.df)}")
        return True