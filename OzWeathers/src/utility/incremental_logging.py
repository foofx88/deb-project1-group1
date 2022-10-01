from database import postgres
import datetime as dt 
from sqlalchemy import Table, Column, String, MetaData
from sqlalchemy import insert, select, func


class IncrementalLogging():
    
    def create_target_table_if_not_exists(self, db_table:str)->Table:
        meta = MetaData()
        target_table = Table(
            db_table, meta, 
            Column("run_timestamp", String, primary_key=True),
            Column("incremental_value", String)
        )
        meta.create_all(self.engine) # creates table if it does not exist
        return target_table 
    
    def get_latest_incremental_value(self, db_table:str)->str:
        target_table = self.create_target_table_if_not_exists(db_table=db_table)
        statement = (
            select(func.max(target_table.c.incremental_value))
        )
        response = self.engine.execute(statement).first()[0]
        if response is None: 
            return None 
        else: 
            return response

    def log(
        self,
        run_timestamp: dt.datetime,
        incremental_value: String,
        db_table: str,
    )->bool:
        target_table = self.create_target_table_if_not_exists(db_table=db_table)
        insert_statement = insert(target_table).values(
            run_timestamp=run_timestamp,
            incremental_value=incremental_value
        )
        self.engine.execute(insert_statement)

        return True 
