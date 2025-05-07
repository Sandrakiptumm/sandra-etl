import pandas as pd
import sqlite3
import psycopg2
from sqlalchemy import create_engine

class Loader:
    @staticmethod
    def to_sqlite(df, database_path, table_name, if_exists='append'):
        conn = sqlite3.connect(database_path)
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        conn.close()

    @staticmethod
    def to_postgresql(df, dbname, user, password, table_name, host='localhost', port=5432, if_exists='append'):
        port = int(port)
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)

    @staticmethod
    def to_csv(df, file_path):
        df.to_csv(file_path, index=False)


