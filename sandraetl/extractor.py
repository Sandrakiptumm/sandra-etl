import requests
import pandas as pd
import sqlite3
import psycopg2


class Extractor:
  @staticmethod
  def from_url(url): #to pandas dataframe
    response = requests.get(url)
    if response.status_code == 200:
      return pd.read_csv(url)
    else:
      raise Exception(f"Failed to fetch data from {url}")
    
  @staticmethod
  def from_sqlite(db_path, query):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
  
  @staticmethod
  def from_postgres(host, dbname, user, password, query):
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
  
  @staticmethod
  def from_csv(file_path):
    """
    Extracts data from a CSV file and returns it as a pandas DataFrame.
    """
    return pd.read_csv(file_path)