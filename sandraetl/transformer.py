import pandas as pd
import numpy as np

class Transformer:
    @staticmethod
    def drop_nulls(df):
        return df.dropna()

    @staticmethod
    def fill_nulls(df, value=0):
        return df.fillna(value)

    @staticmethod
    def convert_to_datetime(df, column_name):
        if column_name in df.columns:
            df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        else:
            print(f"[Warning] Column '{column_name}' not found. Skipping datetime conversion.")
        return df

    @staticmethod
    def convert_to_numeric(df, column_name):
        if column_name in df.columns:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        else:
            print(f"[Warning] Column '{column_name}' not found. Skipping numeric conversion.")
        return df

    @staticmethod
    def convert_to_text(df, column_name):
        if column_name in df.columns:
            df[column_name] = df[column_name].astype(str)
        else:
            print(f"[Warning] Column '{column_name}' not found. Skipping text conversion.")
        return df

    @staticmethod
    def rename_columns(df, columns_mapping):
        missing_columns = [col for col in columns_mapping.keys() if col not in df.columns]
        if missing_columns:
            print(f"[Warning] Columns {missing_columns} not found. Skipping renaming for them.")
        return df.rename(columns=columns_mapping)

    @staticmethod
    def drop_duplicates(df):
        return df.drop_duplicates()

    @staticmethod
    def format_text_columns(df):
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].str.strip().str.lower()
        return df
