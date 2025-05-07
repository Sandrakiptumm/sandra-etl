from .extractor import Extractor
from .transformer import Transformer
from .loader import Loader
import pandas as pd

class ELTPipeline:
    def __init__(self, extract_config, transform_config, load_config):      
        self.extract_config = extract_config
        self.transform_config = transform_config
        self.load_config = load_config
    def run(self):
        """
        Executes the full ELT process: Extract → Transform → Load
        """
        # 1. Extract Data
        source_type = self.extract_config['source_type']
        if source_type == 'csv':
            df = Extractor.from_csv(self.extract_config['file_path'])
        elif source_type == 'sqlite':
            df = Extractor.from_sqlite(self.extract_config['database_path'], self.extract_config['query'])
        elif source_type == 'postgresql':
            df = Extractor.from_postgres(
                self.extract_config['host'],
                self.extract_config['dbname'],
                self.extract_config['user'],
                self.extract_config['password'],
                self.extract_config['query']
            )
        elif source_type == 'api':
            df = Extractor.from_url(self.extract_config['url'])
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        # 2. Transform Data
        if self.transform_config.get('drop_nulls', False):
            df = Transformer.drop_nulls(df)

        if self.transform_config.get('fill_nulls') is not None:
            df = Transformer.fill_nulls(df, self.transform_config['fill_nulls'])

        if self.transform_config.get('convert_to_datetime'):
            df = Transformer.convert_to_datetime(df, self.transform_config['convert_to_datetime'])

        if self.transform_config.get('convert_to_numeric'):
            df = Transformer.convert_to_numeric(df, self.transform_config['convert_to_numeric'])

        if self.transform_config.get('rename_columns'):
            df = Transformer.rename_columns(df, self.transform_config['rename_columns'])

        if self.transform_config.get('drop_duplicates', False):
            df = Transformer.drop_duplicates(df)

        if self.transform_config.get('format_text_columns', False):
            df = Transformer.format_text_columns(df)

        # 3. Load Data
        target_type = self.load_config['target_type']
        if target_type == 'csv':
            Loader.to_csv(df, self.load_config['file_path'])
        elif target_type == 'sqlite':
            Loader.to_sqlite(df, self.load_config['database_path'], self.load_config['table_name'])
        elif target_type == 'postgresql':
            Loader.to_postgresql(
                df = df,
                dbname=self.load_config['dbname'],
                user=self.load_config['user'],
                password=self.load_config['password'],
                table_name= self.load_config['table_name'],
                host=self.load_config.get('host', 'localhost'),
                port=self.load_config.get('port',5432)
                
            )
        else:
            raise ValueError(f"Unsupported target type: {target_type}")








