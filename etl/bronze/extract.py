import logging
import utils
import pandas as pd
from sqlalchemy import create_engine
from config.db_settings import connection_string, file_settings, table_schema



def import_data_files():
    try:
        global cn 
        engine = create_engine(connection_string)
        cn = engine.connect()

        for config in file_settings: 
            read_data_files(config)
        
    except Exception as e:
        logging.log(logging.ERROR, f'An error occurred : {e}')
        raise
    finally:
        if engine:
            engine.dispose()


def read_data_files(config):
    file_name, table_name = config['file_name'], config['table_name']
    try:
        if config['file_type'] == 'excel':  
            df = pd.read_excel(config['file_name'])
        if config['file_type'] == 'csv':  
            df = pd.read_csv(config['file_name'])

        df.columns = [str.replace(str.title(s), ' ', '_') for s in df.columns]
        df.to_sql(config['table_name'], cn, schema = table_schema['staging'], if_exists = 'replace', index=False)   
        logging.info( f'The extraction of {file_name} to {table_name} has completed.')

    except Exception as e:
        logging.error(f'\nAn error occurred while extracting  {file_name} to {table_name} : {e}')
        raise
