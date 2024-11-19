import pandas as pd
import logging
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from config.db_settings import connection_string, table_schema

# Database setup
engine = create_engine(connection_string)
metadata = MetaData()
silver_borrowers = Table('Borrowers', metadata, autoload_with=engine, schema=table_schema['model'])

Session = sessionmaker(bind=engine)
session = Session()

def load_borrowers():
    """Load borrowers data from staging to silver schema, handling insertions and updates."""
    try:
        bronze_data = pd.read_sql_table('Borrowers', engine, schema=table_schema['staging'])
        today_date = pd.to_datetime('today')
        
        for index, row in bronze_data.iterrows():
            # Check if Borrower_Id exists in dimension table
            existing_borrower = session.query(silver_borrowers).filter(silver_borrowers.c.Borrower_Id == row['Borrower_Id']).first()

            # If Borrower_Id exists and no changes, do nothing
            if existing_borrower and (
                existing_borrower.State == row['State'] and
                existing_borrower.City == row['City'] and
                existing_borrower.Zip_Code == row['Zip_Code'] and
                existing_borrower.Borrower_Credit_Score == row['Borrower_Credit_Score']
            ):
                continue

            # If Borrower_Id exists but attributes have changed, update
            elif existing_borrower:
                session.query(silver_borrowers).filter(silver_borrowers.c.Borrower_Id == row['Borrower_Id']).update({
                    'State': row['State'],
                    'City': row['City'],
                    'Zip_Code': row['Zip_Code'],
                    'Borrower_Credit_Score': row['Borrower_Credit_Score'],
                    'Updated_At': today_date
                })

            # If new record, insert
            else:
                insert_data = {
                    'Borrower_Id': row['Borrower_Id'],
                    'State': row['State'],
                    'City': row['City'],
                    'Zip_Code': row['Zip_Code'],
                    'Borrower_Credit_Score': row['Borrower_Credit_Score'],
                    'Created_At': today_date,
                    'Updated_At': today_date
                }
                session.execute(silver_borrowers.insert(), insert_data)
                
        session.commit()

    except Exception as e:
        logging.error(f'An error occurred while moving data from Borrowers to Dim_Borrowers: {e}')
        raise

if __name__ == '__main__':
    load_borrowers()
