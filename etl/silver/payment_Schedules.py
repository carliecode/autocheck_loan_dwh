import logging
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from config.db_settings import connection_string, table_schema

# Database setup
engine = create_engine(connection_string)
metadata = MetaData()
silver_payment_schedules = Table('Payment_Schedules', metadata, autoload_with=engine, schema=table_schema['model'])

Session = sessionmaker(bind=engine)
session = Session()

def load_payment_schedules():
    """Load payment schedules data from staging to silver schema, handling insertions and updates."""
    try:
        bronze_data = pd.read_sql_table('Payment_Schedules', engine, schema=table_schema['staging'])
        today_date = pd.to_datetime('today')
        
        for index, row in bronze_data.iterrows():
            # Check if Schedule_Id exists in the dimension table
            existing_payment = session.query(silver_payment_schedules).filter(silver_payment_schedules.c.Schedule_Id == row['Schedule_Id']).first()

            # If Schedule_Id exists and no changes, do nothing
            if existing_payment and (
                existing_payment.Loan_Id == row['Loan_Id'] and
                existing_payment.Expected_Payment_Date == row['Expected_Payment_Date'] and
                existing_payment.Expected_Payment_Amount == row['Expected_Payment_Amount']
            ):
                continue

            # If Schedule_Id exists but attributes have changed, update
            elif existing_payment:
                session.query(silver_payment_schedules).filter(silver_payment_schedules.c.Schedule_Id == row['Schedule_Id']).update({
                    'Loan_Id': row['Loan_Id'],
                    'Expected_Payment_Date': row['Expected_Payment_Date'],
                    'Expected_Payment_Amount': row['Expected_Payment_Amount'],
                    'Updated_At': today_date
                })

            # If new record, insert
            else:
                insert_data = {
                    'Schedule_Id': row['Schedule_Id'],
                    'Loan_Id': row['Loan_Id'],
                    'Expected_Payment_Date': row['Expected_Payment_Date'],
                    'Expected_Payment_Amount': row['Expected_Payment_Amount'],
                    'Created_At': today_date,
                    'Updated_At': today_date
                }
                session.execute(silver_payment_schedules.insert(), insert_data)

        # Commit changes
        session.commit()

    except Exception as e:
        logging.error(f'An error occurred while moving data from Payment_Schedules to Dim_Payment_Schedules: {e}')
        raise

if __name__ == '__main__':
    load_payment_schedules()
