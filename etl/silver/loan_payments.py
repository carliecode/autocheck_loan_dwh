import logging
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from config.db_settings import connection_string, table_schema
from utils import validate_and_cast_date

# Database setup
engine = create_engine(connection_string)
metadata = MetaData()
silver_loan_payments = Table('Loan_Payments', metadata, autoload_with=engine, schema=table_schema['model'])

Session = sessionmaker(bind=engine)
session = Session()

def load_loan_payments():
    """Process loan payments data, handling insertions and updates."""
    try:
        payments = pd.read_sql_table('Payments', engine, schema=table_schema['staging'])
        loans = pd.read_sql_table('Loans', engine, schema=table_schema['staging'])

        bronze_data = pd.merge(payments, loans, on='Loan_Id', how='inner')
        today_date = pd.to_datetime('today')

        for index, row in bronze_data.iterrows():
            row['Date_Paid'] = validate_and_cast_date(row['Date_Paid'])

            # Check if Payment_Id exists in the dimension table
            existing_payment = session.query(silver_loan_payments).filter(silver_loan_payments.c.Payment_Id == row['Payment_Id']).first()

            # If Payment_Id exists and no changes, do nothing
            if existing_payment and (
                existing_payment.Amount_Paid == row['Amount_Paid'] and
                existing_payment.Date_Paid == row['Date_Paid']
            ):
                continue

            # If Payment_Id exists but attributes have changed, update
            elif existing_payment:
                session.query(silver_loan_payments).filter(silver_loan_payments.c.Payment_Id == row['Payment_Id']).update({
                    'Loan_Id': row['Loan_Id'],
                    'Amount_Paid': row['Amount_Paid'],
                    'Date_Paid': row['Date_Paid'],
                    'Updated_At': today_date
                })

            # If new record, insert
            else:
                insert_data = {
                    'Payment_Id': row['Payment_Id'],
                    'Loan_Id': row['Loan_Id'],
                    'Amount_Paid': row['Amount_Paid'],
                    'Date_Paid': row['Date_Paid'],
                    'Created_At': today_date,
                    'Updated_At': today_date
                }
                session.execute(silver_loan_payments.insert(), insert_data)

        # Commit changes
        session.commit()

    except Exception as e:
        logging.error(f'An error occurred while moving data from Payments to Fact_Loan_Payments: {e}')
        raise

if __name__ == '__main__':
    loan_payments()
