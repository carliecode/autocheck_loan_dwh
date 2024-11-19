import logging
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from config.db_settings import connection_string, table_schema
from utils import validate_and_cast_date

engine = create_engine(connection_string)
metadata = MetaData()
silver_loans= Table('Loans', metadata, autoload_with=engine, schema=table_schema['model'])

Session = sessionmaker(bind=engine)
session = Session()

def load_loans():
    try:

        bronze_data = pd.read_sql_table('Loans', engine, schema=table_schema['staging'])
        today_date = pd.to_datetime('today')
        
        for index, row in bronze_data.iterrows():
            
            row['Date_Of_Release'] = validate_and_cast_date(row['Date_Of_Release'])
            row['Maturity_Date'] = validate_and_cast_date(row['Maturity_Date'])

            # Check if Loan_Id exists in dimension table
            existing_loan = session.query(silver_loans).filter(silver_loans.c.Loan_Id == row['Loan_Id']).first()

            # If Loan_Id exists and no changes, do nothing
            if existing_loan and (                
                existing_loan.Borrower_Id == row['Borrower_Id'] and
                existing_loan.Term == row['Term'] and
                existing_loan.Interest_Rate == row['Interest_Rate'] and 
                existing_loan.Loan_Amount == row['Loan_Amount'] and 
                existing_loan.Down_Payment == row['Down_Payment'] and 
                existing_loan.Payment_Frequency == row['Payment_Frequency'] and
                existing_loan.Date_Of_Release == row['Date_Of_Release'] and
                existing_loan.Maturity_Date == row['Maturity_Date']
            ): continue

            # If Loan_Id exists but attributes have changed, update
            elif existing_loan:
                session.query(silver_loans).filter(silver_loans.c.Loan_Id == row['Loan_Id']).update({
                    'Borrower_Id': row['Borrower_Id'],
                    'Term': row['Term'],
                    'Interest_Rate': row['Interest_Rate'],
                    'Loan_Amount': row['Loan_Amount'],
                    'Down_Payment': row['Down_Payment'],
                    'Payment_Frequency': row['Payment_Frequency'],
                    'Date_Of_Release': row['Date_Of_Release'],
                    'Maturity_Date': row['Maturity_Date'],
                    'Updated_At': today_date
                })

            # If new record, insert
            else:
                insert_data = {
                    'Loan_Id': row['Loan_Id'],
                    'Borrower_Id': row['Borrower_Id'],
                    'Term': row['Term'],
                    'Interest_Rate': row['Interest_Rate'],
                    'Loan_Amount': row['Loan_Amount'],
                    'Down_Payment': row['Down_Payment'],
                    'Payment_Frequency': row['Payment_Frequency'],
                    'Date_Of_Release': row['Date_Of_Release'],
                    'Maturity_Date': row['Maturity_Date'],
                    'Created_At': today_date,
                    'Updated_At': today_date
                }
                session.execute(silver_loans.insert(), insert_data)

        # Commit changes
        session.commit()

    except Exception as e:
        logging.error(f'An error occured while moving data from Loans to Dim_Loans {e}')
        raise

    