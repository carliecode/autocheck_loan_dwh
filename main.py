import logging
import utils
from etl.bronze.extract import import_data_files
from etl.silver.borrowers import load_borrowers
from etl.silver.loans import load_loans
from etl.silver.payment_Schedules import load_payment_schedules
from etl.silver.loan_payments import load_loan_payments

# Configure logging
logging.basicConfig(
    filename=utils.LOG_FILE_NAME,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.getLogger().addHandler(utils.PrometheusLoggingHandler())

def run_etl():
    """Run the ETL process."""
    try:
        logging.info('AutoCheck (Loan DWH) - Starting.')

        # Extraction Layer
        logging.info('Reading input files (csv and excel) to db staging.')
        import_data_files()
        logging.info('All input files have been imported to db staging.')

        # Load Models
        logging.info('Processing Customer (Borrower) information...')
        load_borrowers()

        logging.info('Processing Loan information...')
        load_loans()

        logging.info('Processing Loan Payment Schedules information...')
        load_payment_schedules()

        logging.info('Processing Loan Payments information...')
        load_loan_payments()

        logging.info('AutoCheck (Loan DWH) - Completed.\n\n')

    except Exception as e:
        logging.error(f'An error occurred during ETL process: {e}')
        raise

if __name__ == '__main__':
    run_etl()
