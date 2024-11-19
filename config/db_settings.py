connection_string =  f'mssql+pyodbc://sa:Businessweek$123@localhost/autocheck_db?driver=ODBC+Driver+17+for+SQL+Server'

table_schema = {
    'staging':'bronze',
    'model':'silver',
    'datamart':'gold'
    }

file_settings = [
    {
        'name': 'borrowers',
        'file_name': 'data\\Borrower_Data.xlsx',
        'file_type': 'excel',
        'table_name': 'Borrowers'
    },
    {
        'name': 'loans',
        'file_name': 'data\\Loan_Data.csv',
        'file_type': 'csv',
        'table_name': 'Loans'
    },
    {
        'name': 'schedules',
        'file_name': 'data\\Schedule_Data.xlsx',
        'file_type': 'excel',
        'table_name': 'Payment_Schedules'
    },
    {
        'name': 'repayments',
        'file_name': 'data\\Repayment_Data.csv',
        'file_type': 'csv',
        'table_name': 'Payments'
    }
]     
