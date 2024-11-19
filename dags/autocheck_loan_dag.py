from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from main import run_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'AutoCheck - Loan Data Pipeline',
    default_args=default_args,
    schedule_interval='0 8 * * *', 
)

task = PythonOperator(
    task_id='Process Loan Data',
    python_callable=run_etl,
    dag=dag,
)