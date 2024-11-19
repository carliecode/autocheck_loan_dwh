import pandas as pd
import calendar
import logging
import prometheus_client
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway



PUSHGATEWAY_URL = 'http://localhost:9092'
PROMETHEUS_JOB_NAME = 'autocheck_loan_dwh'
LOG_FILE_NAME = f"logs\\autocheck_{pd.to_datetime('today').strftime('%Y-%m-%d')}.log"

registry = CollectorRegistry()
log_counter = Counter('log_entries', 'Count of log entries by level', ['level'], registry=registry) 

class PrometheusLoggingHandler(logging.Handler):
    
    def emit(self, record):

        if record.levelname == 'INFO': 
            log_counter.labels(level='info').inc() 
        elif record.levelname == 'WARNING': 
            log_counter.labels(level='warning').inc() 
        elif record.levelname == 'ERROR': 
            log_counter.labels(level='error').inc() 
        try:
            push_to_gateway(PUSHGATEWAY_URL, job=PROMETHEUS_JOB_NAME, registry=registry) 
        except Exception as e:
            logging.error(f'Prometheus: {e}')


        


def validate_and_cast_date(date_str):

    month, day, year = map(int, date_str.split('/'))
    last_valid_day = calendar.monthrange(year, month)[1]
    
    if day <= last_valid_day:
        return pd.to_datetime(date_str, format='%m/%d/%Y').date()
    else:
        return pd.to_datetime(f"{month}/{last_valid_day}/{year}", format='%m/%d/%Y').date()

