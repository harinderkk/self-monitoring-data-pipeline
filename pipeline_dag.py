from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

from ingestion import fetch_all_series, store_market_data, fetch_edgar_filings, store_edgar_filings, CANADIAN_COMPANIES
from monitor import run_all_checks
from alerts import process_unexplained_anomalies

default_args = {
    'owner': 'sedar_pipeline',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def ingest_market_data():
    df = fetch_all_series()
    stored = store_market_data(df)
    print(f"Stored {stored} market data records")
    return stored

def ingest_edgar_filings():
    all_filings = []
    for company, cik in CANADIAN_COMPANIES.items():
        filings = fetch_edgar_filings(cik, company, limit=5)
        all_filings.extend(filings)
    stored = store_edgar_filings(all_filings)
    print(f"Stored {stored} EDGAR filings")
    return stored

def run_monitor():
    anomalies = run_all_checks()
    print(f"Monitor detected {len(anomalies)} anomalies")
    return len(anomalies)

def run_alerts():
    results = process_unexplained_anomalies()
    print(f"Processed {len(results)} anomaly explanations")
    return len(results)

def ingest_market_data():
    total = fetch_all_series()  # now handles watermarks internally
    print(f"Stored {total} new market data records")
    return total

def ingest_edgar_filings():
    total = 0
    for company, cik in CANADIAN_COMPANIES.items():
        total += fetch_and_store_edgar(company, cik)  # new function
    print(f"Stored {total} new EDGAR filings")
    return total

with DAG(
    dag_id='sedar_pipeline',
    default_args=default_args,
    description='AI-powered self-monitoring Canadian financial data pipeline',
    schedule='0 9 * * 1-5',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['sedar', 'financial', 'monitoring'],
) as dag:

    task_ingest_market = PythonOperator(
        task_id='ingest_market_data',
        python_callable=ingest_market_data,
    )

    task_ingest_edgar = PythonOperator(
        task_id='ingest_edgar_filings',
        python_callable=ingest_edgar_filings,
    )

    task_monitor = PythonOperator(
        task_id='run_monitor',
        python_callable=run_monitor,
    )

    task_alerts = PythonOperator(
        task_id='run_alerts',
        python_callable=run_alerts,
    )

    [task_ingest_market, task_ingest_edgar] >> task_monitor >> task_alerts