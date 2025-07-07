#!/home/krasti/MLOps_project/venv/bin/python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from transform_script import transfrom
import pandas as pd
from pathlib import Path

default_args = {
    'owner': 'danila_krasnov',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 5),
    'retries': 1,
}

def extract_load(**kwargs):
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    profit_data = pd.read_csv('/home/krasti/MLOps_project/data/profit_table.csv')
    flags_activity = transfrom(profit_data, date_str)

    flags_activity['calculation_date'] = date_str
    output_path = '/home/krasti/MLOps_project/data/flags_activity.csv'

    if Path(output_path).exists():
        existing_data = pd.read_csv(output_path)
        existing_data = existing_data[existing_data['calculation_date'] != date_str]
        final_result = pd.concat([existing_data, flags_activity], ignore_index=True)
    else:
        final_result = flags_activity
    final_result.to_csv(output_path, index=False)

with DAG(
    'danila_krasnov_client_activity_sequential',
    default_args=default_args,
    schedule_interval='0 0 5 * *',
    catchup=False,
) as dag:
    etl_task = PythonOperator(
        task_id='sequential_processing',
        python_callable=extract_load,
        provide_context=True,
    )