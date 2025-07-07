#!/home/krasti/MLOps_project/venv/bin/python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from transform_script import transfrom
import pandas as pd
from pathlib import Path

default_args = {
    'owner': 'danila_krasnov',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 5),
    'retries': 1,
}

def process_product(**kwargs):
    ti = kwargs['ti']
    product = kwargs['product']
    date_str = kwargs['execution_date'].strftime('%Y-%m-%d')
    
    profit_data = pd.read_csv('/home/krasti/MLOps_project/data/profit_table.csv')
    flags = transfrom(profit_data, date_str)
    flags = flags[['id', f'flag_{product}']]
    ti.xcom_push(key=f'result_{product}', value=flags.to_json())

def merge_results(**kwargs):
    ti = kwargs['ti']
    products = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    date_str = kwargs['execution_date'].strftime('%Y-%m-%d')
    
    merged = None
    for product in products:
        product_data = pd.read_json(ti.xcom_pull(key=f'result_{product}'))
        if merged is None:
            merged = product_data
        else:
            merged = merged.merge(product_data, on='id')
    
    merged['calculation_date'] = date_str
    output_path = '/home/krasti/MLOps_project/data/flags_activity_parallel.csv'
    
    if Path(output_path).exists():
        existing_data = pd.read_csv(output_path)
        existing_data = existing_data[existing_data['calculation_date'] != date_str]
        final_result = pd.concat([existing_data, merged], ignore_index=True)
    else:
        final_result = merged
    final_result.to_csv(output_path, index=False)

with DAG(
    'danila_krasnov_client_activity_parallel',
    default_args=default_args,
    schedule_interval='0 0 5 * *',
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id='start')
    merge = PythonOperator(
        task_id='merge_results',
        python_callable=merge_results,
    )
    end = EmptyOperator(task_id='end')

    products = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    for product in products:
        task = PythonOperator(
            task_id=f'process_{product}',
            python_callable=process_product,
            op_kwargs={'product': product},
        )
        start >> task >> merge
    merge >> end