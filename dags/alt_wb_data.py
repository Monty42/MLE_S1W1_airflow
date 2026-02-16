# dag scripts for World Bank Dataset
from datetime import timedelta
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from steps.wb_data import create_table, extract, transform, load
LOG_FORMAT  = f'WB_DATA DAG - '

with DAG(
    dag_id='prepare_wb_data_alt',
    schedule='@once',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["WorldBank", "ETL", "Test"], 
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=0.5)}) as dag:

    step1 = PythonOperator(task_id='create_table', python_callable=create_table)
    step2 = PythonOperator(task_id='extract', python_callable=extract)
    step3 = PythonOperator(task_id='transform', python_callable=transform)
    step4 = PythonOperator(task_id='load', python_callable=load)

    # step1 >> step2 >> step3 >> step4
    chain([step1, step2], step3, step4)
