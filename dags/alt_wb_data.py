# dag scripts for World Bank Dataset
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from steps.wb_data import create_table, extract, transform, load
LOG_FORMAT  = f'WB_DATA DAG - '

with DAG(
    dag_id='prepare_wb_data_alt',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["WorldBank", "ETL", "Test"]) as dag:

    step1 = PythonOperator(task_id='1', python_callable=create_table)
    step2 = PythonOperator(task_id='2', python_callable=extract)
    step3 = PythonOperator(task_id='3', python_callable=transform)
    step4 = PythonOperator(task_id='4', python_callable=load)

    [step1, step2] >> step3 >> step4
    # эквивалентно chain([step1, step2], step3, step4)
