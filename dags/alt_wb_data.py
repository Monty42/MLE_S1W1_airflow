# dag scripts for World Bank Dataset
from airflow import DAG
from datetime import timedelta
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from steps.wb_data import create_table, extract, transform, load
from steps.messages import (
    send_telegram_task_success_message,
    send_telegram_dag_success_message,
    send_telegram_failure_message,
)

LOG_FORMAT  = f'WB_DATA DAG - '

with DAG(
    dag_id='prepare_wb_data_alt',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["WorldBank", "ETL", "Test"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    },
    on_success_callback=send_telegram_dag_success_message,
    on_failure_callback=send_telegram_failure_message,
) as dag:

    step1 = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        on_success_callback=send_telegram_task_success_message,
    )
    step2 = PythonOperator(
        task_id='extract',
        python_callable=extract,
        on_success_callback=send_telegram_task_success_message,
    )
    step3 = PythonOperator(
        task_id='transform',
        python_callable=transform,
        on_success_callback=send_telegram_task_success_message,
    )
    step4 = PythonOperator(
        task_id='load',
        python_callable=load,
        on_success_callback=send_telegram_task_success_message,
    )

    [step1, step2] >> step3 >> step4
    # эквивалентно chain([step1, step2], step3, step4)

