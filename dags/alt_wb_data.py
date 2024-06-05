# dag scripts for World Bank Dataset
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from steps.wb_data import create_table, extract, transform, load
LOG_FORMAT  = f'WB_DATA DAG - '

# Реализуйте DAG с помощью контекстного менеджера `with`
with DAG() as dag:
    pass