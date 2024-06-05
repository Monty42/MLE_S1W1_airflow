# dag scripts for World Bank Dataset
from collections import defaultdict
import sys
import json
import logging
from typing import Dict, List

import pendulum
import pandas as pd
import numpy as np
from pycountry import countries
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.decorators import dag, task
from sqlalchemy import (
    Table, Column, Float, Integer,
    MetaData, String, UniqueConstraint, inspect)

sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
LOG_FORMAT  = f'WB_DATA DAG - '

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["WorldBank", "ETL", "Test"]
)
def prepare_wb_data():
    """Prepare data World Bank dataset"""
    @task()
    def create_table():
        """Create Task part for DAG"""
        # Создайте таблицу в базе данных Destination_db, доступ к который вы получили при прохождении курса
        pass

    @task()
    def extract():
        """Extract Task part for DAG"""
        # Импортируйте данные из локальных файлов или базы данных Source_db, доступ к который вы получили при прохождении курса
        pass

    @task()
    def transform():
        """Transform Task part for DAG"""
        # Измение данные полученные на шаге Extract и верните финальный датафрейм для записи
        pass

    @task()
    def load(data: pd.DataFrame):
        """Load Task part for DAG"""
        # Выгрузите данные в созданную таблицу в базу данных Destination_db, доступ к который вы получили при прохождении курса
        pass

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

prepare_wb_data()

if __name__ == '__main__':
    prepare_wb_data()
