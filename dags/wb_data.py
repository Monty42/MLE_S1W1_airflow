# dag scripts for World Bank Dataset
from collections import defaultdict
import os
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
    @task()
    def create_table():
        logging.info(LOG_FORMAT +  'Start the create table part')
        hook = PostgresHook('destination_db') # Подключение должны быть заранее установлено через UI airflow
        db_engine = hook.get_sqlalchemy_engine()
        # get_sqlalchemy_engine()
        # Create a metadata object
        metadata = MetaData()
        # Define the table structure
        wb_table = Table('wb_statistic_dag', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('project_id', String),
            Column('countryname_off', String),
            Column('countryname', String),
            Column('countrycode', String),
            Column('sector1', String),
            Column('year', String),
            Column('year_close', String),
            Column('totalamt', String),
            Column('vvp', Float),
            Column('population', Float),
            Column('target', Integer),
            UniqueConstraint('project_id', name='unique_project_constraint')
        )
        
        if not inspect(db_engine).has_table(wb_table.name):
            metadata.create_all(db_engine)

    @task()
    def extract():
        logging.info(LOG_FORMAT + 'Start the extract part')
        # Hooks
        logging.info(LOG_FORMAT + 'Connect to Posgres')
        # подключаем БД Posgres из которой будем брать большшую часть данных
        hook_psql = PostgresHook('source_db') # Подключение должны быть заранее установлено через UI airflow
        conn_psql = hook_psql.get_conn()

        # подключаем БД SQLite из которой будем брать данные численности с 1981 по 2000 года
        logging.info(LOG_FORMAT + 'Connect to SQLite')
        try: # подключиться к ДБ с помощью хуков, БД предварительно должна быть добавлена в Connections в админке Airflow
            hook_sqlite = SqliteHook('population_db') # Подключение устанавливается в локальную директорию ./tmp Dockecer образа
            conn_sqlite = hook_sqlite.get_conn()
            logging.info(LOG_FORMAT + 'Connect to SQLite with HOOK SUCSESS')
        except:
            # Если по какой-то причине не удалось подключиться, попробуем понять почему
            import sqlite3
            if sqlite3.connect('./tmp/population_data.db'):
                conn_sqlite = sqlite3.connect('./tmp/population_data.db')
                logging.info(LOG_FORMAT + 'Connect to SQLite with sqlite3 SUCSESS')
            else:
                if os.path.exists('./tmp/population_data.db'):
                    raise(LOG_FORMAT + 'Файл есть в контейнере, но неверные параметры подключения')
                else:
                    raise(LOG_FORMAT + 'Вы не переложили базу данных при создании контейнера')
        
        # формируем шаблон для импорта всех данных из БД
        sql_template = "select * from {}"

        # извлекаем данные численности из трех разных источников
        logging.info(LOG_FORMAT + 'Extract data')
        with open('/opt/airflow/tmp/population_data_1960_1980.json', 'r') as f:
            df_population_first = pd.read_json(f)
        
        df_population_second  = pd.read_sql(
            sql_template.format('population_data'), conn_sqlite)
        df_population_third = pd.read_sql(
            sql_template.format('s1w1_population_data_2001_2017'), conn_psql)
        
        # объединяем файл численности
        df_population = df_population_first.merge(
            df_population_second.merge(
                df_population_third,
                on=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'],
                how='left'
            ),
            on=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'],
            how='left'
        )

        logging.info(LOG_FORMAT + 'Extract other data')
        # достаем остальные данные из БД Posgres
        df_rural = pd.read_sql(sql_template.format('s1w1_rural_population_percent'), conn_psql)
        df_electricity = pd.read_sql(sql_template.format('s1w1_electricity_access_percent'), conn_psql)
        df_project = pd.read_sql(sql_template.format('s1w1_projects_data'), conn_psql)
        df_vvp = pd.read_sql(sql_template.format('s1w1_vvp_data'), conn_psql)
        logging.info('Closing connections')
        conn_psql.close()
        conn_sqlite.close()
        # данные по странам тоже импортируем, были подготовлены на этапе EDA
        with open('/opt/airflow/tmp/country_not_found_mapping.json', 'r') as f:
            country_not_found_mapping = json.load(f)
        with open('/opt/airflow/tmp/non_countries.json', 'r') as f:
            non_countries = json.load(f)
        # формируем словарь со всеми данными полученными на этом этапе
        data = {
            'df_population': df_population,
            'df_rural': df_rural,
            'df_electricity': df_electricity,
            'df_project': df_project,
            'df_vvp': df_vvp,
            'country_mapping': country_not_found_mapping,
            'non_countries': non_countries['non_countries']
        }
        del df_population, df_rural, df_electricity, df_project, df_vvp, country_not_found_mapping
        logging.info(LOG_FORMAT + 'End of extract data')
        # отдаем данные
        return data

    @task()
    def transform(data: Dict) -> pd.DataFrame:
        # Your code here
        pass

    @task()
    def load(data: pd.DataFrame):
        # Your code here
        pass
    
    # создаем таблицу
    create_table()
    # экспортируем данные
    data = extract()
    # преобразовываем данные
    # transformed_data = transform(data)
    # загружаем данные в целевую таблицу
    # load(transformed_data)


if __name__ == '__main__':
    prepare_wb_data()
