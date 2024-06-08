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
from dotenv import load_dotenv
from pycountry import countries
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.decorators import dag, task
from sqlalchemy import (
    Table, Column, Float, Integer,
    MetaData, String, UniqueConstraint, inspect)

load_dotenv()

sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
LOG_FORMAT  = f'WB_DATA DAG - '

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["WorldBank", "ETL", "Test"],
    default_args={
        'retries': 3, 
        'retry_delay': pendulum.duration(minutes=1) 
    },
     catchup=False
)
def prepare_wb_data():
    @task(retries=3, retry_delay=pendulum.duration(minutes=1))
    def create_table():
        logging.info(LOG_FORMAT + 'Start the create table part')
        hook = PostgresHook('destination_db')  # Подключение должны быть заранее установлено через UI airflow
        db_engine = hook.get_sqlalchemy_engine()
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
            Column('totalamt', Float),
            Column('vvp', Float),
            Column('population', Float),
            Column('s1w1_electricity_access_percent', Float),
            Column('s1w1_rural_population_percent', Float),
            Column('target', Integer),
            UniqueConstraint('project_id', name='unique_project_constraint')
        )

        if not inspect(db_engine).has_table(wb_table.name):
            metadata.create_all(db_engine)

    @task(retries=3, retry_delay=pendulum.duration(minutes=1))
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

    @task(retries=3, retry_delay=pendulum.duration(minutes=1))
    def transform(data: Dict) -> pd.DataFrame:
        logging.info(LOG_FORMAT + 'Start the transform part')
        
        def transform_projects(
                df: pd.DataFrame, 
                country_mapping: Dict,
                non_countries: List[str],
            ) -> pd.DataFrame:
            logging.info(LOG_FORMAT +  'Transform projects dataset')
            # убираем лишнее в названии страны
            df['countryname'] = df['countryname'].str.split(';').str.get(0)
            # подготовливаем данные для поля `countrycode`
            project_country_abbrev_dict = defaultdict(str)
            for country in df['countryname'].drop_duplicates().sort_values():
                try:
                    project_country_abbrev_dict[country] = countries.lookup(country).alpha_3
                except: 
                    continue
            project_country_abbrev_dict.update(country_mapping)
            # добавляем столбец `countrycode` в датафрейм
            df['countrycode'] = df['countryname'].apply(lambda x: project_country_abbrev_dict[x])
            # работаем с датами, выбираем только год
            df['boardapprovaldate'] = pd.to_datetime(df['boardapprovaldate'])
            df['closingdate'] = pd.to_datetime(df['closingdate'])
            df['year'] = df['boardapprovaldate'].dt.year
            df['year_close'] = df['closingdate'].dt.year
            # заполняем пропуски для того, чтобы удалить элементы, у которых невозможно найти `countrycode`
            df.fillna('', inplace=True)
            df = df[df.countrycode != '']
            # создаем поле `target` на основе года закрытия
            df['target'] = df.year_close.apply(lambda x: 1 if x != '' else 0)
            # преобразуем столбцы с датами к формату строки (иначе постгрес будет ругаться)
            df['year'] = df['year'].astype(str).str.slice(stop=4)
            df['year_close'] = df['year_close'].astype(str).str.slice(stop=4)
            # чистим поле `sector1` от лишних символов
            df['sector1'] = df['sector1'].replace('!$!0', np.nan)
            df['sector1'] = df['sector1'].replace('!.+', '', regex=True)
            df['sector1'] = df['sector1'].replace('^(\(Historic\))', '', regex=True)
            # оставляем в датафрейме только страны
            df = df[~df['countryname'].isin(non_countries)]

            df['totalamt'] = df['totalamt'].replace('[\$,]', '', regex=True).astype(float)
            # выбираем только нужные столбцы
            df = df[['id', 'countryname', 'sector1', 'countrycode', 'totalamt', 'year', 'year_close', 'target']]
            return df
        
        def transform_other(df: pd.DataFrame, non_countries: List[str], target_column: str)  -> pd.DataFrame:
            # удаляем информацию об индикаторох, так как в одной таблице присутствует только один индикатор
            df.drop(columns=['Indicator Name', 'Indicator Code'], inplace=True)
            df.drop_duplicates(subset=['Country Name', 'Country Code'], inplace=True)
            # мелтим таблицу, чтобы получить ее вертикальную версию
            df_melt = df.melt(
                id_vars=['Country Name', 'Country Code'],
                var_name='year',
                value_name=f'{target_column}'
            )
            # в полученном датафрейме заполняем пустые значения
            df_melt[f'{target_column}'] = (
                df_melt.sort_values('year')
                        .groupby(['Country Name', 'Country Code'])[f'{target_column}']
                        .fillna(method='ffill')
                        .fillna(method='bfill')
            )
            # чистим поле страны от регионов
            df_melt = df_melt[~df_melt['Country Name'].isin(non_countries)]
            # переводим столбец индикатора к числовому типу
            df_melt[f'{target_column}'] = df_melt[f'{target_column}'].astype(float)
            return df_melt
        
        logging.info(LOG_FORMAT + 'Collect data from extract')
        logging.info(LOG_FORMAT + f'{type(data)}')
        logging.info(LOG_FORMAT + 'Transform projects')
        # работаем с файлом проектов
        df_project = transform_projects(data['df_project'], data['country_mapping'], data['non_countries'])
        logging.info(LOG_FORMAT + 'Transform other')
        # собираем общий файл с экономическими индикаторами
        df_vvp = transform_other(data['df_vvp'], data['non_countries'], 'vvp')
        df_population  = transform_other(data['df_population'], data['non_countries'], 'population')
        df_electricity = transform_other(data['df_electricity'], data['non_countries'], 's1w1_electricity_access_percent')
        df_rural = transform_other(data['df_rural'], data['non_countries'], 's1w1_rural_population_percent')
        
        df_indicator = df_vvp.merge(
            df_population,
            on=('Country Name', 'Country Code', 'year'),
        ).merge(
            df_electricity,
            on=('Country Name', 'Country Code', 'year'),
        ).merge(
            df_rural,
            on=('Country Name', 'Country Code', 'year'),
        )
        # оставляем только нужные столбца
        df_indicator.columns = ['countryname', 'countrycode', 'year', 'vvp', 'population', 's1w1_electricity_access_percent', 's1w1_rural_population_percent']
        logging.info(LOG_FORMAT + f'Number of clear data -- {df_indicator.countrycode.isna().sum()}')
        logging.info(LOG_FORMAT + f'Number of clear data -- {df_project.countrycode.isna().sum()}')
        logging.info(LOG_FORMAT + 'Merging data')
        # джоиним датафрейм проектов и датафрейм индикаторов
        df_project_meta = df_project.merge(
            df_indicator,
            on=('countrycode', 'year'),
            how='left',
        )
        # переименуем столбцы, так как при джоине были одинаковые колонки, но с разными значениями
        df_project_meta.rename(
            columns={
                'countryname_x': 'countryname_off',
                'countryname_y': 'countryname',
            },
            inplace=True,
        )
        df_project_meta.reset_index(drop=True, inplace=True)
        # переименуем поле `id`
        df_project_meta.rename(columns={'id': 'project_id'}, inplace=True)
        logging.info(LOG_FORMAT + f'Size of final data {df_project_meta.shape}')
        logging.info(LOG_FORMAT + 'Push data from transform part to task_instance')
        logging.info(LOG_FORMAT + 'End of transform part')
        return df_project_meta


    @task(retries=3, retry_delay=pendulum.duration(minutes=1))
    def load(data: pd.DataFrame):
        logging.info(LOG_FORMAT + f'{data.columns.tolist()}')
        # загружаем данные в целевую таблицу
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="wb_statistic_dag",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['project_id'],
            rows=data.values.tolist()
        )

    send_success_notification = TelegramOperator(
        task_id='send_success_notification',
        telegram_conn_id='dag_notification',
        chat_id='-4239968294',
        text='DAG prepare_wb_data completed successfully!',
    )

    send_failure_notification = TelegramOperator(
        task_id='send_failure_notification',
        telegram_conn_id='dag_notification',
        chat_id='-4239968294',
        text='DAG prepare_wb_data failed!',
        trigger_rule='one_failed'
    )

    create_table_task = create_table()
    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task)

    create_table_task >> extract_task >> transform_task >> load_task >> send_success_notification
    create_table_task >> extract_task >> transform_task >> load_task >> send_failure_notification

prepare_wb_data()

if __name__ == '__main__':
    prepare_wb_data()
