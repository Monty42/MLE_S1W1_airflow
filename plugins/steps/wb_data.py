# plugins for wb_data pipeline
from collections import defaultdict
import sys
import json
import logging
from typing import Dict, List

import pandas as pd
import numpy as np
from pycountry import countries
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import (
    Table, Column, Float, Integer,
    MetaData, String, UniqueConstraint, inspect)

sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
LOG_FORMAT  = f'WB_DATA DAG - '

def create_table(**kwargs):
    logging.info(LOG_FORMAT +  'Start the create table part')
    hook = PostgresHook('destination_db') # Подключение должны быть заранее установлено через UI airflow
    db_engine = hook.get_sqlalchemy_engine()
    # get_sqlalchemy_engine()
    # Create a metadata object
    metadata = MetaData()
    # Define the table structure
    wb_table = Table('alt_wb_statistic_dag', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('project_id_alt', String),
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
        UniqueConstraint('project_id_alt', name='unique_project_alt_constraint')
    )
    
    if not inspect(db_engine).has_table(wb_table.name):
        metadata.create_all(db_engine)

def extract(**kwargs):
    """
    #### Extract task
    """
    logging.info(LOG_FORMAT + 'Start the extract part')
    # Hooks
    logging.info(LOG_FORMAT + 'Connect to DataBases')
    hook_psql = PostgresHook('source_db') # Подключение должны быть заранее установлено через UI airflow
    hook_sqllite = SqliteHook('population_db') # Подключение устанавливается в локальную директорию ./tmp Dockecer образа
    # Connections
    logging.info(LOG_FORMAT + 'Connect to DataBases')
    conn_psql = hook_psql.get_conn()
    conn_sqllite = hook_sqllite.get_conn()
    # Selection Template
    sql_template = "select * from {}"
    
    # Extract data from different datasources
    logging.info(LOG_FORMAT + 'Extract data')
    with open('/opt/airflow/tmp/population_data_1960_1980.json', 'r') as f:
        df_population_first = pd.read_json(f)
    
    df_population_second  = pd.read_sql(
        sql_template.format('population_data_1981_2000'), conn_sqllite)
    df_population_third = pd.read_sql(
        sql_template.format('s1w1_population_data_2001_2017'), conn_psql)
    
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
    df_rural = pd.read_sql(sql_template.format('s1w1_rural_population_percent'), conn_psql)
    df_electricity = pd.read_sql(sql_template.format('s1w1_electricity_access_percent'), conn_psql)
    df_project = pd.read_sql(sql_template.format('s1w1_projects_data'), conn_psql)
    df_vvp = pd.read_sql(sql_template.format('s1w1_vvp_data'), conn_psql)
    logging.info('Closing connections')
    conn_psql.close()
    conn_sqllite.close()
    with open('/opt/airflow/tmp/country_not_found_mapping.json', 'r') as f:
        country_not_found_mapping = json.load(f)
    with open('/opt/airflow/tmp/non_countries.json', 'r') as f:
        non_countries = json.load(f)

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
    logging.info(LOG_FORMAT + 'Push data from extract part to task_instance')
    ti = kwargs['ti']
    ti.xcom_push('extracted_data', data)

def transform(**kwargs):
    """
    #### Transform task
    """
    logging.info(LOG_FORMAT + 'Start the transform part')
    def transform_projects(
            df: pd.DataFrame, 
            country_mapping: Dict,
            non_countries: List[str],
        ) -> pd.DataFrame:
        logging.info(LOG_FORMAT +  'Transform projects dataset')
        df['countryname'] = df['countryname'].str.split(';').str.get(0)
        project_country_abbrev_dict = defaultdict(str)
        for country in df['countryname'].drop_duplicates().sort_values():
            try:
                project_country_abbrev_dict[country] = countries.lookup(country).alpha_3
            except: 
                continue
        project_country_abbrev_dict.update(country_mapping)
        df['countrycode'] = df['countryname'].apply(lambda x: project_country_abbrev_dict[x])
        df['boardapprovaldate'] = pd.to_datetime(df['boardapprovaldate'])
        df['closingdate'] = pd.to_datetime(df['closingdate'])
        df['year'] = df['boardapprovaldate'].dt.year
        df['year_close'] = df['closingdate'].dt.year
        df.fillna('', inplace=True)
        df = df[df.countrycode != '']
        df['target'] = df.year_close.apply(lambda x: 1 if x != '' else 0)
        df['year'] = df['year'].astype(str).str.slice(stop=4)
        df['year_close'] = df['year_close'].astype(str).str.slice(stop=4)
        
        df['sector1'] = df['sector1'].replace('!$!0', np.nan)
        df['sector1'] = df['sector1'].replace('!.+', '', regex=True)
        df['sector1'] = df['sector1'].replace('^(\(Historic\))', '', regex=True)
        df = df[~df['countryname'].isin(non_countries)]
        df = df[['id', 'countryname', 'sector1', 'countrycode', 'totalamt', 'year', 'year_close', 'target']]
        return df
    
    def transform_other(df: pd.DataFrame, non_countries: List[str], target_column: str)  -> pd.DataFrame:
        df.drop(columns=['Indicator Name', 'Indicator Code'], inplace=True)
        df.drop_duplicates(subset=['Country Name', 'Country Code'], inplace=True)
        df_melt = df.melt(
            id_vars=['Country Name', 'Country Code'],
            var_name='year',
            value_name=f'{target_column}'
        )
        df_melt[f'{target_column}'] = (
            df_melt.sort_values('year')
                    .groupby(['Country Name', 'Country Code'])[f'{target_column}']
                    .fillna(method='ffill')
                    .fillna(method='bfill')
        )
        df_melt = df_melt[~df_melt['Country Name'].isin(non_countries)]
        df_melt[f'{target_column}'] = df_melt[f'{target_column}'].astype(float)
        return df_melt
    logging.info(LOG_FORMAT + 'Collect data from extract')
    ti = kwargs['ti'] # получение объекта task_instance
    data = ti.xcom_pull(task_ids='extract', key='extracted_data') # выгрузка данных из task_instance
    logging.info(LOG_FORMAT + f'{type(data)}')
    logging.info(LOG_FORMAT + 'Transform projects')
    df_project = transform_projects(data['df_project'], data['country_mapping'], data['non_countries'])
    logging.info(LOG_FORMAT + 'Transform other')
    df_vvp = transform_other(data['df_vvp'], data['non_countries'], 'vvp')
    df_population  = transform_other(data['df_population'], data['non_countries'], 'population')
    df_indicator = df_vvp.merge(
        df_population,
        on=('Country Name', 'Country Code', 'year'),
    )
    df_indicator.columns = ['countryname', 'countrycode', 'year', 'vvp', 'population']
    logging.info(LOG_FORMAT + f'Number of clear data -- {df_indicator.countrycode.isna().sum()}')
    logging.info(LOG_FORMAT + f'Number of clear data -- {df_project.countrycode.isna().sum()}')
    logging.info(LOG_FORMAT + 'Merging data')
    df_project_meta = df_project.merge(
        df_indicator,
        on=('countrycode', 'year'),
        how='left',
    )
    df_project_meta.rename(
        columns={
            'countryname_x': 'countryname_off',
            'countryname_y': 'countryname',
        },
        inplace=True,
    )
    df_project_meta.reset_index(drop=True, inplace=True)
    df_project_meta.rename(columns={'id': 'project_id_alt'}, inplace=True)
    logging.info(LOG_FORMAT + f'Size of final data {df_project_meta.shape}')
    logging.info(LOG_FORMAT + 'Push data from transform part to task_instance')
    ti.xcom_push('transformed_data', df_project_meta) # вместо return отправляем данные передатчику task_instance

def load(**kwargs):
    """
    #### Load task
    """
    logging.info(LOG_FORMAT + 'Collect data from transform')
    ti = kwargs['ti'] # получение объекта task_instance
    data = ti.xcom_pull(task_ids='transform', key='transformed_data') 
    logging.info(LOG_FORMAT + f'{data.columns.tolist()}')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="alt_wb_statistic_dag",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist()
    )