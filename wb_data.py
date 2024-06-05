# ETL-pipeline for World Bank Dataset
from collections import defaultdict
import os
import sys
import json
import logging
from typing import Dict, List
from datetime import datetime

import sqlite3
import numpy as np
import pandas as pd
from pycountry import countries
from dotenv import load_dotenv
from sqlalchemy import (
    Table, Column, DateTime, Float, Integer,
    MetaData, String, UniqueConstraint, create_engine, inspect)

sys.path.append('../')
logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
LOG_FORMAT  = f'WB_DATA ETL - '

def prepare_wb_data() -> None:
    """ETL-pipeline for WorldBank Dataset"""

    def _create_engine(source: str):
        load_dotenv()
        host = os.environ.get(f'DB_{source}_HOST')
        port = os.environ.get(f'DB_{source}_PORT')
        username = os.environ.get(f'DB_{source}_USER')
        password = os.environ.get(f'DB_{source}_PASSWORD')
        db = os.environ.get(f'DB_{source}_NAME')
        
        return create_engine(
            f'postgresql://{username}:{password}@{host}:{port}/{db}')


    def create_table():
        logging.info(LOG_FORMAT +  'Start the create table part')
        db_engine = _create_engine('DESTINATION')
        # Create a metadata object
        metadata = MetaData()
        # Define the table structure
        wb_table = Table('wb_statistic_etl', metadata,
            Column('index', Integer, primary_key=True, autoincrement=True),
            Column('project_id', String),
            Column('countryname_off', String),
            Column('countryname', String),
            Column('country_code', String),
            Column('year', DateTime),
            Column('totalamt', Integer),
            Column('vvp', Float),
            Column('population', Float),
            Column('target', Integer),
            UniqueConstraint('index', name='unique_index_constraint')
        )
        if not inspect(db_engine).has_table(wb_table.name):
            metadata.create_all(db_engine)

    def extract() -> Dict:
        logging.info(LOG_FORMAT + 'Start the extract part')
        # Engines
        logging.info(LOG_FORMAT + 'Connect to DataBases')
        engine_psql = _create_engine('SOURCE')
        engine_sqlite = create_engine('sqlite:///data/preprocess_data/population_data.db')
        # second type connection for sqlite
        # import sqlite3
        # engine_sqllite = sqlite3.connect('data/clear_data/population_data.db')

        # Selection Template
        sql_template = "select * from {}"
        
        # Extract data from different datasources
        logging.info(LOG_FORMAT + 'Extract data')

        logging.info(LOG_FORMAT + 'Extract population data')
        with open('data/preprocess_data/population_data_1960_1980.json', 'r') as f:
            df_population_first = pd.read_json(f)
        
        df_population_second  = pd.read_sql('population_data', engine_sqlite)
        df_population_third = pd.read_sql(
            sql_template.format('s1w1_population_data_2001_2017'), engine_psql)
        
        logging.info(LOG_FORMAT + 'Merge population data')
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
        df_rural = pd.read_sql(sql_template.format('s1w1_rural_population_percent'), engine_psql)
        df_electricity = pd.read_sql(sql_template.format('s1w1_electricity_access_percent'), engine_psql)
        df_project = pd.read_sql(sql_template.format('s1w1_projects_data'), engine_psql)
        df_vvp = pd.read_sql(sql_template.format('s1w1_vvp_data'), engine_psql)
        with open('data/preprocess_data/country_not_found_mapping.json', 'r') as f:
            country_not_found_mapping = json.load(f)
        with open('data/preprocess_data/non_countries.json', 'r') as f:
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
        logging.info(LOG_FORMAT + 'End of extract data')
        return data

    def transform(data: Dict) -> pd.DataFrame:
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
            return df_melt
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
        logging.info(LOG_FORMAT + f'Size of final data {df_project_meta.shape}')
        logging.info(LOG_FORMAT + 'End of transform part')
        return df_project_meta

    def load(data: pd.DataFrame):
        logging.info(LOG_FORMAT + 'Start the load part')
        try:
            db_engine = _create_engine('DESTINATION')
            data.to_sql(
                'wb_statistic_etl',
                con=db_engine,
                index=True,
                if_exists='append'
            )
            logging.info(LOG_FORMAT + 'Loading status: OK')
        except:
            logging.info(LOG_FORMAT + 'Loading status: FAIL')
        
    # create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


if __name__ == '__main__':
    prepare_wb_data()
