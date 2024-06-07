# ETL-pipeline for World Bank Dataset
from collections import defaultdict
import os
import sys
import json
import logging
from typing import Dict, List

import numpy as np
import pandas as pd
from pycountry import countries
from dotenv import load_dotenv
from sqlalchemy import create_engine

sys.path.append('../')
logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
LOG_FORMAT  = f'WB_DATA ETL - '

def prepare_wb_data() -> None:
    """ETL-pipeline for WorldBank Dataset"""

    def _create_engine(source: str):
        """Create engine for `source` database.
        
        For example:
        If `source = "DESTINATION"`, then load envirment from `.env` file with keys:
            - DB_DESTINATION_HOST
            - DB_DESTINATION_PORT
            - DB_DESTINATION_USER
            - DB_DESTINATION_PASSWORD
            - DB_DESTINATION_NAME
        After that create engine with `sqlalchemy.create_engine()`
        """
        load_dotenv()
        host = os.environ.get(f'DB_{source}_HOST')
        port = os.environ.get(f'DB_{source}_PORT')
        username = os.environ.get(f'DB_{source}_USER')
        password = os.environ.get(f'DB_{source}_PASSWORD')
        db = os.environ.get(f'DB_{source}_NAME')
        
        return create_engine(
            f'postgresql://{username}:{password}@{host}:{port}/{db}')

    def extract() -> Dict:
        """Extract data from different sources.

        Returns:
            Dict: dictionary with different data in format:
                "key_data": dataframe/data/else
        """
        logging.info(LOG_FORMAT + 'Start the extract part')
        # Engines
        logging.info(LOG_FORMAT + 'Connect to DataBases')
        engine_psql = _create_engine('SOURCE')
        engine_sqlite = create_engine('sqlite:///data/preprocess_data/population_data.db')
        # вы можете выбрать другое ядро подключения к базе SQLite
        # import sqlite3
        # engine_sqllite = sqlite3.connect('data/clear_data/population_data.db')

        # формируем шаблон для импорта всех данных из БД
        sql_template = "select * from {}"

        # извлекаем данные численности из трех разных источников
        logging.info(LOG_FORMAT + 'Extract data')
        with open('data/preprocess_data/population_data_1960_1980.json', 'r') as f:
            df_population_first = pd.read_json(f)
        
        df_population_second  = pd.read_sql(
            sql_template.format('population_data'), engine_sqlite)
        df_population_third = pd.read_sql(
            sql_template.format('s1w1_population_data_2001_2017'), engine_psql)
        
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
        df_rural = pd.read_sql(sql_template.format('s1w1_rural_population_percent'), engine_psql)
        df_electricity = pd.read_sql(sql_template.format('s1w1_electricity_access_percent'), engine_psql)
        df_project = pd.read_sql(sql_template.format('s1w1_projects_data'), engine_psql)
        df_vvp = pd.read_sql(sql_template.format('s1w1_vvp_data'), engine_psql)
        # данные по странам тоже импортируем, были подготовлены на этапе EDA
        with open('data/preprocess_data/country_not_found_mapping.json', 'r') as f:
            country_not_found_mapping = json.load(f)
        with open('data/preprocess_data/non_countries.json', 'r') as f:
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

    def transform(data: Dict) -> pd.DataFrame:
        """Transform part of ETL-pipeline

        Args:
            data (Dict): data from extract part

        Returns:
            pd.DataFrame: result data for load pfrt
        """
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
        # получаем данне с шага extract
        logging.info(LOG_FORMAT + f'{type(data)}')
        logging.info(LOG_FORMAT + 'Transform projects')
        # работаем с файлом проектов
        df_project = transform_projects(data['df_project'], data['country_mapping'], data['non_countries'])
        logging.info(LOG_FORMAT + 'Transform other')
        # собираем общий файл с экономическими индикаторами
        df_vvp = transform_other(data['df_vvp'], data['non_countries'], 'vvp')
        df_population  = transform_other(data['df_population'], data['non_countries'], 'population')
        df_indicator = df_vvp.merge(
            df_population,
            on=('Country Name', 'Country Code', 'year'),
        )
        # оставляем только нужные столбца
        df_indicator.columns = ['countryname', 'countrycode', 'year', 'vvp', 'population']
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
        logging.info(LOG_FORMAT + f'Size of final data {df_project_meta.shape}')
        logging.info(LOG_FORMAT + 'End of transform part')
        return df_project_meta

    def load(data: pd.DataFrame):
        """Load part for ETL-pipeline

        Args:
            data (pd.DataFrame): Data from transform part
        """
        logging.info(LOG_FORMAT + 'Start the load part')
        try:
            # Подключаемся к целевой БД
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

    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


if __name__ == '__main__':
    prepare_wb_data()
    