# plugins for wb_data pipeline
from collections import defaultdict
import json
import logging
import os
from typing import Dict, List

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from pycountry import countries
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    inspect,
)

LOG_FORMAT = "WB_DATA DAG - "


def create_table(**kwargs):
    """Create destination table if it does not exist."""
    logging.info(LOG_FORMAT + "Start the create table part")
    # Connection must be configured in Airflow UI
    hook = PostgresHook("destination_db")
    db_engine = hook.get_sqlalchemy_engine()

    metadata = MetaData()
    wb_table = Table(
        "alt_wb_statistic_dag",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("project_id_alt", String),
        Column("countryname_off", String),
        Column("countryname", String),
        Column("countrycode", String),
        Column("sector1", String),
        Column("year", String),
        Column("year_close", String),
        Column("totalamt", String),
        Column("vvp", Float),
        Column("population", Float),
        Column("target", Integer),
        UniqueConstraint("project_id_alt", name="unique_project_alt_constraint"),
    )

    if not inspect(db_engine).has_table(wb_table.name):
        metadata.create_all(db_engine)

def extract(**kwargs):
    """Extract raw data from different databases."""
    logging.info(LOG_FORMAT + "Start the extract part")
    logging.info(LOG_FORMAT + "Connect to Postgres")
    hook_psql = PostgresHook("source_db")
    conn_psql = hook_psql.get_conn()

    logging.info(LOG_FORMAT + "Connect to SQLite")
    try:
        hook_sqlite = SqliteHook("population_db")
        conn_sqlite = hook_sqlite.get_conn()
        logging.info(LOG_FORMAT + "Connect to SQLite with HOOK SUCCESS")
    except Exception as error:  # pragma: no cover - debug helper
        import sqlite3

        if sqlite3.connect("./tmp/population_data.db"):
            conn_sqlite = sqlite3.connect("./tmp/population_data.db")
            logging.info(LOG_FORMAT + "Connect to SQLite with sqlite3 SUCCESS")
        elif os.path.exists("./tmp/population_data.db"):
            raise ValueError(
                LOG_FORMAT
                + "Файл есть в контейнере, но неверные параметры подключения"
            ) from error
        else:
            raise ValueError(
                LOG_FORMAT + "Вы не переложили базу данных при создании контейнера"
            ) from error
    
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
    logging.info(LOG_FORMAT + 'Push data from extract part to task_instance')
    ti = kwargs['ti']
    ti.xcom_push('extracted_data', data)

def transform(**kwargs):
    """Transform raw data into final analytical dataset."""
    logging.info(LOG_FORMAT + "Start the transform part")

    def transform_projects(
        df: pd.DataFrame,
        country_mapping: Dict,
        non_countries: List[str],
    ) -> pd.DataFrame:
        logging.info(LOG_FORMAT + "Transform projects dataset")
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
    
    def transform_other(
        df: pd.DataFrame,
        non_countries: List[str],
        target_column: str,
    ) -> pd.DataFrame:
        """Prepare indicator data to a long format."""
        df.drop(columns=["Indicator Name", "Indicator Code"], inplace=True)
        df.drop_duplicates(subset=["Country Name", "Country Code"], inplace=True)
        df_melt = df.melt(
            id_vars=["Country Name", "Country Code"],
            var_name="year",
            value_name=f"{target_column}",
        )
        df_melt[f"{target_column}"] = (
            df_melt.sort_values("year")
            .groupby(["Country Name", "Country Code"])[f"{target_column}"]
            .fillna(method="ffill")
            .fillna(method="bfill")
        )
        df_melt = df_melt[~df_melt["Country Name"].isin(non_countries)]
        df_melt[f"{target_column}"] = df_melt[f"{target_column}"].astype(float)
        return df_melt

    logging.info(LOG_FORMAT + "Collect data from extract")
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="extract", key="extracted_data")
    logging.info(LOG_FORMAT + f"{type(data)}")
    logging.info(LOG_FORMAT + "Transform projects")
    df_project = transform_projects(
        data["df_project"],
        data["country_mapping"],
        data["non_countries"],
    )
    logging.info(LOG_FORMAT + "Transform other")
    df_vvp = transform_other(data["df_vvp"], data["non_countries"], "vvp")
    df_population = transform_other(
        data["df_population"],
        data["non_countries"],
        "population",
    )
    df_indicator = df_vvp.merge(
        df_population,
        on=("Country Name", "Country Code", "year"),
    )
    df_indicator.columns = [
        "countryname",
        "countrycode",
        "year",
        "vvp",
        "population",
    ]
    logging.info(
        LOG_FORMAT
        + "Number of clear data -- %s",
        df_indicator.countrycode.isna().sum(),
    )
    logging.info(
        LOG_FORMAT
        + "Number of clear data -- %s",
        df_project.countrycode.isna().sum(),
    )
    logging.info(LOG_FORMAT + "Merging data")
    # джоиним датафрейм проектов и датафрейм индикаторов
    df_project_meta = df_project.merge(
        df_indicator,
        on=('countrycode', 'year'),
        how='left',
    )
    # переименуем столбцы, так как при джоине были одинаковые колонки, но с разными значениями
    df_project_meta.rename(
        columns={
            "countryname_x": "countryname_off",
            "countryname_y": "countryname",
        },
        inplace=True,
    )
    df_project_meta.reset_index(drop=True, inplace=True)
    df_project_meta.rename(columns={"id": "project_id_alt"}, inplace=True)
    logging.info(LOG_FORMAT + f"Size of final data {df_project_meta.shape}")
    logging.info(LOG_FORMAT + "Push data from transform part to task_instance")
    ti.xcom_push("transformed_data", df_project_meta)

def load(**kwargs):
    """Load transformed data into destination table."""
    logging.info(LOG_FORMAT + "Collect data from transform")
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="transform", key="transformed_data")
    logging.info(LOG_FORMAT + f"{data.columns.tolist()}")
    hook = PostgresHook("destination_db")
    hook.insert_rows(
        table="alt_wb_statistic_dag",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=["project_id_alt"],
        rows=data.values.tolist(),
    )
