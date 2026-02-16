# DAG: World Bank ETL (split transforms)
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pendulum
from airflow.decorators import dag, task
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
    text,
)

# -------------------- CONFIG --------------------
LOG_PREFIX = "WB_DATA DAG - "
CONN_SRC_PG = "source_db"
CONN_DST_PG = "destination_db"
CONN_SQLITE = "population_db"

BASE_DIR = Path("/opt/airflow")
DATA_DIR = BASE_DIR / "data" / "wb_etl"      # промежуточные файлы
TMP_DIR = BASE_DIR / "tmp"

POP_JSON_1960_1980 = TMP_DIR / "population_data_1960_1980.json"
COUNTRY_MAP_JSON   = TMP_DIR / "country_not_found_mapping.json"
NON_COUNTRIES_JSON = TMP_DIR / "non_countries.json"

DST_TABLE = "wb_statistic_dag"

# -------------------- UTILS --------------------
def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str) -> None:
    logging.info(f"{LOG_PREFIX}{msg}")

def save_df(df: pd.DataFrame, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return str(path)

def read_df(path_str: str) -> pd.DataFrame:
    return pd.read_parquet(path_str)

# -------------------- DAG --------------------
@dag(
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["WorldBank", "ETL", "Prod"],
    default_args={"retries": 3, "retry_delay": timedelta(seconds=10)},
    catchup=False,
)
def prepare_wb_data_cheat():
    @task()
    def create_table() -> None:
        log("Create destination table if not exists")
        hook = PostgresHook(CONN_DST_PG)
        engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()

        wb_table = Table(
            DST_TABLE,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("project_id_3", String, nullable=False),
            Column("countryname_off", String),
            Column("countryname", String),
            Column("countrycode", String(3), index=True),
            Column("sector1", String),
            Column("year", Integer),
            Column("year_close", Integer),
            Column("totalamt", Float),
            Column("vvp", Float),
            Column("population", Float),
            Column("target", Integer),
            UniqueConstraint("project_id_3", name="project_id_3_constraint"),
        )
        if not inspect(engine).has_table(wb_table.name):
            metadata.create_all(engine)

    @task()
    def extract() -> Dict[str, str]:
        """
        Тянем:
          - PG: projects, vvp, population_2001_2017
          - SQLite: population_1981_2000
          - JSON: population_1960_1980
        Возвращаем пути к parquet + json-строки с мэппингами.
        """
        ensure_dirs()
        log("Extract: PG")
        hook_psql = PostgresHook(CONN_SRC_PG)
        with hook_psql.get_conn() as conn_psql:
            sql = "SELECT * FROM {}"
            df_project = pd.read_sql(sql.format("s1w1_projects_data"), conn_psql)
            df_vvp = pd.read_sql(sql.format("s1w1_vvp_data"), conn_psql)
            df_population_third = pd.read_sql(sql.format("s1w1_population_data_2001_2017"), conn_psql)

        log("Extract: SQLite")
        hook_sqlite = SqliteHook(CONN_SQLITE)
        with hook_sqlite.get_conn() as conn_sqlite:
            df_population_second = pd.read_sql("SELECT * FROM population_data", conn_sqlite)

        log("Extract: JSON 1960–1980")
        df_population_first = pd.read_json(POP_JSON_1960_1980)

        # merge population blocks (wide)
        on_cols = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]
        df_population = df_population_first.merge(
            df_population_second.merge(df_population_third, on=on_cols, how="left"),
            on=on_cols,
            how="left",
        )

        paths = {
            "projects_path": save_df(df_project, DATA_DIR / "raw_projects.parquet"),
            "vvp_path": save_df(df_vvp, DATA_DIR / "raw_vvp.parquet"),
            "population_path": save_df(df_population, DATA_DIR / "raw_population.parquet"),
        }

        with open(COUNTRY_MAP_JSON, "r") as f:
            country_mapping = json.load(f)
        with open(NON_COUNTRIES_JSON, "r") as f:
            non_countries = json.load(f)["non_countries"]

        return {
            **paths,
            "country_mapping": json.dumps(country_mapping),
            "non_countries": json.dumps(non_countries),
        }

    # ----------- TRANSFORM SUBTASKS -----------
    @task()
    def transform_projects_task(projects_path: str, country_mapping_json: str, non_countries_json: str) -> str:
        log("Transform: projects")
        df = read_df(projects_path).copy()
        country_mapping: Dict[str, str] = json.loads(country_mapping_json)
        non_countries: List[str] = json.loads(non_countries_json)

        df["countryname"] = df["countryname"].str.split(";").str[0].str.strip()

        # countrycode map via pycountry + fallback mapping
        unique_countries = df["countryname"].dropna().unique().tolist()
        code_map: Dict[str, str] = {}
        for name in unique_countries:
            code = None
            try:
                code = countries.lookup(name).alpha_3
            except Exception:
                pass
            if not code and name in country_mapping:
                code = country_mapping[name]
            if code:
                code_map[name] = code
        df["countrycode"] = df["countryname"].map(code_map)

        # dates -> years
        df["boardapprovaldate"] = pd.to_datetime(df["boardapprovaldate"], errors="coerce")
        df["closingdate"] = pd.to_datetime(df["closingdate"], errors="coerce")
        df["year"] = df["boardapprovaldate"].dt.year
        df["year_close"] = df["closingdate"].dt.year

        # target
        df["target"] = (df["year_close"].notna()).astype(int)

        # sector cleanup
        s = df["sector1"].astype(str)
        s = s.replace({"!$!0": np.nan}, regex=False)
        s = s.str.replace("!.+", "", regex=True)
        s = s.str.replace(r"^(\(Historic\))", "", regex=True).str.strip()
        df["sector1"] = s

        # numeric
        df["totalamt"] = pd.to_numeric(df.get("totalamt"), errors="coerce")

        # keep only valid countries and codes
        df = df.loc[df["countrycode"].notna() & ~df["countryname"].isin(non_countries)]

        df_out = (
            df[["id", "countryname", "sector1", "countrycode", "totalamt", "year", "year_close", "target"]]
            .rename(columns={"id": "project_id"})
        )
        return save_df(df_out, DATA_DIR / "tr_projects.parquet")

    @task()
    def transform_indicator_task(wide_parquet_path: str, non_countries_json: str, target_col: str) -> str:
        """
        Превращает wide-таблицу индикатора в long формат:
        [Country Name, Country Code, year, target_col] c ffill/bfill по странам.
        """
        log(f"Transform: indicator -> {target_col}")
        df = read_df(wide_parquet_path).copy()
        non_countries: List[str] = json.loads(non_countries_json)

        # один индикатор на таблицу -> удаляем мета-колонки
        df = (
            df.drop(columns=["Indicator Name", "Indicator Code"], errors="ignore")
            .drop_duplicates(subset=["Country Name", "Country Code"])
        )

        # wide -> long
        m = df.melt(
            id_vars=["Country Name", "Country Code"],
            var_name="year",
            value_name=target_col
        )

        # отсекаем регионы/агрегаты
        m = m[~m["Country Name"].isin(non_countries)].copy()

        # год -> числовой, сортировка стабильная внутри групп
        m["year"] = pd.to_numeric(m["year"], errors="coerce").astype("Int64")
        m.sort_values(["Country Name", "Country Code", "year"], inplace=True, kind="mergesort")

        # корректное заполнение пропусков с сохранением индекса
        g = m.groupby(["Country Name", "Country Code"], sort=False, observed=True)[target_col]
        filled = g.transform(lambda s: s.ffill().bfill())

        # финальное приведение типов
        m[target_col] = pd.to_numeric(filled, errors="coerce").astype(float)

        # (опционально) лог простых DQ-метрик
        null_share = m[target_col].isna().mean()
        log(f"Transform: {target_col} null share after fill = {null_share:.4%}")

        out = m[["Country Name", "Country Code", "year", target_col]]
        return save_df(out, DATA_DIR / f"tr_indicator_{target_col}.parquet")

    @task()
    def merge_indicators_task(vvp_long_path: str, pop_long_path: str) -> str:
        """
        Джойнит два long-индикатора в один датафрейм:
        [countryname, countrycode, year, vvp, population]
        """
        log("Transform: merge indicators")
        df_v = read_df(vvp_long_path)
        df_p = read_df(pop_long_path)

        df_ind = (
            df_v.merge(df_p, on=["Country Name", "Country Code", "year"], how="inner")
            .rename(columns={"Country Name": "countryname", "Country Code": "countrycode"})
            [["countryname", "countrycode", "year", "vvp", "population"]]
        )
        return save_df(df_ind, DATA_DIR / "tr_indicators.parquet")

    @task()
    def join_projects_with_indicators_task(projects_tr_path: str, indicators_path: str) -> str:
        """
        Финальный джойн проектов и индикаторов по [countrycode, year].
        """
        log("Transform: join projects + indicators")
        df_prj = read_df(projects_tr_path)
        df_ind = read_df(indicators_path)

        df_final = df_prj.merge(df_ind, on=["countrycode", "year"], how="left")
        df_final = df_final.rename(
            columns={"countryname_x": "countryname_off", "countryname_y": "countryname"}
        )

        # нормализация типов
        for col in ["year", "year_close"]:
            df_final[col] = pd.to_numeric(df_final[col], errors="coerce").astype("Int64")
        for col in ["vvp", "population", "totalamt"]:
            df_final[col] = pd.to_numeric(df_final[col], errors="coerce")
        df_final["target"] = df_final["target"].fillna(0).astype(int)

        log(f"Transform: final shape {df_final.shape}")
        return save_df(df_final, DATA_DIR / "wb_final.parquet")

    # ----------- LOAD -----------
    @task()
    def load(final_parquet_path: str) -> None:
        """
        UPSERT по project_id в {DST_TABLE}.
        """
        df = read_df(final_parquet_path)
        cols = [
            "project_id",
            "countryname_off",
            "countryname",
            "countrycode",
            "sector1",
            "year",
            "year_close",
            "totalamt",
            "vvp",
            "population",
            "target",
        ]
        df = df[cols].copy()

        hook = PostgresHook(CONN_DST_PG)
        engine = hook.get_sqlalchemy_engine()
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "project_id"])
        insert_sql = text(
            f"""
            INSERT INTO {DST_TABLE} ({", ".join(cols)})
            VALUES ({", ".join([f":{c}" for c in cols])})
            ON CONFLICT (project_id)
            DO UPDATE SET {set_clause}
            """
        )

        batch = 5_000
        log(f"Load: upserting {len(df)} rows")
        with engine.begin() as conn:
            for i in range(0, len(df), batch):
                conn.execute(insert_sql, df.iloc[i:i+batch].to_dict(orient="records"))
        log("Load: done")

    # ---------- GRAPH ----------
    create_table_task = create_table()

    extracted = extract()
    # разбор XCom
    projects_path = extracted["projects_path"]
    vvp_path = extracted["vvp_path"]
    population_path = extracted["population_path"]
    country_mapping_json = extracted["country_mapping"]
    non_countries_json = extracted["non_countries"]

    # T: проекты
    tr_projects = transform_projects_task(projects_path, country_mapping_json, non_countries_json)

    # T: индикаторы по отдельности
    vvp_long = transform_indicator_task(vvp_path, non_countries_json, target_col="vvp")
    pop_long = transform_indicator_task(population_path, non_countries_json, target_col="population")
    
    # T: мердж индикаторов
    indicators = merge_indicators_task(vvp_long, pop_long)

    # T: финальный джойн
    final_path = join_projects_with_indicators_task(tr_projects, indicators)

    # L: загрузка
    create_table_task >> final_path >> load(final_path)


prepare_wb_data_cheat()
