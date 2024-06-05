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
    """Create Table part for alternative DAG"""
    pass

def extract(**kwargs):
    """Extract task for alternative DAG"""
    # Данные должны передаваться с помощью `xcom_pull` и `xcom_push`
    pass

def transform(**kwargs):
    """Transform task for alternative DAG"""
    # Данные должны передаваться с помощью объекта TaskInstance и функции `xcom_pull` и `xcom_push`
    pass

def load(**kwargs):
    """Load task for alternative DAG"""
    # Данные должны передаваться с помощью объекта TaskInstance и функции `xcom_pull` и `xcom_push`
    pass