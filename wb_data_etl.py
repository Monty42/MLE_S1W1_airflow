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
        pass

    def extract() -> Dict:
        """Extract data from different sources.

        Returns:
            Dict: dictionary with different data in format:
                "key_data": dataframe/data/else
        """
        pass

    def transform(data: Dict) -> pd.DataFrame:
        """Transform part of ETL-pipeline

        Args:
            data (Dict): data from extract part

        Returns:
            pd.DataFrame: result data for load pfrt
        """
        pass

    def load(data: pd.DataFrame):
        """Load part for ETL-pipeline

        Args:
            data (pd.DataFrame): Data from transform part
        """
        pass
    
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


if __name__ == '__main__':
    prepare_wb_data()
