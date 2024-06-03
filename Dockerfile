FROM apache/airflow:2.7.3-python3.10
# копируем файл в целевую директорию
COPY requirements.txt ./tmp/requirements.txt
COPY data/preprocess_data/ ./tmp/
# COPY data/preprocess_data/country_not_found_mapping.json ./tmp/country_not_found_mapping.json
# COPY data/preprocess_data/non_countries.json ./tmp/non_countries.json
# COPY data/preprocess_data/population_data_1960_1980.json ./tmp/population_data_1960_1980.json


# установка необходимых библиотек
RUN pip install -U pip
RUN pip install -r ./tmp/requirements.txt

