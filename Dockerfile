FROM apache/airflow:2.7.3-python3.10
# TODO: поставить флаг кастомной сборки в compose.yaml
# копируем файл в целевую директорию
COPY requirements.txt ./tmp/requirements.txt
COPY data/preprocess_data ./tmp/

# установка необходимых библиотек
RUN pip install -U pip
RUN pip install -r ./tmp/requirements.txt
