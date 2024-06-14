from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
from airflow.models import Variable


# Переменные задаются в веб-интерфейсе Airflow: Admin -> Variables
TELEGRAM_CONN_ID = Variable.get("telegram_conn_id", default_var="default_value")
TELEGRAM_TOKEN = Variable.get("telegram_token", default_var="default_value")
TELTGRAM_CHAT_ID = Variable.get("telegram_chat_id", default_var="default_value")


def send_telegram_task_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(
        telegram_conn_id=TELEGRAM_CONN_ID,
        token=TELEGRAM_TOKEN,
        chat_id=TELTGRAM_CHAT_ID,
    )
    task = context['task_instance_key_str']
    run_id = context['run_id']
    
    message = f'Исполнение TASK {task} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': TELTGRAM_CHAT_ID,
        'text': message,
    }) # отправление сообщения


def send_telegram_dag_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(
        telegram_conn_id=TELEGRAM_CONN_ID,
        token=TELEGRAM_TOKEN,
        chat_id=TELTGRAM_CHAT_ID,
    )
    dag = context['dag']
    run_id = context['run_id']

    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': TELTGRAM_CHAT_ID,
        'text': message,
        'parse_mode': None,  # чтобы не было ошибки при прасинге `context['dag']`
    }) # отправление сообщения


def send_telegram_failure_message(context): # на вход принимаем словарь со контекстными переменными
    message_hook = TelegramHook(
        telegram_conn_id=TELEGRAM_CONN_ID,
        token=TELEGRAM_TOKEN,
        chat_id=TELTGRAM_CHAT_ID,
    )
    dag = context['task_instance_key_str']
    run_id = context['run_id']

    message = f'Ошибка DAG {dag} с id={run_id}!' # определение текста сообщения
    message_hook.send_message({
        'chat_id': TELTGRAM_CHAT_ID,
        'text': message,
    }) # отправление сообщения


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    TELEGRAM_CONN_ID = os.getenv("TELEGRAM_CONN_ID")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    TELTGRAM_CHAT_ID = os.getenv("TELTGRAM_CHAT_ID")

    send_telegram_task_success_message({"task_instance_key_str": "test_task_instance_key_str", "run_id": "test_success"})
    send_telegram_failure_message({"task_instance_key_str": "test_task_instance_key_str", "run_id": "test_failure"})