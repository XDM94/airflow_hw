from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from transform_script import transform  # Импортируем функцию трансформации

# Определяем пути к файлам
BASE_DIR = '/home/usr1/PycharmProjects/airflow_hw/data/profit_table.csv'
PROFIT_TABLE = os.path.join(BASE_DIR, 'profit_table.csv')
FLAGS_ACTIVITY = os.path.join(BASE_DIR, 'flags_activity.csv')


# Функция для этапа Extract
def extract(**kwargs):
    if not os.path.exists(PROFIT_TABLE):
        raise FileNotFoundError(f"Файл {PROFIT_TABLE} не найден!")
    df = pd.read_csv(PROFIT_TABLE)
    kwargs['ti'].xcom_push(key='extracted_data', value=df.to_dict())


# Функция для этапа Transform
def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract')
    df = pd.DataFrame.from_dict(data)
    result = transform(df)
    kwargs['ti'].xcom_push(key='transformed_data', value=result.to_dict())


# Функция для этапа Load
def load(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    df = pd.DataFrame.from_dict(transformed_data)

    # Сохраняем данные с добавлением в файл, если он уже существует
    if os.path.exists(FLAGS_ACTIVITY):
        existing_data = pd.read_csv(FLAGS_ACTIVITY)
        df = pd.concat([existing_data, df]).drop_duplicates()
    df.to_csv(FLAGS_ACTIVITY, index=False)


# Определяем DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'customer_activity_etl',
        default_args=default_args,
        description='ETL процесс для расчета витрины активности клиентов',
        schedule_interval='0 0 5 * *',  # Запуск каждый месяц 5-го числа
        start_date=datetime(2024, 3, 1),
        catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
