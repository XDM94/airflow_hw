from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import os
import sys
import logging

# Добавляем путь к скрипту трансформации
sys.path.append('/opt/airflow/scripts')
from transform_script import transform  # Импортируем функцию трансформации

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Пути к файлам
data_file = '/opt/airflow/data/profit_table.csv'
output_file = '/opt/airflow/data/flags_activity.csv'


def extract():
    """Извлечение данных из CSV."""
    logging.info("Чтение данных из CSV...")
    df = pd.read_csv(data_file)
    logging.info(f"Данные извлечены: {len(df)} строк.")
    return df


def transform_data(**context):
    """Трансформация данных."""
    df = context['ti'].xcom_pull(task_ids='extract_task')
    if df is None:
        raise ValueError("Данные не извлечены!")
    logging.info("Применение трансформации...")
    transformed_df = transform(pd.DataFrame(df))
    logging.info("Трансформация завершена.")
    return transformed_df.to_dict()  # Возвращаем как словарь для передачи через XCom


def load(**context):
    """Загрузка данных в CSV."""
    transformed_df = context['ti'].xcom_pull(task_ids='transform_task')
    if transformed_df is None:
        raise ValueError("Данные не трансформированы!")
    df = pd.DataFrame(transformed_df)
    logging.info("Запись данных в CSV...")
    file_exists = os.path.isfile(output_file)
    df.to_csv(output_file, mode='a', header=not file_exists, index=False)
    logging.info("Данные успешно записаны.")


# Определяем DAG
dag = DAG(
    'etl_process',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 10, 5),
        'retries': 1,
    },
    schedule_interval='@monthly',  # Запускать каждый месяц
    catchup=False,
)

# Операторы для задач
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    provide_context=True,  # Для передачи XCom
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,  # Для передачи XCom
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,  # Для передачи XCom
    dag=dag,
)

# Определяем последовательность задач
extract_task >> transform_task >> load_task
