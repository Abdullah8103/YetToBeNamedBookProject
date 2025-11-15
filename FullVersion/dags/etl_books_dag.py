from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

from src.etl.extract import extract_gutenberg_titles_and_plots, extract_nyt_books
from src.etl.load import load_books_to_mongodb

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

dag = DAG(
    'etl_books',
    default_args=default_args,
    description='ETL DAG for Gutenberg + NYT books',
    schedule_interval="@daily",
    catchup=False
)


def etl_task():
    # Extract
    df_gutenberg = extract_gutenberg_titles_and_plots(num_books=50)
    df_nyt = extract_nyt_books(num_books=20)

    # Combine
    df_all = pd.concat([df_gutenberg, df_nyt], ignore_index=True)
    df_all = df_all.drop_duplicates(subset="title").reset_index(drop=True)

    # Load
    load_books_to_mongodb(df_all)


etl = PythonOperator(
    task_id="etl_books",
    python_callable=etl_task,
    dag=dag
)