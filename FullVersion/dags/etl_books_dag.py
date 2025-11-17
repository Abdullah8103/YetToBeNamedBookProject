from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your extract/load functions
from src.etl.extract import extract_gutenberg_titles_and_plots, fetch_nyt_bestsellers
from src.etl.load import load_books_to_mongodb

RAW_DATA_DIR = "/mnt/c/Users/abbas/Documents/GitHub/YetToBeNamedBookProject/FullVersion/data/raw"
COMBINED_DATA_FILE = os.path.join(RAW_DATA_DIR, "all_books.csv")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# MongoDB connection URI
MONGO_URI = "mongodb+srv://abbassimuhammadabdullah_db_user:Easy8103@yettobenamedbookproject.bvjbjom.mongodb.net/?appName=YetToBeNamedBookProject"

# Create Mongo client once
client = MongoClient(MONGO_URI, server_api=ServerApi("1"))

def extract_gutenberg_task(**kwargs):
    df = extract_gutenberg_titles_and_plots(num_books=500)
    output_path = os.path.join(RAW_DATA_DIR, "gutenberg_books.csv")
    print(f"Writing Gutenberg CSV to {output_path}")
    df.to_csv(output_path, index=False)

list_names = ['hardcover-fiction', 'hardcover-nonfiction']  # example
start_date = datetime(2005, 1, 1)
end_date = datetime(2025, 11, 15)

def extract_nyt_task(**kwargs):
    df = fetch_nyt_bestsellers(list_names, start_date, end_date)
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    df.to_csv(os.path.join(RAW_DATA_DIR, "nyt_books.csv"), index=False)


def combine_books_task():
    # Load Gutenberg books
    gutenberg_file = os.path.join(RAW_DATA_DIR, "gutenberg_books.csv")
    if not os.path.exists(gutenberg_file):
        raise FileNotFoundError(f"Gutenberg file not found: {gutenberg_file}")

    df_gutenberg = pd.read_csv(gutenberg_file)

    # Find all NYT CSV files
    nyt_files = [
        os.path.join(RAW_DATA_DIR, f)
        for f in os.listdir(RAW_DATA_DIR)
        if f.endswith(".csv") and "hardcover" in f
    ]

    if not nyt_files:
        raise FileNotFoundError("No NYT CSV files found in RAW_DATA_DIR")

    # Load all NYT CSVs
    df_nyt_list = [pd.read_csv(f) for f in nyt_files]

    # Combine Gutenberg + NYT
    df_all = pd.concat([df_gutenberg] + df_nyt_list, ignore_index=True)

    # Save combined CSV
    df_all.to_csv(COMBINED_DATA_FILE, index=False)
    print(f"Combined {len(df_all)} books into {COMBINED_DATA_FILE}")

def transform_books_task(**kwargs):
    import pandas as pd
    from src.etl.transform import transform_books

    df = pd.read_csv(COMBINED_DATA_FILE)
    df = transform_books(df)
    df.to_csv(os.path.join(RAW_DATA_DIR, "all_books_clean.csv"), index=False)
def load_to_mongo_task(**kwargs):
    import pandas as pd
    df = pd.read_csv(os.path.join(RAW_DATA_DIR, "all_books_clean.csv"))
    load_books_to_mongodb(df, client=client, db_name="books_db", collection_name="all_books")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

with DAG(
        dag_id='etl_books',
        default_args=default_args,
        description='ETL for books from NYT and Gutenberg',
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract_gutenberg", python_callable=extract_gutenberg_task)
    t2 = PythonOperator(task_id="extract_nyt", python_callable=extract_nyt_task)
    t3 = PythonOperator(task_id="combine_books", python_callable=combine_books_task)
    t4 = PythonOperator(task_id="transform_books", python_callable=transform_books_task)
    t5 = PythonOperator(task_id="load_to_mongo", python_callable=load_to_mongo_task)

    [t1, t2] >> t3 >> t4 >> t5