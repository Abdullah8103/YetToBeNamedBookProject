from ETL.Extract.csv_extractor import fetch_books_from_csv
from ETL.Transform.csv_cleaning import clean_csv_books
from ETL.Load.loader import save_books
import pandas as pd
pd.set_option('display.max_columns', None)

def run_pipeline():
    csv_raw = fetch_books_from_csv("data/books_summary.csv")

    csv_df = clean_csv_books(csv_raw)

    save_books(csv_df)

if __name__ == "__main__":
    run_pipeline()
