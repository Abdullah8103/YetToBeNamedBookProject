import pandas as pd
def clean_csv_books(df):
    df = df.drop_duplicates(subset='book_name')
    df = df.dropna()

    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    df['summaries'] = df['summaries'].str.strip().str.replace('\xa0', ' ', regex=False)

    max_title_length = 100
    df = df[df['book_name'].str.len() <= max_title_length]

    return df