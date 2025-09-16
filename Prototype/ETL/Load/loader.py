import pandas as pd
def save_books(df, path="data/cleaned_books.csv"):
    df.to_csv(path, index=False)