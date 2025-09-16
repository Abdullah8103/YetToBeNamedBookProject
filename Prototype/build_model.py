import pandas as pd
pd.set_option('display.max_columns', None)
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
import numpy as np
import joblib

def build_model():
    book_dataset = pd.read_csv('Data/cleaned_books.csv')

    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

    summary_array = book_dataset['summaries'].to_numpy()
    embeddings = model.encode(summary_array)
    embeddings_norm = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
    n_clusters = len(book_dataset['categories'].unique())
    kmeans = KMeans(n_clusters=n_clusters, random_state=42).fit(embeddings_norm)

    joblib.dump(kmeans, "models/kmeans.pkl")

if __name__ == "__main__":
    build_model()
