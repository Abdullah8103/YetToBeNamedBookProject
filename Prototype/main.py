import pandas as pd
pd.set_option('display.max_columns', None)
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
import numpy as np
import joblib

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

kmeans = joblib.load("models/kmeans.pkl")
labels = kmeans.labels_

book_dataset = pd.read_csv('Data/cleaned_books.csv')
n_clusters = len(book_dataset['categories'].unique())

summary_array = book_dataset['summaries'].to_numpy()
embeddings = model.encode(summary_array)
embeddings_norm = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

for i in range(n_clusters):
    print(f"\nCluster {i} sample titles:")
    cluster_books = book_dataset[labels == i]['book_name'].head(5).tolist()
    for title in cluster_books:
        print(" -", title)

user_input = "a book about e-commerce companies"
user_embedding = model.encode(user_input)
user_embedding_norm = user_embedding / np.linalg.norm(user_embedding)
centroids_norm = kmeans.cluster_centers_ / np.linalg.norm(kmeans.cluster_centers_, axis=1, keepdims=True)
cosine_sims = np.dot(centroids_norm, user_embedding_norm)
closest_cluster_idx = np.argmax(cosine_sims)

print("Closest cluster index:", closest_cluster_idx)

cluster_books_idx = np.where(labels == closest_cluster_idx)[0]
cluster_embeddings = embeddings_norm[cluster_books_idx]
similarities = np.dot(cluster_embeddings, user_embedding_norm)
top5_idx_within_cluster = similarities.argsort()[::-1][:5]
top5_books_idx = cluster_books_idx[top5_idx_within_cluster]

print("Top 5 similar books:")
for idx in top5_books_idx:
    print(" -", book_dataset.iloc[idx]['book_name'])

for idx in top5_books_idx:
    title = book_dataset.iloc[idx]['book_name']
    summary = book_dataset.iloc[idx]['summaries']
    print(f"Title: {title}\nSummary: {summary}\n{'-'*60}")