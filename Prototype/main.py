import pandas as pd
pd.set_option('display.max_columns', None)
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
import numpy as np

book_dataset = pd.read_csv("books_summary.csv")
book_dataset = book_dataset.drop_duplicates(subset='book_name')
book_dataset = book_dataset.dropna()

if 'Unnamed: 0' in book_dataset.columns:
    book_dataset = book_dataset.drop(columns=['Unnamed: 0'])

book_dataset['summaries'] = book_dataset['summaries'].str.strip().str.replace('\xa0', ' ', regex=False)
max_title_length = 100
book_dataset = book_dataset[book_dataset['book_name'].str.len() <= max_title_length]

print(book_dataset.info())
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
summary_array = book_dataset['summaries'].to_numpy()
embeddings = model.encode(summary_array)
embeddings_norm = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
n_clusters=len(book_dataset['categories'].unique())
kmeans = KMeans(n_clusters=n_clusters, random_state=42).fit(embeddings_norm)
labels = kmeans.labels_

for i in range(n_clusters):
    print(f"\nCluster {i} sample titles:")
    cluster_books = book_dataset[labels == i]['book_name'].head(5).tolist()
    for title in cluster_books:
        print(" -", title)

user_input = "History of dictators and leaders"
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