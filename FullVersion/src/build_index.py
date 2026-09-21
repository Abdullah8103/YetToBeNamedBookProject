import os
import faiss
import numpy as np
from pymongo import MongoClient
import pickle
import ast  # for converting strings back to lists

MONGO_URI = os.environ["MONGO_URI"]
INDEX_FILE = "faiss_index.bin"
DOCS_FILE = "docs.pkl"

client = MongoClient(MONGO_URI)
collection = client.books_db.all_books

# Load embeddings and metadata
docs = list(collection.find({}, {"title": 1, "plot_description": 1, "embedding": 1}))

# Convert string embeddings to numpy arrays
embeddings = np.array([ast.literal_eval(d["embedding"]) for d in docs]).astype("float32")

# Build FAISS index
d = embeddings.shape[1]
index = faiss.IndexFlatIP(d)
index.add(embeddings)

# Save index. For docs.pkl, keep only what the serving app (app.py) actually
# uses -- title and plot_description. The embeddings already live in the
# FAISS index above, and dropping Mongo's ObjectId here means the serving
# app doesn't need pymongo/bson installed just to unpickle this file.
faiss.write_index(index, INDEX_FILE)
clean_docs = [{"title": d["title"], "plot_description": d["plot_description"]} for d in docs]
with open(DOCS_FILE, "wb") as f:
    pickle.dump(clean_docs, f)

print(f"FAISS index saved to {INDEX_FILE}")
print(f"Docs metadata saved to {DOCS_FILE}")