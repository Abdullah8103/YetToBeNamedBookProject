import faiss
import numpy as np
from pymongo import MongoClient
import pickle
import ast  # for converting strings back to lists

MONGO_URI = "mongodb+srv://abbassimuhammadabdullah_db_user:Easy8103@yettobenamedbookproject.bvjbjom.mongodb.net/?appName=YetToBeNamedBookProject"
INDEX_FILE = "faiss_index.bin"
DOCS_FILE = "docs.pkl"

client = MongoClient(MONGO_URI)
collection = client.books_db.all_books

# Load embeddings and metadata
docs = list(collection.find({}, {"title": 1, "plot_description": 1, "embedding": 1}))
titles = [d["title"] for d in docs]
plots = [d["plot_description"] for d in docs]

# Convert string embeddings to numpy arrays
embeddings = np.array([ast.literal_eval(d["embedding"]) for d in docs]).astype("float32")

# Build FAISS index
d = embeddings.shape[1]
index = faiss.IndexFlatIP(d)
index.add(embeddings)

# Save index and docs
faiss.write_index(index, INDEX_FILE)
with open(DOCS_FILE, "wb") as f:
    pickle.dump(docs, f)

print(f"FAISS index saved to {INDEX_FILE}")
print(f"Docs metadata saved to {DOCS_FILE}")