import os
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import pickle

BASE_DIR = os.path.dirname(__file__)
INDEX_FILE = os.path.join(BASE_DIR, "faiss_index.bin")
DOCS_FILE = os.path.join(BASE_DIR, "docs.pkl")

app = FastAPI()

# Templates folder
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Load FAISS index
index = faiss.read_index(INDEX_FILE)

# Load docs
with open(DOCS_FILE, "rb") as f:
    docs = pickle.load(f)

titles = [d["title"] for d in docs]

# Load embedding model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # one level up from src
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "results": []})


@app.post("/", response_class=HTMLResponse)
def recommend(request: Request, plot: str = Form(...), k: int = Form(5)):
    # Compute embedding for input plot
    query_embedding = model.encode(plot, convert_to_numpy=True).astype("float32")
    query_embedding /= np.linalg.norm(query_embedding)

    # Search FAISS
    D, I = index.search(query_embedding.reshape(1, -1), k)

    # Get titles only
    result_titles = [titles[i] for i in I[0]]

    return templates.TemplateResponse("index.html", {"request": request, "results": result_titles})
