FROM python:3.11-slim

WORKDIR /app

# System deps needed by faiss / sentence-transformers wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install only what the app needs to run (lean image, faster build)
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

# App code + pre-built search artifacts (faiss index, docs) + templates
COPY src/ ./src/
COPY templates/ ./templates/

EXPOSE 8000

# sentence-transformers caches the embedding model on first run;
# bake it into the image at build time so container startup is fast
# and doesn't need network access.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"

CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000"]
