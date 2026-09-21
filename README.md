# Book Similarity Search

An ETL pipeline and search app that recommends books by plot similarity
rather than keyword matching. You describe a plot in your own words, it
returns books with the closest semantic meaning, not the closest string.

## Why semantic search here

Plot descriptions rarely share vocabulary even when they share themes.
"A man is exiled and plots revenge" and "a wrongfully imprisoned sailor
schemes his return" describe the same story arc with almost no word
overlap. TF-IDF or keyword search misses that; sentence embeddings
don't, because they encode meaning rather than token frequency. That's
the whole justification for the extra pipeline complexity below over
just grepping descriptions.

## Pipeline

```
Gutenberg (scrape)  ─┐
                      ├─> combine ─> clean + embed ─> MongoDB ─> FAISS index ─> FastAPI search
NYT Bestsellers API ─┘
```

Orchestrated as an Airflow DAG (`dags/etl_books_dag.py`):

1. **Extract** - two independent sources, kept separate on purpose so
   a failure in one (e.g. NYT API rate limit) doesn't block the other.
   Gutenberg is scraped for top-ranked titles and their summaries; NYT
   bestseller data comes from their public API, pulled weekly across a
   fixed date range.
2. **Combine** - the two sources have different schemas (`plot` vs
   `description`), unified into a single `plot_description` column
   before anything downstream has to care which source a row came from.
3. **Transform** - strips titles out of their own plot text (otherwise
   the model partly matches on the title string, not the plot),
   removes stopwords and punctuation, and encodes each cleaned
   description with `all-MiniLM-L6-v2` (384-dim, runs fine on CPU,
   good enough quality-to-latency tradeoff for a corpus this size,
   didn't need a bigger model to get sensible results).
4. **Load** - writes to MongoDB as the system of record.
5. **Index** - `build_index.py` pulls from Mongo, L2-normalizes the
   embeddings, and builds a FAISS `IndexFlatIP`. Inner product on
   normalized vectors is mathematically equivalent to cosine
   similarity, and is what FAISS is actually fast at, so normalizing
   once at index-build time avoids computing cosine distance per query
   later.
6. **Serve** - FastAPI loads the prebuilt index and a flat list of
   `{title, plot_description}` (no DB dependency at request time), embeds
   the incoming query the same way, and returns the nearest neighbours.

Currently indexing ~2,400 books across both sources.

## Known limitations

- The Mongo load is a full drop-and-reload on every run, not an
  upsert. Fine at this scale, wouldn't be at real bestseller-list
  volume or with a live scraping cadence.
- No automated tests yet. The pipeline stages are pure enough
  (extract/transform/load are separable functions) that unit tests
  would be straightforward to add; just haven't yet.
- Search quality hasn't been evaluated against a labeled set, it's
  eyeballed. A proper eval would need something like a small set of
  known-similar book pairs and a ranking metric (e.g. recall@k).

## Running it

The search app ships with a prebuilt FAISS index and doc store, so it
runs standalone with no database or API keys needed:

```bash
cd FullVersion
docker compose up --build
```

Open http://localhost:8000, enter a plot description, get results back.

To rebuild the index from a fresh Mongo load, set `MONGO_URI` in your
environment and run `src/build_index.py` directly.

## Stack

Python, pandas, Airflow, MongoDB, sentence-transformers, FAISS, FastAPI, Docker.
