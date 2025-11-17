import pandas as pd

def clean_plot(row):
    import nltk
    from nltk.corpus import stopwords
    import string

    try:
        stop_words = set(stopwords.words("english"))
    except LookupError:
        nltk.download("stopwords")
        stop_words = set(stopwords.words("english"))

    plot = str(row['plot_description'] or "")
    title = str(row['title'] or "")

    plot = plot.replace(title, "")
    plot = plot.replace("\n", " ").replace("\r", "")
    plot = plot.replace("(This is an automatically generated summary.)", "")
    plot = plot.replace("Show Less", "")

    plot = plot.translate(str.maketrans("", "", string.punctuation)).lower()
    plot = " ".join([w for w in plot.split() if w not in stop_words])
    return " ".join(plot.split())


def transform_books(df):
    import numpy as np
    from sentence_transformers import SentenceTransformer

    # merge cols
    df["plot_description"] = df["plot"].combine_first(df["description"])
    df["plot_description"] = df.apply(clean_plot, axis=1)

    df = df.dropna(subset=["plot_description"])
    df = df[df["plot_description"].str.strip() != ""]

    df = df.drop(columns=["plot", "description"])

    # embeddings
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    arr = df["plot_description"].to_numpy()

    emb = model.encode(arr, convert_to_numpy=True, show_progress_bar=True)
    df["embedding"] = (emb / np.linalg.norm(emb, axis=1, keepdims=True)).tolist()

    return df