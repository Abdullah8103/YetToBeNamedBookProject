from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import math

def get_mongo_client(uri: str):
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    return client


def load_books_to_mongodb(df, client, db_name="books_db", collection_name="all_books"):
    db = client[db_name]

    # --- Reset collection each run ---
    if collection_name in db.list_collection_names():
        db[collection_name].drop()
        print(f"Dropped existing collection '{collection_name}'")

    collection = db[collection_name]

    # Unique index on title
    collection.create_index("title", unique=True)

    # --- Dedupe before inserting ---
    df = df.drop_duplicates(subset=["title"], keep="first")
    print(f"After dedupe, loading {len(df)} books")

    # --- Insert one by one to avoid BulkWriteError ---
    inserted = 0
    for record in df.to_dict("records"):
        try:
            collection.insert_one(record)
            inserted += 1
        except Exception:
            # Will never happen after collection reset,
            # but safe to keep this
            print(f"Skipping duplicate: {record['title']}")

    print(f"Inserted {inserted} books into MongoDB")