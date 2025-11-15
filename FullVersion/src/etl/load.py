from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

def get_mongo_client(uri: str):
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    return client

def load_books_to_mongodb(df, db_name="books_db", collection_name="books"):
    client = get_mongo_client("<YOUR MONGO URI>")
    collection = client[db_name][collection_name]
    collection.insert_many(df.to_dict("records"))
    print(f"Inserted {len(df)} books into MongoDB")