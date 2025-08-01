from pymongo import MongoClient

def get_mongo_collection():
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    db = client["bike_trips"]
    collection = db["trips"]
    return collection

def drop_collection(collection):
    collection.drop()

def insert_data(collection, data, label=""):
    if data:
        collection.insert_many(data)
        print(f"Insertion en base réussie: ({len(data)} documents) {label}")
    else:
        print("[MongoDB] Aucune donnée à insérer", label)