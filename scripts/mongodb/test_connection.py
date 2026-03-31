# from pymongo import MongoClient

# uri = "mongodb+srv://cococarte60_db_user:V1pbzGSB3bgPCR8T@cluster0.gejzu4a.mongodb.net/?appName=Cluster0"

# client = MongoClient(uri)

# db = client["telecom_algerie"]
# collection = db["comments_client"]

# collection.insert_one({"test": "ok"})

# print("Connexion OK")

from pymongo import MongoClient
import json

uri = "mongodb+srv://cococarte60_db_user:V1pbzGSB3bgPCR8T@cluster0.gejzu4a.mongodb.net/?appName=Cluster0"
client = MongoClient(uri)

db = client["telecom_algerie"]
collection = db["commentaires_normalises"]

# charger JSON
with open("/home/mouna/projet_telecom/donnees/transformees/telecom_algerie.commentaires_normalises.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# insert
collection.insert_many(data)

print("Import terminé")