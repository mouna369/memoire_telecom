from pymongo import MongoClient

MONGO_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(MONGO_URI)
db = client["telecom_algerie_new"]
collection = db["dataset_unifie_copie"]

# Convertir 'normalized_arabert' en minuscules (remplacement direct)
resultat = collection.update_many(
    {},
    [
        {
            "$set": {
                "normalized_arabert": {
                    "$toLower": "$normalized_arabert"
                }
            }
        }
    ]
)

print(f"✅ {resultat.modified_count} documents mis en minuscules")

client.close()