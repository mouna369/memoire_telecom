from pymongo import MongoClient

client = MongoClient("mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["telecom_algerie_new"]

# Copier la collection
original = db["dataset_unifie"]
backup = db["dataset_unifie_copie"]

documents = list(original.find({}))
backup.insert_many(documents)

print(f"✅ Backup créé : {backup.count_documents({})} documents")

client.close()