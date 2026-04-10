# from pymongo import MongoClient

# client = MongoClient("mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
# db = client["telecom_algerie_new"]

# # Copier la collection
# original = db["dataset_unifie_sans_doublons"]
# backup = db["dataset_unifie_sans_doublons_copie"]

# documents = list(original.find({}))
# backup.insert_many(documents)

# print(f"✅ Backup créé : {backup.count_documents({})} documents")

# client.close()

from pymongo import MongoClient

# Connexion à MongoDB local
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
SOURCE_COLL = "dataset_unifie_sans_doublons"
BACKUP_COLL = "dataset_unifie_sans_doublons_backup"

print("🔌 Connexion à MongoDB local...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Vérifier que la collection source existe
if SOURCE_COLL not in db.list_collection_names():
    print(f"❌ La collection '{SOURCE_COLL}' n'existe pas.")
    print("Collections disponibles :", db.list_collection_names())
    exit(1)

source = db[SOURCE_COLL]
backup = db[BACKUP_COLL]

# Supprimer l'ancien backup s'il existe
if BACKUP_COLL in db.list_collection_names():
    print(f"⚠️ Le backup '{BACKUP_COLL}' existe déjà. Il va être remplacé.")
    backup.drop()

# Copier tous les documents
documents = list(source.find({}))
if documents:
    backup.insert_many(documents)
    print(f"✅ Backup créé : {backup.count_documents({})} documents dans '{BACKUP_COLL}'")
else:
    print("❌ Aucun document trouvé dans la source.")

client.close()
print("🔒 Connexion fermée.")