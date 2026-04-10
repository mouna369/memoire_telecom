from pymongo import MongoClient

# Configuration MongoDB local
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
OLD_NAME = "dataset_unifie_sans_doublons"
NEW_NAME = "dataset_unifie"

print("🔌 Connexion à MongoDB local...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Vérifier que l'ancienne collection existe
if OLD_NAME not in db.list_collection_names():
    print(f"❌ La collection '{OLD_NAME}' n'existe pas.")
    print("Collections disponibles :", db.list_collection_names())
    exit(1)

# Vérifier si la nouvelle collection existe déjà
if NEW_NAME in db.list_collection_names():
    print(f"⚠️ La collection '{NEW_NAME}' existe déjà.")
    rep = input("Voulez-vous la supprimer avant de renommer ? (oui/non) : ")
    if rep.lower() == 'oui':
        db[NEW_NAME].drop()
        print(f"   Ancienne collection '{NEW_NAME}' supprimée.")
    else:
        print("❌ Opération annulée.")
        exit(0)

# Renommer la collection
print(f"\n🔄 Renommage de '{OLD_NAME}' en '{NEW_NAME}'...")
db[OLD_NAME].rename(NEW_NAME)
print(f"✅ Collection renommée avec succès.")

# Vérification
collections = db.list_collection_names()
print(f"\n📁 Collections dans '{DB_NAME}' :")
for name in collections:
    count = db[name].count_documents({})
    print(f"   - {name} : {count} documents")

client.close()
print("\n🔒 Connexion fermée.")