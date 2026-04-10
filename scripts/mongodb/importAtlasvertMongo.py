import pymongo
from pymongo import MongoClient
import sys

# ============================================================
# CONFIGURATION
# ============================================================
ATLAS_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
ATLAS_DB = "telecom_algerie_new"
ATLAS_COLL = "dataset_unifie_sans_doublons"

LOCAL_URI = "mongodb://localhost:27018/"
LOCAL_DB = "telecom_algerie"
LOCAL_COLL = "dataset_unifie_sans_doublons"

BATCH_SIZE = 5000

# ============================================================
# CONNEXIONS
# ============================================================
print("🔌 Connexion à MongoDB Atlas...")
try:
    atlas_client = MongoClient(ATLAS_URI, serverSelectionTimeoutMS=10000)
    atlas_client.admin.command('ping')
    print("✅ Connecté à Atlas")
except Exception as e:
    print(f"❌ Erreur de connexion à Atlas : {e}")
    sys.exit(1)

print("🔌 Connexion à MongoDB local...")
try:
    local_client = MongoClient(LOCAL_URI, serverSelectionTimeoutMS=5000)
    local_client.admin.command('ping')
    print("✅ Connecté à MongoDB local")
except Exception as e:
    print(f"❌ Erreur de connexion au local : {e}")
    sys.exit(1)

source_db = atlas_client[ATLAS_DB]
source_col = source_db[ATLAS_COLL]

dest_db = local_client[LOCAL_DB]
dest_col = dest_db[LOCAL_COLL]

# ============================================================
# TRANSFERT
# ============================================================
print(f"\n📥 Lecture de {ATLAS_DB}.{ATLAS_COLL}...")
total_docs = source_col.count_documents({})
print(f"   {total_docs} documents à transférer.")

if LOCAL_COLL in dest_db.list_collection_names():
    print(f"\n⚠️  La collection locale '{LOCAL_COLL}' existe déjà.")
    rep = input("Voulez-vous la remplacer ? (oui/non) : ")
    if rep.lower() != 'oui':
        print("❌ Opération annulée.")
        sys.exit(0)
    dest_col.drop()
    print("   Ancienne collection supprimée.")

print("\n🔄 Transfert en cours...")
inserted = 0

# Curseur normal (pas de no_cursor_timeout)
cursor = source_col.find({})
cursor.batch_size(1000)  # optimise les allers-retours

batch = []
for doc in cursor:
    # Optionnel : convertir ObjectId en string (décommente si souhaité)
    # doc['_id'] = str(doc['_id'])
    batch.append(doc)
    if len(batch) >= BATCH_SIZE:
        dest_col.insert_many(batch)
        inserted += len(batch)
        print(f"   {inserted}/{total_docs} documents transférés...")
        batch = []

if batch:
    dest_col.insert_many(batch)
    inserted += len(batch)
    print(f"   {inserted}/{total_docs} documents transférés...")

cursor.close()
print("\n✅ Transfert terminé.")

# ============================================================
# VÉRIFICATION
# ============================================================
final_count = dest_col.count_documents({})
print(f"\n📊 Résultat : {final_count} documents dans {LOCAL_DB}.{LOCAL_COLL}")

atlas_client.close()
local_client.close()
print("\n🔒 Connexions fermées.")