import pymongo
from pymongo import MongoClient
import sys

# ============================================================
# CONFIGURATION
# ============================================================
LOCAL_URI  = "mongodb://localhost:27018/"
LOCAL_DB   = "telecom_algerie"
LOCAL_COLL = "commentaires_normalises_tfidf"           # ← source : ta collection locale

ATLAS_URI  = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
ATLAS_DB   = "telecom_algerie_new"
ATLAS_COLL = "commentaires_normalises_tfidf"           # ← destination : Atlas

BATCH_SIZE = 5000

# ============================================================
# CONNEXIONS
# ============================================================
print("🔌 Connexion à MongoDB local...")
try:
    local_client = MongoClient(LOCAL_URI, serverSelectionTimeoutMS=5000)
    local_client.admin.command('ping')
    print("✅ Connecté à MongoDB local")
except Exception as e:
    print(f"❌ Erreur de connexion au local : {e}")
    sys.exit(1)

print("🔌 Connexion à MongoDB Atlas...")
try:
    atlas_client = MongoClient(ATLAS_URI, serverSelectionTimeoutMS=10000)
    atlas_client.admin.command('ping')
    print("✅ Connecté à Atlas")
except Exception as e:
    print(f"❌ Erreur de connexion à Atlas : {e}")
    sys.exit(1)

source_col = local_client[LOCAL_DB][LOCAL_COLL]
dest_col   = atlas_client[ATLAS_DB][ATLAS_COLL]

# ============================================================
# TRANSFERT
# ============================================================
print(f"\n📥 Lecture de {LOCAL_DB}.{LOCAL_COLL}...")
total_docs = source_col.count_documents({})
print(f"   {total_docs} documents à transférer.")

# Vérifier si la collection Atlas existe déjà
if ATLAS_COLL in atlas_client[ATLAS_DB].list_collection_names():
    count_atlas = dest_col.count_documents({})
    print(f"\n⚠️  La collection Atlas '{ATLAS_COLL}' existe déjà ({count_atlas} docs).")
    rep = input("Voulez-vous la remplacer ? (oui/non) : ")
    if rep.lower() != 'oui':
        print("❌ Opération annulée.")
        sys.exit(0)
    dest_col.drop()
    print("   Ancienne collection Atlas supprimée.")

print("\n🔄 Transfert local → Atlas en cours...")
inserted = 0

cursor = source_col.find({})
cursor.batch_size(1000)

batch = []
for doc in cursor:
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
print(f"\n📊 Résultat : {final_count} documents dans Atlas {ATLAS_DB}.{ATLAS_COLL}")

if final_count == total_docs:
    print("🎉 Transfert complet et vérifié !")
else:
    print(f"⚠️  Écart : {total_docs - final_count} documents manquants !")

local_client.close()
atlas_client.close()
print("\n🔒 Connexions fermées.")