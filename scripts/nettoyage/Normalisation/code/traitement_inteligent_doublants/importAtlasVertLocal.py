from pymongo import MongoClient
import time

# ============================================================
# CONNEXION À MONGODB ATLAS (source)
# ============================================================

ATLAS_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

print("🔌 Connexion à MongoDB Atlas...")
client_atlas = MongoClient(ATLAS_URI)
db_atlas = client_atlas["telecom_algerie_new"]
collection_atlas = db_atlas["dataset_unifie_sans_doublons"]

# ============================================================
# CONNEXION À MONGODB LOCAL (destination)
# ============================================================

# MongoDB local (par défaut : localhost, port 27017)
LOCAL_URI = "mongodb://localhost:27018/"

print("🔌 Connexion à MongoDB local...")
client_local = MongoClient(LOCAL_URI)
db_local = client_local["telecom_algerie"]
collection_local = db_local["dataset_unifie_sans_doublons"]

# ============================================================
# VÉRIFICATION DES CONNEXIONS
# ============================================================

try:
    client_atlas.admin.command('ping')
    print("✅ Connexion Atlas réussie")
except:
    print("❌ Erreur de connexion Atlas")
    exit()

try:
    client_local.admin.command('ping')
    print("✅ Connexion MongoDB local réussie")
except:
    print("❌ Erreur de connexion MongoDB local")
    print("   Assurez-vous que MongoDB est démarré : sudo systemctl start mongod")
    exit()

# ============================================================
# COMPTER LES DOCUMENTS
# ============================================================

total_docs = collection_atlas.count_documents({})
print(f"\n📊 {total_docs} documents à importer")

# ============================================================
# SUPPRIMER L'ANCIENNE COLLECTION LOCALE (si elle existe)
# ============================================================

if "dataset_unifie_sans_doublons" in db_local.list_collection_names():
    print("\n⚠️ La collection existe déjà en local")
    reponse = input("   Voulez-vous la remplacer ? (oui/non) : ")
    if reponse.lower() == 'oui':
        collection_local.drop()
        print("   ✅ Ancienne collection supprimée")
    else:
        print("   ❌ Opération annulée")
        client_atlas.close()
        client_local.close()
        exit()

# ============================================================
# IMPORTATION PAR LOTS (BATCH)
# ============================================================

print("\n📥 Importation en cours...")

batch_size = 5000
count = 0
batch = []

# Utiliser un curseur avec batch_size
cursor = collection_atlas.find({}).batch_size(batch_size)

for doc in cursor:
    # Supprimer l'ancien _id pour en créer un nouveau en local
    if '_id' in doc:
        del doc['_id']
    
    batch.append(doc)
    count += 1
    
    if len(batch) >= batch_size:
        collection_local.insert_many(batch)
        print(f"   {count} documents importés...")
        batch = []

# Importer le dernier lot
if batch:
    collection_local.insert_many(batch)
    print(f"   {count} documents importés...")

print(f"\n✅ Importation terminée : {count} documents")

# ============================================================
# CRÉER DES INDEX
# ============================================================

print("\n📝 Création des index...")

# Créer un index sur Group_ID
collection_local.create_index("Group_ID")
print("   ✅ Index sur Group_ID créé")

# Créer un index sur hash_texte (si présent)
if collection_local.count_documents({"hash_texte": {"$exists": True}}) > 0:
    collection_local.create_index("hash_texte")
    print("   ✅ Index sur hash_texte créé")

# Créer un index sur label_final
collection_local.create_index("label_final")
print("   ✅ Index sur label_final créé")

# ============================================================
# VÉRIFICATION FINALE
# ============================================================

print("\n📊 VÉRIFICATION :")
print(f"   Documents dans MongoDB Atlas : {collection_atlas.count_documents({})}")
print(f"   Documents dans MongoDB local : {collection_local.count_documents({})}")

if collection_atlas.count_documents({}) == collection_local.count_documents({}):
    print("\n✅ IMPORTATION RÉUSSIE !")
else:
    print("\n⚠️ Vérifiez les compteurs")

# ============================================================
# AFFICHER UN APERÇU
# ============================================================

print("\n📝 Aperçu des 3 premiers documents :")
cursor = collection_local.find().limit(3)
for i, doc in enumerate(cursor):
    print(f"\n   Document {i+1} :")
    print(f"      Group_ID : {doc.get('Group_ID', '?')}")
    print(f"      label_final : {doc.get('label_final', '?')}")
    print(f"      nb_occurrences : {doc.get('nb_occurrences', '?')}")

# ============================================================
# FERMETURE DES CONNEXIONS
# ============================================================

client_atlas.close()
client_local.close()
print("\n🔒 Connexions fermées")