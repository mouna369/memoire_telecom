from pymongo import MongoClient
import hashlib
from pymongo.operations import UpdateOne

MONGO_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(MONGO_URI)
db = client["telecom_algerie_new"]

print("="*60)
print("AJOUT DU HASH (VERSION BULK WRITE)")
print("="*60)

# ============================================================
# Traitement de l'ancienne collection
# ============================================================

print("\n📝 Traitement de l'ancienne collection...")
ancienne = db["dataset_unifie_copie"]

total = ancienne.count_documents({})
print(f"   {total} documents trouvés")

# Traitement par lots de 5000 documents
batch_size = 5000
count = 0
operations = []

cursor = ancienne.find({}).batch_size(batch_size)

for doc in cursor:
    texte = doc.get('normalized_arabert', '')
    if texte:
        hash_texte = hashlib.md5(texte.encode()).hexdigest()
        operations.append(
            UpdateOne({"_id": doc["_id"]}, {"$set": {"hash_texte": hash_texte}})
        )
        count += 1
        
        # Exécuter le lot quand il atteint batch_size
        if len(operations) >= batch_size:
            ancienne.bulk_write(operations)
            print(f"   {count} documents traités...")
            operations = []

# Traiter le dernier lot
if operations:
    ancienne.bulk_write(operations)
    print(f"   {count} documents traités...")

print(f"   ✅ {count} documents mis à jour")

# Créer un index
ancienne.create_index("hash_texte", background=True)
print("   ✅ Index créé sur hash_texte")

# ============================================================
# Traitement de la nouvelle collection
# ============================================================

if "dataset_unifie_sans_doublons" in db.list_collection_names():
    print("\n📝 Traitement de la nouvelle collection...")
    nouvelle = db["dataset_unifie_sans_doublons"]
    
    total = nouvelle.count_documents({})
    print(f"   {total} documents trouvés")
    
    count = 0
    operations = []
    cursor = nouvelle.find({}).batch_size(batch_size)
    
    for doc in cursor:
        texte = doc.get('normalized_arabert', '')
        if texte:
            hash_texte = hashlib.md5(texte.encode()).hexdigest()
            operations.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": {"hash_texte": hash_texte}})
            )
            count += 1
            
            if len(operations) >= batch_size:
                nouvelle.bulk_write(operations)
                print(f"   {count} documents traités...")
                operations = []
    
    if operations:
        nouvelle.bulk_write(operations)
        print(f"   {count} documents traités...")
    
    print(f"   ✅ {count} documents mis à jour")
    
    nouvelle.create_index("hash_texte", background=True)
    print("   ✅ Index créé sur hash_texte")
else:
    print("\n⚠️ La collection 'dataset_unifie_sans_doublons' n'existe pas encore")
    print("   Veuillez d'abord créer la nouvelle collection avec Regroupement_doublont.py")

client.close()
print("\n🔒 Connexion fermée")