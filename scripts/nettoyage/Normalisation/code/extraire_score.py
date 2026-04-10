from pymongo import MongoClient

# Connexion MongoDB local
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
SOURCE_COLL = "dataset_unifie_sans_doublons"
TARGET_COLL = "dataset_unifie"

print("🔌 Connexion à MongoDB local...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

if SOURCE_COLL not in db.list_collection_names():
    print(f"❌ La collection source '{SOURCE_COLL}' n'existe pas.")
    exit(1)
if TARGET_COLL not in db.list_collection_names():
    print(f"❌ La collection cible '{TARGET_COLL}' n'existe pas.")
    exit(1)

source = db[SOURCE_COLL]
target = db[TARGET_COLL]

# Index sur normalized_arabert pour accélérer les recherches (optionnel mais recommandé)
target.create_index("Commentaire_Client")

print("📥 Récupération des documents source...")
source_docs = source.find({}, {"Commentaire_Client": 1, "score": 1, "confidence": 1, "reason": 1, "annoté": 1, "label_final": 1})

updated = 0
not_found = 0

for src in source_docs:
    norm_text = src.get("Commentaire_Client")
    if not norm_text:
        continue
    
    # Préparer les champs à copier (exclure ceux qui sont None ou vides si nécessaire, mais on les copie tels quels)
    update_fields = {}
    for field in ["score", "confidence", "reason", "annoté", "label_final"]:
        if field in src:
            update_fields[field] = src[field]
    
    if not update_fields:
        continue
    
    # Chercher le document cible correspondant
    target_doc = target.find_one({"Commentaire_Client": norm_text})
    if target_doc:
        result = target.update_one(
            {"_id": target_doc["_id"]},
            {"$set": update_fields}
        )
        if result.modified_count:
            updated += 1
            if updated % 500 == 0:
                print(f"   {updated} documents mis à jour...")
    else:
        not_found += 1

print(f"\n✅ Mise à jour terminée.")
print(f"   Documents mis à jour : {updated}")
print(f"   Documents source sans correspondance : {not_found}")

client.close()
print("🔒 Connexion fermée.")