# from pymongo import MongoClient

# # Connexion MongoDB local
# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# SOURCE_COLL = "dataset_unifie_sans_doublons"
# TARGET_COLL = "dataset_unifie"

# print("🔌 Connexion à MongoDB local...")
# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]

# if SOURCE_COLL not in db.list_collection_names():
#     print(f"❌ La collection source '{SOURCE_COLL}' n'existe pas.")
#     exit(1)
# if TARGET_COLL not in db.list_collection_names():
#     print(f"❌ La collection cible '{TARGET_COLL}' n'existe pas.")
#     exit(1)

# source = db[SOURCE_COLL]
# target = db[TARGET_COLL]

# # Index sur normalized_arabert pour accélérer les recherches (optionnel mais recommandé)
# target.create_index("Commentaire_Client")

# print("📥 Récupération des documents source...")
# source_docs = source.find({}, {"Commentaire_Client": 1, "score": 1, "confidence": 1, "reason": 1, "annoté": 1, "label_final": 1})

# updated = 0
# not_found = 0

# for src in source_docs:
#     norm_text = src.get("Commentaire_Client")
#     if not norm_text:
#         continue
    
#     # Préparer les champs à copier (exclure ceux qui sont None ou vides si nécessaire, mais on les copie tels quels)
#     update_fields = {}
#     for field in ["score", "confidence", "reason", "annoté", "label_final"]:
#         if field in src:
#             update_fields[field] = src[field]
    
#     if not update_fields:
#         continue
    
#     # Chercher le document cible correspondant
#     target_doc = target.find_one({"Commentaire_Client": norm_text})
#     if target_doc:
#         result = target.update_one(
#             {"_id": target_doc["_id"]},
#             {"$set": update_fields}
#         )
#         if result.modified_count:
#             updated += 1
#             if updated % 500 == 0:
#                 print(f"   {updated} documents mis à jour...")
#     else:
#         not_found += 1

# print(f"\n✅ Mise à jour terminée.")
# print(f"   Documents mis à jour : {updated}")
# print(f"   Documents source sans correspondance : {not_found}")

# client.close()
# print("🔒 Connexion fermée.")
from pymongo import MongoClient, UpdateOne
import csv

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI    = "mongodb://localhost:27018/"
DB_NAME      = "telecom_algerie"
SOURCE_COLL  = "dataset_unifie_sans_doublons"   # contient les scores
TARGET_COLL  = "commentaires_normalises_tfidf"                 # à mettre à jour
OUTPUT_FILE  = "sans_correspondance.csv"

FIELDS_TO_COPY = ["score", "confidence", "reason", "annoté",
                  "label_final", "conflit", "labels_originaux"]

# ============================================================
# CONNEXION
# ============================================================
print("🔌 Connexion à MongoDB local...")
client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

for coll in [SOURCE_COLL, TARGET_COLL]:
    if coll not in db.list_collection_names():
        print(f"❌ La collection '{coll}' n'existe pas.")
        client.close()
        exit(1)

source = db[SOURCE_COLL]
target = db[TARGET_COLL]

# Index pour accélérer la recherche
target.create_index("normalized_arabert")
print("✅ Index créé sur 'normalized_arabert'")

# ============================================================
# LECTURE SOURCE
# ============================================================
projection = {"normalized_arabert": 1, "_id": 1}
for f in FIELDS_TO_COPY:
    projection[f] = 1

print("\n📥 Récupération des documents source...")
source_docs = list(source.find({}, projection))
print(f"   {len(source_docs)} documents dans la source")

# ============================================================
# TRANSFERT
# ============================================================
updated      = 0
already_ok   = 0
not_found    = 0
skipped      = 0
not_found_list = []
bulk_ops     = []
BATCH_SIZE   = 500

for i, src in enumerate(source_docs):

    # Clé de correspondance : normalized_arabert
    cle = src.get("normalized_arabert")
    if not cle or str(cle).strip() == "":
        skipped += 1
        continue

    # Construire les champs à copier (seulement ceux présents et non null)
    update_fields = {}
    for field in FIELDS_TO_COPY:
        if field in src and src[field] is not None:
            update_fields[field] = src[field]

    if not update_fields:
        skipped += 1
        continue

    # Chercher le document cible
    target_doc = target.find_one(
        {"normalized_arabert": cle},
        {"_id": 1}
    )

    if target_doc:
        bulk_ops.append(
            UpdateOne(
                {"_id": target_doc["_id"]},
                {"$set": update_fields}
            )
        )
        updated += 1

        # Envoi par lots
        if len(bulk_ops) >= BATCH_SIZE:
            result = target.bulk_write(bulk_ops, ordered=False)
            already_ok += (len(bulk_ops) - result.modified_count)
            bulk_ops = []

        if updated % 500 == 0:
            print(f"   {updated} documents traités...")
    else:
        not_found += 1
        not_found_list.append({
            "_id_source":        str(src["_id"]),
            "normalized_arabert": cle,
            "score":             src.get("score"),
            "confidence":        src.get("confidence"),
            "reason":            src.get("reason"),
            "annoté":            src.get("annoté"),
            "label_final":       src.get("label_final"),
            "conflit":           src.get("conflit"),
            "labels_originaux":  src.get("labels_originaux"),
        })

# Dernier lot
if bulk_ops:
    result = target.bulk_write(bulk_ops, ordered=False)
    already_ok += (len(bulk_ops) - result.modified_count)

# ============================================================
# RÉSUMÉ
# ============================================================
print(f"\n✅ Mise à jour terminée.")
print(f"   Documents source total         : {len(source_docs)}")
print(f"   Documents mis à jour           : {updated - already_ok}")
print(f"   Documents déjà à jour          : {already_ok}")
print(f"   Documents skippés (pas de clé) : {skipped}")
print(f"   Sans correspondance            : {not_found}")
print(f"   Total traités                  : {updated + skipped + not_found}")

# ============================================================
# EXPORT CSV des sans correspondance
# ============================================================
if not_found_list:
    fieldnames = ["_id_source", "normalized_arabert", "score", "confidence",
                  "reason", "annoté", "label_final", "conflit", "labels_originaux"]
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(not_found_list)
    print(f"\n💾 Sans correspondance sauvegardés dans '{OUTPUT_FILE}'")
else:
    print("\n✅ Tous les documents source ont trouvé une correspondance.")

client.close()
print("🔒 Connexion fermée.")