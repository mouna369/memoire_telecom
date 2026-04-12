from pymongo import MongoClient, UpdateMany
import csv

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI       = "mongodb://localhost:27018/"
DB_NAME         = "telecom_algerie"
SOURCE_COLL     = "dataset_unifie_sans_doublons"
TARGET_COLL     = "dataset_unifie"

# Fichier CSV généré par le 1er script (les sans correspondance)
INPUT_FILE      = "sans_correspondance.csv"
OUTPUT_FILE     = "encore_sans_correspondance.csv"  # ceux qui restent introuvables

FIELDS_TO_COPY  = ["score", "confidence", "reason", "annoté",
                   "label_final", "conflit", "labels_originaux"]

# ============================================================
# CONNEXION
# ============================================================
print("🔌 Connexion à MongoDB local...")
client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

source = db[SOURCE_COLL]
target = db[TARGET_COLL]

target.create_index("Commentaire_Client")
print("✅ Index créé sur 'Commentaire_Client'")

# ============================================================
# LECTURE DU CSV (les sans correspondance du 1er script)
# ============================================================
print(f"\n📥 Lecture de '{INPUT_FILE}'...")
with open(INPUT_FILE, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    sans_correspondance = list(reader)

print(f"   {len(sans_correspondance)} documents à re-vérifier")

# ============================================================
# 2ÈME PASSE : correspondance via Commentaire_Client
# ============================================================
updated        = 0
not_found      = 0
skipped        = 0
not_found_list = []

for row in sans_correspondance:

    # Récupérer le Commentaire_Client depuis la SOURCE MongoDB
    # (le CSV ne contient que normalized_arabert, on va chercher dans source)
    id_source = row.get("_id_source", "").strip()
    if not id_source:
        skipped += 1
        continue

    # Récupérer le doc source complet pour avoir Commentaire_Client + les champs
    from bson import ObjectId
    try:
        src = source.find_one(
            {"_id": ObjectId(id_source)},
            {"Commentaire_Client": 1, "score": 1, "confidence": 1,
             "reason": 1, "annoté": 1, "label_final": 1,
             "conflit": 1, "labels_originaux": 1}
        )
    except Exception:
        skipped += 1
        continue

    if not src:
        skipped += 1
        continue

    cle = src.get("Commentaire_Client")
    if not cle or str(cle).strip() == "":
        skipped += 1
        continue

    # Construire les champs à copier
    update_fields = {}
    for field in FIELDS_TO_COPY:
        if field in src and src[field] is not None:
            update_fields[field] = src[field]

    if not update_fields:
        skipped += 1
        continue

    # Chercher dans target via Commentaire_Client
    target_docs = list(target.find(
        {"Commentaire_Client": cle},
        {"_id": 1}
    ))

    if target_docs:
        # Mettre à jour TOUS les docs qui matchent
        result = target.update_many(
            {"Commentaire_Client": cle},
            {"$set": update_fields}
        )
        updated += result.modified_count
        if updated % 500 == 0:
            print(f"   {updated} documents mis à jour...")
    else:
        not_found += 1
        not_found_list.append({
            "_id_source":         id_source,
            "normalized_arabert": row.get("normalized_arabert", ""),
            "Commentaire_Client": cle,
            "score":              src.get("score"),
            "confidence":         src.get("confidence"),
            "reason":             src.get("reason"),
            "annoté":             src.get("annoté"),
            "label_final":        src.get("label_final"),
            "conflit":            src.get("conflit"),
            "labels_originaux":   src.get("labels_originaux"),
        })

# ============================================================
# RÉSUMÉ
# ============================================================
print(f"\n✅ 2ème passe terminée.")
print(f"   Documents re-vérifiés          : {len(sans_correspondance)}")
print(f"   Documents mis à jour           : {updated}")
print(f"   Documents skippés              : {skipped}")
print(f"   Encore sans correspondance     : {not_found}")

# ============================================================
# EXPORT CSV des encore introuvables
# ============================================================
if not_found_list:
    fieldnames = ["_id_source", "normalized_arabert", "Commentaire_Client",
                  "score", "confidence", "reason", "annoté",
                  "label_final", "conflit", "labels_originaux"]
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(not_found_list)
    print(f"\n💾 Encore introuvables sauvegardés dans '{OUTPUT_FILE}'")
else:
    print("\n🎉 Tous les documents ont trouvé une correspondance !")

client.close()
print("🔒 Connexion fermée.")