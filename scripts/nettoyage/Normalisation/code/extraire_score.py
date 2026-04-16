# # from pymongo import MongoClient

# # # Connexion MongoDB local
# # MONGO_URI = "mongodb://localhost:27018/"
# # DB_NAME = "telecom_algerie"
# # SOURCE_COLL = "dataset_unifie_sans_doublons"
# # TARGET_COLL = "dataset_unifie"

# # print("🔌 Connexion à MongoDB local...")
# # client = MongoClient(MONGO_URI)
# # db = client[DB_NAME]

# # if SOURCE_COLL not in db.list_collection_names():
# #     print(f"❌ La collection source '{SOURCE_COLL}' n'existe pas.")
# #     exit(1)
# # if TARGET_COLL not in db.list_collection_names():
# #     print(f"❌ La collection cible '{TARGET_COLL}' n'existe pas.")
# #     exit(1)

# # source = db[SOURCE_COLL]
# # target = db[TARGET_COLL]

# # # Index sur normalized_arabert pour accélérer les recherches (optionnel mais recommandé)
# # target.create_index("Commentaire_Client")

# # print("📥 Récupération des documents source...")
# # source_docs = source.find({}, {"Commentaire_Client": 1, "score": 1, "confidence": 1, "reason": 1, "annoté": 1, "label_final": 1})

# # updated = 0
# # not_found = 0

# # for src in source_docs:
# #     norm_text = src.get("Commentaire_Client")
# #     if not norm_text:
# #         continue
    
# #     # Préparer les champs à copier (exclure ceux qui sont None ou vides si nécessaire, mais on les copie tels quels)
# #     update_fields = {}
# #     for field in ["score", "confidence", "reason", "annoté", "label_final"]:
# #         if field in src:
# #             update_fields[field] = src[field]
    
# #     if not update_fields:
# #         continue
    
# #     # Chercher le document cible correspondant
# #     target_doc = target.find_one({"Commentaire_Client": norm_text})
# #     if target_doc:
# #         result = target.update_one(
# #             {"_id": target_doc["_id"]},
# #             {"$set": update_fields}
# #         )
# #         if result.modified_count:
# #             updated += 1
# #             if updated % 500 == 0:
# #                 print(f"   {updated} documents mis à jour...")
# #     else:
# #         not_found += 1

# # print(f"\n✅ Mise à jour terminée.")
# # print(f"   Documents mis à jour : {updated}")
# # print(f"   Documents source sans correspondance : {not_found}")

# # client.close()
# # print("🔒 Connexion fermée.")
# from pymongo import MongoClient, UpdateOne
# import csv

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI    = "mongodb://localhost:27018/"
# DB_NAME      = "telecom_algerie"
# SOURCE_COLL  = "dataset_unifie_diribiha_label"   # contient les scores
# TARGET_COLL  = "commentaires_normalises"                 # à mettre à jour
# OUTPUT_FILE  = "sans_correspondance.csv"

# FIELDS_TO_COPY = ["score", "confidence", "reason", "annoté",
#                   "label_final", "conflit", "labels_originaux"]

# # ============================================================
# # CONNEXION
# # ============================================================
# print("🔌 Connexion à MongoDB local...")
# client = MongoClient(MONGO_URI)
# db     = client[DB_NAME]

# for coll in [SOURCE_COLL, TARGET_COLL]:
#     if coll not in db.list_collection_names():
#         print(f"❌ La collection '{coll}' n'existe pas.")
#         client.close()
#         exit(1)

# source = db[SOURCE_COLL]
# target = db[TARGET_COLL]

# # Index pour accélérer la recherche
# target.create_index("Commentaire_Client")
# print("✅ Index créé sur 'Commentaire_Client'")

# # ============================================================
# # LECTURE SOURCE
# # ============================================================
# projection = {"normalized_arabert": 1, "_id": 1}
# for f in FIELDS_TO_COPY:
#     projection[f] = 1

# print("\n📥 Récupération des documents source...")
# source_docs = list(source.find({}, projection))
# print(f"   {len(source_docs)} documents dans la source")

# # ============================================================
# # TRANSFERT
# # ============================================================
# updated      = 0
# already_ok   = 0
# not_found    = 0
# skipped      = 0
# not_found_list = []
# bulk_ops     = []
# BATCH_SIZE   = 500

# for i, src in enumerate(source_docs):

#     # Clé de correspondance : normalized_arabert
#     cle = src.get("Commentaire_Client")
#     if not cle or str(cle).strip() == "":
#         skipped += 1
#         continue

#     # Construire les champs à copier (seulement ceux présents et non null)
#     update_fields = {}
#     for field in FIELDS_TO_COPY:
#         if field in src and src[field] is not None:
#             update_fields[field] = src[field]

#     if not update_fields:
#         skipped += 1
#         continue

#     # Chercher le document cible
#     target_doc = target.find_one(
#         {"normalized_arabert": cle},
#         {"_id": 1}
#     )

#     if target_doc:
#         bulk_ops.append(
#             UpdateOne(
#                 {"_id": target_doc["_id"]},
#                 {"$set": update_fields}
#             )
#         )
#         updated += 1

#         # Envoi par lots
#         if len(bulk_ops) >= BATCH_SIZE:
#             result = target.bulk_write(bulk_ops, ordered=False)
#             already_ok += (len(bulk_ops) - result.modified_count)
#             bulk_ops = []

#         if updated % 500 == 0:
#             print(f"   {updated} documents traités...")
#     else:
#         not_found += 1
#         not_found_list.append({
#             "_id_source":        str(src["_id"]),
#             "normalized_arabert": cle,
#             "score":             src.get("score"),
#             "confidence":        src.get("confidence"),
#             "reason":            src.get("reason"),
#             "annoté":            src.get("annoté"),
#             "label_final":       src.get("label_final"),
#             "conflit":           src.get("conflit"),
#             "labels_originaux":  src.get("labels_originaux"),
#         })

# # Dernier lot
# if bulk_ops:
#     result = target.bulk_write(bulk_ops, ordered=False)
#     already_ok += (len(bulk_ops) - result.modified_count)

# # ============================================================
# # RÉSUMÉ
# # ============================================================
# print(f"\n✅ Mise à jour terminée.")
# print(f"   Documents source total         : {len(source_docs)}")
# print(f"   Documents mis à jour           : {updated - already_ok}")
# print(f"   Documents déjà à jour          : {already_ok}")
# print(f"   Documents skippés (pas de clé) : {skipped}")
# print(f"   Sans correspondance            : {not_found}")
# print(f"   Total traités                  : {updated + skipped + not_found}")

# # ============================================================
# # EXPORT CSV des sans correspondance
# # ============================================================
# if not_found_list:
#     fieldnames = ["_id_source", "normalized_arabert", "score", "confidence",
#                   "reason", "annoté", "label_final", "conflit", "labels_originaux"]
#     with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(not_found_list)
#     print(f"\n💾 Sans correspondance sauvegardés dans '{OUTPUT_FILE}'")
# else:
#     print("\n✅ Tous les documents source ont trouvé une correspondance.")

# client.close()
# print("🔒 Connexion fermée.")
from pymongo import MongoClient, UpdateOne
import csv

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI    = "mongodb://localhost:27018/"
DB_NAME      = "telecom_algerie"
SOURCE_COLL  = "dataset_unifie_diribiha_label"   # contient les scores
TARGET_COLL  = "commentaires_normalises"         # à mettre à jour
OUTPUT_FILE  = "sans_correspondance.csv"

# ⚠️ AJOUT DU CHAMP "label" DANS LA LISTE !
FIELDS_TO_COPY = ["score", "confidence", "reason", "annoté",
                  "label_final", "conflit", "labels_originaux", "label"]
#                                                                ↑
#                                                          NOUVEAU !

# ============================================================
# CONNEXION
# ============================================================
print("🔌 Connexion à MongoDB local...")
client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

# Vérifier que les collections existent
for coll in [SOURCE_COLL, TARGET_COLL]:
    if coll not in db.list_collection_names():
        print(f"❌ La collection '{coll}' n'existe pas.")
        client.close()
        exit(1)

source = db[SOURCE_COLL]
target = db[TARGET_COLL]

# Index pour accélérer la recherche sur Commentaire_Client_Original
target.create_index("Commentaire_Client_Original")
print("✅ Index créé sur 'Commentaire_Client_Original'")

# ============================================================
# LECTURE SOURCE
# ============================================================
# On récupère Commentaire_Client_Original et les champs à copier
projection = {"Commentaire_Client_Original": 1, "_id": 1}
for f in FIELDS_TO_COPY:
    projection[f] = 1

print("\n📥 Récupération des documents source...")
source_docs = list(source.find({}, projection))
print(f"   {len(source_docs)} documents dans la source")

# Afficher un exemple des champs disponibles dans source
if source_docs:
    print("\n📋 Exemple des champs disponibles dans source :")
    sample = source_docs[0]
    for field in FIELDS_TO_COPY:
        if field in sample:
            print(f"   {field}: {sample[field]}")

# Statistiques
updated       = 0
already_ok    = 0
not_found     = 0
skipped       = 0
not_found_list = []
bulk_ops      = []
BATCH_SIZE    = 500

# ============================================================
# TRANSFERT - Correspondance sur Commentaire_Client_Original
# ============================================================
print("\n🔄 Recherche des correspondances (par Commentaire_Client_Original)...")

for i, src in enumerate(source_docs):

    # Clé de correspondance : Commentaire_Client_Original (texte brut)
    cle = src.get("Commentaire_Client_Original")
    if not cle or str(cle).strip() == "":
        skipped += 1
        continue

    # Construire les champs à copier (seulement ceux présents et non null)
    update_fields = {}
    for field in FIELDS_TO_COPY:
        if field in src and src[field] is not None:
            # Conversion spéciale pour le score si c'est une string
            if field == "score" and isinstance(src[field], str):
                try:
                    update_fields[field] = float(src[field])
                except:
                    update_fields[field] = src[field]
            else:
                update_fields[field] = src[field]

    if not update_fields:
        skipped += 1
        continue

    # Chercher le document cible par Commentaire_Client_Original EXACT
    target_doc = target.find_one(
        {"Commentaire_Client_Original": cle},
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
            print(f"   {updated} documents mis à jour...")
    else:
        not_found += 1
        not_found_list.append({
            "_id_source":        str(src["_id"]),
            "Commentaire_Client_Original": cle[:100] + "..." if len(cle) > 100 else cle,
            "score":             src.get("score"),
            "confidence":        src.get("confidence"),
            "reason":            src.get("reason"),
            "annoté":            src.get("annoté"),
            "label":             src.get("label"),  # ← AJOUTÉ
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
print("\n" + "=" * 70)
print("📊 RÉSULTATS DE LA MISE À JOUR")
print("=" * 70)
print(f"   Documents source total           : {len(source_docs)}")
print(f"   ✅ Documents mis à jour          : {updated - already_ok}")
print(f"   ✅ Documents déjà à jour         : {already_ok}")
print(f"   ⏭️  Documents skippés (pas de clé) : {skipped}")
print(f"   ❌ Sans correspondance           : {not_found}")
print(f"   📊 Total traités                 : {updated + skipped + not_found}")

# ============================================================
# EXPORT CSV des sans correspondance
# ============================================================
if not_found_list:
    fieldnames = ["_id_source", "Commentaire_Client_Original", "score", "confidence",
                  "reason", "annoté", "label", "label_final", "conflit", "labels_originaux"]
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(not_found_list)
    
    print(f"\n💾 {len(not_found_list)} commentaires sans correspondance sauvegardés dans '{OUTPUT_FILE}'")
    
    # Afficher les 5 premiers sans correspondance
    print("\n📋 Exemples de commentaires sans correspondance (5 premiers) :")
    for i, item in enumerate(not_found_list[:5]):
        print(f"   {i+1}. {item['Commentaire_Client_Original'][:80]}...")
else:
    print("\n✅ TOUS les documents source ont trouvé une correspondance !")

# ============================================================
# VÉRIFICATION FINALE
# ============================================================
print("\n🔍 VÉRIFICATION FINALE")
print("=" * 70)

# Compter combien de documents cible ont maintenant un score
total_target = target.count_documents({})
with_score = target.count_documents({"score": {"$exists": True}})
without_score = target.count_documents({"score": {"$exists": False}})
with_label = target.count_documents({"label": {"$exists": True}})

print(f"   Collection cible ({TARGET_COLL}) :")
print(f"   Total documents      : {total_target}")
print(f"   Avec score           : {with_score}")
print(f"   Sans score           : {without_score}")
print(f"   Avec label           : {with_label}")

# Vérifier un document spécifique
print("\n🔍 Vérification d'un document spécifique :")
test_doc = target.find_one({"Commentaire_Client_Original": "المنتقلون من الادياسال الى الفيير هذه الخدمة لا تعمل ( معرفة رقم الثابت)"})
if test_doc:
    print(f"   Commentaire_Client_Original trouvé !")
    print(f"   score: {test_doc.get('score')}")
    print(f"   label: {test_doc.get('label')}")
    print(f"   confidence: {test_doc.get('confidence')}")
else:
    print("   ❌ Document non trouvé")

if without_score > 0:
    print(f"\n⚠️  ATTENTION : {without_score} commentaires n'ont toujours pas de score !")
    print("   → Vérifiez le fichier CSV pour les correspondances manquantes")
    
    # Afficher un exemple de document cible sans score
    example = target.find_one({"score": {"$exists": False}})
    if example:
        print("\n📋 Exemple de document cible sans score :")
        print(f"   _id: {example.get('_id')}")
        print(f"   Commentaire_Client_Original: {example.get('Commentaire_Client_Original', '')[:80]}")
        print(f"   Commentaire_Client: {example.get('Commentaire_Client', '')[:80]}")
else:
    print("\n✅ TOUS les commentaires ont un score !")

client.close()
print("\n🔒 Connexion fermée.")