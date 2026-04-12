# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# Ajoute le champ 'Commentaire_Client_Original' dans commentaires_sans_urls_arobase2
# en le copiant depuis commentaires_bruts2.Commentaire_Client, en utilisant _id comme clé.
# """

# from pymongo import MongoClient, UpdateOne

# # Connexion MongoDB local
# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# COLL_SOURCE = "commentaires_bruts"               # contient Commentaire_Client
# COLL_TARGET = "commentaires_normalises"   # collection à enrichir

# print("🔌 Connexion à MongoDB local...")
# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]

# # Vérifier que les deux collections existent
# for col in [COLL_SOURCE, COLL_TARGET]:
#     if col not in db.list_collection_names():
#         print(f"❌ La collection '{col}' n'existe pas.")
#         exit(1)

# print("✅ Collections trouvées.")

# # 1. Charger tous les documents de la source (commentaires_bruts2) dans un dictionnaire
# print("\n📥 Chargement des commentaires bruts...")
# bruts_cursor = db[COLL_SOURCE].find({}, {"_id": 1, "Commentaire_Client": 1})
# bruts_dict = {}
# for doc in bruts_cursor:
#     bruts_dict[doc["_id"]] = doc.get("Commentaire_Client", "")

# print(f"   {len(bruts_dict)} documents chargés depuis {COLL_SOURCE}.")

# # 2. Mettre à jour la collection cible
# print(f"\n🔄 Mise à jour de '{COLL_TARGET}' avec Commentaire_Client_Original...")
# target_coll = db[COLL_TARGET]
# total = target_coll.count_documents({})
# updated = 0
# not_found = 0
# batch_size = 500
# bulk_ops = []

# for doc in target_coll.find({}, {"_id": 1}):
#     doc_id = doc["_id"]
#     original_text = bruts_dict.get(doc_id)
#     if original_text is not None:
#         bulk_ops.append(
#             UpdateOne({"_id": doc_id}, {"$set": {"Commentaire_Client_Original": original_text}})
#         )
#         updated += 1
#     else:
#         not_found += 1

#     if len(bulk_ops) >= batch_size:
#         if bulk_ops:
#             target_coll.bulk_write(bulk_ops, ordered=False)
#             bulk_ops = []

# if bulk_ops:
#     target_coll.bulk_write(bulk_ops, ordered=False)

# print(f"   ✅ {updated} documents mis à jour (champ ajouté).")
# if not_found:
#     print(f"   ⚠️ {not_found} documents sans correspondance dans {COLL_SOURCE}.")

# # 3. Vérification rapide
# sample = target_coll.find_one({"Commentaire_Client_Original": {"$exists": True}})
# if sample:
#     print("\n📝 Exemple de document mis à jour :")
#     print(f"   _id : {sample.get('_id')}")
#     print(f"   Commentaire_Client_Original : {sample.get('Commentaire_Client_Original')[:100]}...")

# client.close()
# print("\n🔒 Connexion fermée.")



#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ajoute le champ 'Commentaire_Client_Original' dans dataset_unifie
en le copiant depuis commentaires_normalises.Commentaire_Client_Original,
en utilisant Commentaire_Client comme clé de correspondance.
"""

from pymongo import MongoClient, UpdateOne

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI    = "mongodb://localhost:27018/"
DB_NAME      = "telecom_algerie"
COLL_SOURCE  = "commentaires_normalises"   # contient Commentaire_Client_Original
COLL_TARGET  = "dataset_unifie"            # collection à enrichir

BATCH_SIZE   = 500

# ============================================================
# CONNEXION
# ============================================================
print("🔌 Connexion à MongoDB local...")
client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

for col in [COLL_SOURCE, COLL_TARGET]:
    if col not in db.list_collection_names():
        print(f"❌ La collection '{col}' n'existe pas.")
        client.close()
        exit(1)

print("✅ Collections trouvées.")

# ============================================================
# 1. Charger la source dans un dictionnaire
#    clé   = Commentaire_Client  (texte nettoyé — commun aux deux collections)
#    valeur = Commentaire_Client_Original (texte brut original)
# ============================================================
print(f"\n📥 Chargement de '{COLL_SOURCE}'...")
source_cursor = db[COLL_SOURCE].find(
    {},
    {"_id": 0, "Commentaire_Client": 1, "Commentaire_Client_Original": 1}
)

# On construit le dict de correspondance
mapping = {}
for doc in source_cursor:
    cle    = doc.get("Commentaire_Client", "")
    valeur = doc.get("Commentaire_Client_Original", "")
    if cle and valeur:
        mapping[cle] = valeur

print(f"   {len(mapping)} correspondances chargées.")

# ============================================================
# 2. Mettre à jour dataset_unifie
# ============================================================
print(f"\n🔄 Mise à jour de '{COLL_TARGET}' avec 'Commentaire_Client_Original'...")

target_coll = db[COLL_TARGET]
total       = target_coll.count_documents({})
updated     = 0
not_found   = 0
bulk_ops    = []

for doc in target_coll.find({}, {"_id": 1, "Commentaire_Client": 1}):
    cle = doc.get("Commentaire_Client", "")

    original = mapping.get(cle)

    if original:
        bulk_ops.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"Commentaire_Client_Original": original}}
            )
        )
        updated += 1
    else:
        not_found += 1

    # Envoi par lots
    if len(bulk_ops) >= BATCH_SIZE:
        target_coll.bulk_write(bulk_ops, ordered=False)
        bulk_ops = []
        print(f"   {updated} documents traités...")

# Dernier lot
if bulk_ops:
    target_coll.bulk_write(bulk_ops, ordered=False)

# ============================================================
# RÉSUMÉ
# ============================================================
print(f"\n✅ Mise à jour terminée.")
print(f"   Total documents dans target    : {total}")
print(f"   Documents mis à jour           : {updated}")
print(f"   Sans correspondance            : {not_found}")

# ============================================================
# 3. Vérification rapide
# ============================================================
sample = target_coll.find_one({"Commentaire_Client_Original": {"$exists": True}})
if sample:
    print("\n📝 Exemple de document mis à jour :")
    print(f"   _id                          : {sample.get('_id')}")
    print(f"   Commentaire_Client           : {str(sample.get('Commentaire_Client', ''))[:80]}")
    print(f"   Commentaire_Client_Original  : {str(sample.get('Commentaire_Client_Original', ''))[:80]}")

client.close()
print("\n🔒 Connexion fermée.")