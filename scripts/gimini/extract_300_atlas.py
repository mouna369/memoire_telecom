# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# extract_300_atlas.py – Extrait les commentaires depuis MongoDB Atlas
# Colonne CSV : normalized_arabert (nom réel du champ)
# """

# import pandas as pd
# from pymongo import MongoClient
# from datetime import datetime
# import config

# print("─" * 60)
# print(f"  📦 Extraction de {config.BATCH_SIZE} commentaires...")
# print("─" * 60)

# try:
#     client = MongoClient(config.MONGO_URI)
#     db = client[config.DB_NAME]
#     coll = db[config.INPUT_COLL]
#     client.admin.command('ping')
#     print("  ✅ Connecté à MongoDB Atlas")
# except Exception as e:
#     print(f"  ❌ Erreur de connexion : {e}")
#     exit(1)

# total     = coll.count_documents({config.TEXT_COL: {"$exists": True, "$ne": ""}})
# labeled   = coll.count_documents({config.FLAG_COL: True})
# remaining = coll.count_documents({
#     config.TEXT_COL: {"$exists": True, "$ne": ""},
#     config.FLAG_COL: {"$nin": [True, "pending"]}
# })

# print(f"\n  📊 État de la base :")
# print(f"     • Total           : {total}")
# print(f"     • Annotés         : {labeled} ({labeled/total*100:.1f}%)")
# print(f"     • Restants        : {remaining}")

# if remaining <= 0:
#     print("\n  ✅ TOUT EST TERMINÉ ! 🎉")
#     client.close()
#     exit(0)

# # Extraire avec le vrai _id MongoDB
# docs = list(
#     coll.find(
#         {
#             config.TEXT_COL: {"$exists": True, "$ne": ""},
#             config.FLAG_COL: {"$nin": [True, "pending"]}
#         },
#         {"_id": 1, config.TEXT_COL: 1}
#     ).limit(config.BATCH_SIZE)
# )

# # Marquer pending par _id (fiable)
# ids = [d["_id"] for d in docs]
# coll.update_many(
#     {"_id": {"$in": ids}},
#     {"$set": {config.FLAG_COL: "pending", "pending_date": datetime.now()}}
# )
# print(f"  🏷️  {len(ids)} commentaires marqués 'pending'")

# # Export CSV — colonne nommée normalized_arabert
# timestamp  = datetime.now().strftime('%Y%m%d_%H%M%S')
# OUTPUT_CSV = f"batch_{config.BATCH_SIZE}_{timestamp}.csv"

# rows = [{"mongo_id": str(d["_id"]), "normalized_arabert": d[config.TEXT_COL]} for d in docs]
# df   = pd.DataFrame(rows)
# import csva
# df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)

# print(f"  ✅ CSV → {OUTPUT_CSV}  ({len(df)} lignes)")
# client.close()

# print(f"\n  🎉 Prêt pour Gemini !")
# print(f"  ➡️  Copie {OUTPUT_CSV} dans Gemini, puis :")
# print(f"      python mark_as_done_atlas.py <fichier_annoté.csv>")


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_300_atlas.py – Extrait les commentaires depuis MongoDB Atlas
Colonne CSV : normalized_arabert (nom réel du champ)
"""

import pandas as pd
import csv
from pymongo import MongoClient
from datetime import datetime
import config

print("─" * 60)
print(f"  📦 Extraction de {config.BATCH_SIZE} commentaires...")
print("─" * 60)

try:
    client = MongoClient(config.MONGO_URI)
    db = client[config.DB_NAME]
    coll = db[config.INPUT_COLL]
    client.admin.command('ping')
    print("  ✅ Connecté à MongoDB Atlas")
except Exception as e:
    print(f"  ❌ Erreur de connexion : {e}")
    exit(1)

total     = coll.count_documents({config.TEXT_COL: {"$exists": True, "$ne": ""}})
labeled   = coll.count_documents({config.FLAG_COL: True})
remaining = coll.count_documents({
    config.TEXT_COL: {"$exists": True, "$ne": ""},
    config.FLAG_COL: {"$nin": [True, "pending"]}
})

print(f"\n  📊 État de la base :")
print(f"     • Total           : {total}")
print(f"     • Annotés         : {labeled} ({labeled/total*100:.1f}%)")
print(f"     • Restants        : {remaining}")

if remaining <= 0:
    print("\n  ✅ TOUT EST TERMINÉ ! 🎉")
    client.close()
    exit(0)

# Extraire avec le vrai _id MongoDB
docs = list(
    coll.find(
        {
            config.TEXT_COL: {"$exists": True, "$ne": ""},
            config.FLAG_COL: {"$nin": [True, "pending"]}
        },
        {"_id": 1, config.TEXT_COL: 1}
    ).limit(config.BATCH_SIZE)
)

# Marquer pending par _id (fiable)
ids = [d["_id"] for d in docs]
coll.update_many(
    {"_id": {"$in": ids}},
    {"$set": {config.FLAG_COL: "pending", "pending_date": datetime.now()}}
)
print(f"  🏷️  {len(ids)} commentaires marqués 'pending'")

# Export CSV — QUOTE_ALL pour protéger les virgules dans les textes arabes
timestamp  = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_CSV = f"batch_{config.BATCH_SIZE}_{timestamp}.csv"

rows = [{"mongo_id": str(d["_id"]), "normalized_arabert": d[config.TEXT_COL]} for d in docs]
df   = pd.DataFrame(rows)

# ✅ QUOTE_ALL : tous les champs entre guillemets → évite les bugs CSV avec l'arabe
df.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding="utf-8-sig",
    quoting=csv.QUOTE_ALL
)

print(f"  ✅ CSV → {OUTPUT_CSV}  ({len(df)} lignes)")
client.close()

print(f"\n  🎉 Prêt pour Gemini !")
print(f"  ➡️  Copie {OUTPUT_CSV} dans Gemini, puis :")
print(f"      python mark_as_done_atlas.py <fichier_annoté.csv>")