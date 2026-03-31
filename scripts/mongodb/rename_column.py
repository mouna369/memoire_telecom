#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rename_column.py – Renomme Commentaire_Client en Commentaire_Client_Original
dans la collection commentaires_bruts
"""

from pymongo import MongoClient
import configuration

print("─" * 60)
print("  ✏️  RENOMMAGE DE LA COLONNE")
print("─" * 60)

client = MongoClient(configuration.MONGO_URI, serverSelectionTimeoutMS=10000)
db   = client['telecom_algerie_new']
coll = db['commentaires_bruts']

total = coll.count_documents({})
print(f"  📦 Total documents : {total}")

# Renommer le champ
result = coll.update_many(
    {"Commentaire_Client": {"$exists": True}},
    [
        {"$set"  : {"Commentaire_Client_Original": "$Commentaire_Client"}},
        {"$unset": "Commentaire_Client"}
    ]
)

print(f"  ✅ {result.modified_count} documents mis à jour")

# Vérification
sample = coll.find_one({})
print(f"\n  📋 Champs après renommage :")
for key in sample.keys():
    if key != "_id":
        print(f"     • {key}")

client.close()
print("\n  ✅ RENOMMAGE TERMINÉ")