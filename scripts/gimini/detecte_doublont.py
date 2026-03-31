#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_doublons.py – Vérifie et supprime les doublons dans OUTPUT
"""

from pymongo import MongoClient
from collections import Counter
import config

print("─" * 60)
print("  🔍 VÉRIFICATION DES DOUBLONS")
print("─" * 60)

try:
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command('ping')
    db          = client[config.DB_NAME]
    output_coll = db[config.OUTPUT_COLL]
    print(f"  ✅ Connecté à {config.DB_NAME}")
except Exception as e:
    print(f"❌ Connexion échouée : {e}")
    exit(1)

# ── 1. Récupérer tous les mongo_id ─────────────────────────────────────
total = output_coll.count_documents({})
print(f"\n  📦 Total documents dans OUTPUT : {total}")

if total == 0:
    print("  ℹ️  Collection vide, rien à vérifier.")
    client.close()
    exit(0)

all_ids = [
    doc["mongo_id"]
    for doc in output_coll.find({}, {"mongo_id": 1, "_id": 0})
    if doc.get("mongo_id")
]

# ── 2. Compter les occurrences ──────────────────────────────────────────
counter    = Counter(all_ids)
doublons   = {k: v for k, v in counter.items() if v > 1}
nb_unique  = len(counter)
nb_doubles = len(doublons)
nb_extra   = sum(v - 1 for v in doublons.values())  # documents en trop

print(f"  📊 mongo_id uniques     : {nb_unique}")
print(f"  📊 mongo_id en doublon  : {nb_doubles}")
print(f"  📊 Documents en trop    : {nb_extra}")

if nb_doubles == 0:
    print("\n  ✅ Aucun doublon trouvé ! La base est propre.")
    client.close()
    exit(0)

# ── 3. Afficher les doublons ────────────────────────────────────────────
print(f"\n  ⚠️  {nb_doubles} mongo_id apparaissent plusieurs fois :")
print(f"  {'mongo_id':<35} {'occurrences':>10}")
print(f"  {'─'*35} {'─'*10}")
for mid, count in sorted(doublons.items(), key=lambda x: -x[1])[:20]:
    print(f"  {str(mid):<35} {count:>10}x")

if nb_doubles > 20:
    print(f"  ... et {nb_doubles - 20} autres")

# ── 4. Proposer la suppression ──────────────────────────────────────────
print(f"\n  Options :")
print(f"  [1] Supprimer les doublons (garder 1 exemplaire par mongo_id)")
print(f"  [2] Afficher uniquement, ne rien supprimer")
choix = input("  Ton choix (1/2) : ").strip()

if choix != "1":
    print("  ℹ️  Aucune modification effectuée.")
    client.close()
    exit(0)

# ── 5. Supprimer les doublons ───────────────────────────────────────────
supprimés = 0
for mongo_id, count in doublons.items():
    # Récupérer tous les _id MongoDB pour ce mongo_id
    docs = list(output_coll.find({"mongo_id": mongo_id}, {"_id": 1}))
    # Garder le premier, supprimer les autres
    ids_a_supprimer = [d["_id"] for d in docs[1:]]
    result = output_coll.delete_many({"_id": {"$in": ids_a_supprimer}})
    supprimés += result.deleted_count

print(f"\n  🗑️  {supprimés} documents supprimés")
print(f"  ✅ OUTPUT total après nettoyage : {output_coll.count_documents({})}")

client.close()
print("\n  ✅ VÉRIFICATION TERMINÉE")