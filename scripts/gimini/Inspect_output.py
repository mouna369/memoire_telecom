#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inspect_output.py – Inspecte les documents dans OUTPUT pour voir leur contenu réel
"""

from pymongo import MongoClient
import config

print("─" * 60)
print("  🔬 INSPECTION DE LA COLLECTION OUTPUT")
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

total = output_coll.count_documents({})
print(f"\n  📦 Total documents dans OUTPUT : {total}")

if total == 0:
    print("  ℹ️  Collection vide.")
    client.close()
    exit(0)

# ── 1. Voir les colonnes/champs disponibles ─────────────────────────────
print(f"\n  📋 CHAMPS disponibles dans les documents :")
sample = output_coll.find_one({})
for key, val in sample.items():
    print(f"     • {key:30s} = {str(val)[:60]}")

# ── 2. Compter les docs AVEC et SANS texte arabe ───────────────────────
avec_texte    = output_coll.count_documents({"normalized_arabert": {"$exists": True, "$ne": ""}})
sans_texte    = output_coll.count_documents({"normalized_arabert": {"$exists": False}})
texte_vide    = output_coll.count_documents({"normalized_arabert": ""})

# Vérifier aussi avec le nom mal orthographié (backslash)
avec_backslash = output_coll.count_documents({"normalized\\_arabert": {"$exists": True}})

print(f"\n  📊 État du champ 'normalized_arabert' :")
print(f"     ✅ Avec texte arabe       : {avec_texte}")
print(f"     ❌ Sans le champ          : {sans_texte}")
print(f"     ⚠️  Champ vide            : {texte_vide}")
if avec_backslash > 0:
    print(f"     🐛 Avec backslash (bug)  : {avec_backslash}  ← PROBLÈME DÉTECTÉ")

# ── 3. Afficher 5 exemples de documents ────────────────────────────────
print(f"\n  📄 5 exemples de documents :")
print(f"  {'─'*60}")
for i, doc in enumerate(output_coll.find({}).limit(5)):
    print(f"\n  Document #{i+1}")
    for key, val in doc.items():
        if key == "_id":
            continue
        print(f"     {key:30s} : {str(val)[:70]}")
    print(f"  {'─'*60}")

# ── 4. Si bug backslash détecté → proposer correction ──────────────────
if avec_backslash > 0:
    print(f"\n  🐛 {avec_backslash} documents ont le champ 'normalized\\_arabert' (avec backslash)")
    print(f"  ➡️  Ces documents ont été insérés avec le mauvais nom de colonne")
    print(f"\n  Options :")
    print(f"  [1] Corriger automatiquement (renommer le champ)")
    print(f"  [2] Supprimer ces documents mal insérés")
    print(f"  [3] Ne rien faire")
    choix = input("  Ton choix (1/2/3) : ").strip()

    if choix == "1":
        # Renommer le champ dans tous les documents
        result = output_coll.update_many(
            {"normalized\\_arabert": {"$exists": True}},
            [{"$set": {"normalized_arabert": "$normalized\\_arabert"}},
             {"$unset": "normalized\\_arabert"}]
        )
        print(f"  ✅ {result.modified_count} documents corrigés")

    elif choix == "2":
        result = output_coll.delete_many({"normalized\\_arabert": {"$exists": True}})
        print(f"  🗑️  {result.deleted_count} documents supprimés")
    else:
        print("  ℹ️  Aucune modification.")
# Voir les documents avec backslash
print("\n  📄 3 exemples de documents MAL insérés (avec backslash) :")
print("  " + "─"*60)
for i, doc in enumerate(output_coll.find({"normalized\\_arabert": {"$exists": True}}).limit(3)):
    print(f"\n  Document #{i+1}")
    for key, val in doc.items():
        if key == "_id":
            continue
        print(f"     {key:35s} : {str(val)[:70]}")
    print("  " + "─"*60)
client.close()
print("\n  ✅ INSPECTION TERMINÉE")