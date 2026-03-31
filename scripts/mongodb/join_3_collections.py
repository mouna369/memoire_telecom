#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
join_3_collections.py – Relie les 3 collections par _id
Résultat final : dataset_unifie avec tous les champs
"""

import csv
import os
from pymongo import MongoClient
from datetime import datetime
import configuration

print("─" * 60)
print("  🔗 JOINTURE DES 3 COLLECTIONS")
print("─" * 60)

try:
    client = MongoClient(configuration.MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command('ping')
    db = client['telecom_algerie_new']
    print(f"  ✅ Connecté à telecom_algerie_new")
except Exception as e:
    print(f"❌ Connexion échouée : {e}")
    exit(1)

coll_brut      = db["commentaires_bruts"]
coll_normalise = db["commentaires_normalises"]
coll_labeled   = db["comments_labeled"]

print(f"\n  📊 État des collections :")
print(f"     • commentaires_bruts       : {coll_brut.count_documents({}):>6}")
print(f"     • commentaires_normalises  : {coll_normalise.count_documents({}):>6}")
print(f"     • comments_labeled         : {coll_labeled.count_documents({}):>6}")

# ── 1. Charger annotations { mongo_id → annotation } ───────────────────
print(f"\n  📦 Chargement des annotations...")
annotations = {}
for doc in coll_labeled.find({}):
    mid = str(doc.get("mongo_id", "")).strip()
    if mid:
        annotations[mid] = {
            "label"      : doc.get("label", ""),
            "score"      : doc.get("score", ""),
            "confidence" : doc.get("confidence", ""),
            "reason"     : doc.get("reason", ""),
        }
print(f"  ✅ {len(annotations)} annotations chargées")

# ── 2. Charger normalisés { _id → doc } ────────────────────────────────
print(f"\n  📦 Chargement des normalisés...")
normalises = {}
for doc in coll_normalise.find({}):
    _id = str(doc.get("_id", "")).strip()
    normalises[_id] = {
        "Commentaire_Client" : doc.get("Commentaire_Client", ""),     # ← sans emojis
        "normalized_arabert" : doc.get("normalized_arabert", ""),     # ← normalisé AraBERT
        "normalized_full"    : doc.get("normalized_full", ""),
        "emojis_originaux"   : str(doc.get("emojis_originaux", [])),
        "emojis_sentiment"   : str(doc.get("emojis_sentiment", [])),
    }
print(f"  ✅ {len(normalises)} normalisés chargés")

# ── 3. Jointure sur commentaires_bruts ─────────────────────────────────
print(f"\n  🔗 Jointure en cours...")

joined     = []
avec_norm  = 0
avec_label = 0
sans_norm  = 0
sans_label = 0

for doc in coll_brut.find({}):
    _id = str(doc.get("_id", "")).strip()

    # Base depuis commentaires_bruts
    row = {
        "mongo_id"                    : _id,
        "Commentaire_Client_Original" : doc.get("Commentaire_Client_Original", ""),  # ← avec emojis
        "commentaire_moderateur"      : doc.get("commentaire_moderateur", ""),
        "date"                        : doc.get("date", ""),
        "source"                      : doc.get("source", ""),
        "moderateur"                  : doc.get("moderateur", ""),
        "statut"                      : doc.get("statut", ""),
    }

    # Jointure avec commentaires_normalises
    norm = normalises.get(_id)
    if norm:
        row["Commentaire_Client"]  = norm["Commentaire_Client"]   # ← sans emojis
        row["normalized_arabert"]  = norm["normalized_arabert"]   # ← normalisé
        row["normalized_full"]     = norm["normalized_full"]
        row["emojis_originaux"]    = norm["emojis_originaux"]
        row["emojis_sentiment"]    = norm["emojis_sentiment"]
        avec_norm += 1
    else:
        row["Commentaire_Client"]  = doc.get("Commentaire_Client_Original", "")
        row["normalized_arabert"]  = ""
        row["normalized_full"]     = ""
        row["emojis_originaux"]    = ""
        row["emojis_sentiment"]    = ""
        sans_norm += 1

    # Jointure avec comments_labeled
    ann = annotations.get(_id)
    if ann:
        row["label"]      = ann["label"]
        row["score"]      = ann["score"]
        row["confidence"] = ann["confidence"]
        row["reason"]     = ann["reason"]
        row["annoté"]     = True
        avec_label += 1
    else:
        row["label"]      = ""
        row["score"]      = ""
        row["confidence"] = ""
        row["reason"]     = ""
        row["annoté"]     = False
        sans_label += 1

    joined.append(row)

print(f"\n  📊 Résultats :")
print(f"     • Total joint             : {len(joined)}")
print(f"     • Avec normalisé          : {avec_norm}")
print(f"     • Sans normalisé          : {sans_norm}")
print(f"     • Avec annotation (label) : {avec_label}")
print(f"     • Sans annotation         : {sans_label}")

# ── 4. Export CSV ───────────────────────────────────────────────────────
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

fieldnames = [
    "mongo_id",
    "Commentaire_Client_Original",   # depuis bruts  (avec emojis)
    "Commentaire_Client",            # depuis normalises (sans emojis)
    "normalized_arabert",            # depuis normalises (normalisé AraBERT)
    "normalized_full",
    "emojis_originaux",
    "emojis_sentiment",
    "source", "date", "moderateur", "statut",
    "commentaire_moderateur",
    "label", "score", "confidence", "reason", "annoté"
]

# CSV complet
csv_complet = f"dataset_complet_{timestamp}.csv"
with open(csv_complet, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(joined)
print(f"\n  💾 CSV complet  → {csv_complet} ({len(joined)} lignes)")

# CSV seulement annotés
annotes    = [r for r in joined if r["annoté"]]
csv_annote = f"dataset_annote_{timestamp}.csv"
with open(csv_annote, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(annotes)
print(f"  💾 CSV annoté   → {csv_annote} ({len(annotes)} lignes)")

# ── 5. Créer collection MongoDB unifiée ─────────────────────────────────
print(f"\n  Créer la collection MongoDB 'dataset_unifie' ?")
choix = input("  (oui/non) : ").strip().lower()

if choix in ["oui", "o", "yes", "y"]:
    db["dataset_unifie"].drop()
    db["dataset_unifie"].insert_many(annotes)
    print(f"  ✅ Collection 'dataset_unifie' créée : {len(annotes)} documents annotés")

client.close()
print("\n  ✅ JOINTURE TERMINÉE AVEC SUCCÈS")