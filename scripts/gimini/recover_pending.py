#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
recover_pending.py – Récupère les commentaires bloqués en 'pending'
Pour les cas où Gemini a sauté des lignes lors de l'annotation
"""

import pandas as pd
import csv
from pymongo import MongoClient
from datetime import datetime
import config

print("─" * 60)
print("  🔄 RÉCUPÉRATION DES PENDING BLOQUÉS")
print("─" * 60)

try:
    client = MongoClient(config.MONGO_URI)
    db   = client[config.DB_NAME]
    coll = db[config.INPUT_COLL]
    output_coll = db[config.OUTPUT_COLL]
    client.admin.command('ping')
    print("  ✅ Connecté à MongoDB Atlas")
except Exception as e:
    print(f"  ❌ Erreur de connexion : {e}")
    exit(1)

# ── 1. Trouver les pending ───────────────────────────────────────────────
pending_docs = list(coll.find(
    {config.FLAG_COL: "pending"},
    {"_id": 1, config.TEXT_COL: 1, "pending_date": 1}
))

print(f"\n  📊 Pending trouvés : {len(pending_docs)}")

if not pending_docs:
    print("  ✅ Aucun pending bloqué — tout est propre !")
    client.close()
    exit(0)

# ── 2. Vérifier lesquels sont déjà dans OUTPUT (annotés malgré tout) ────
output_ids  = set(output_coll.distinct("mongo_id"))
truly_stuck = []
already_done = []

for doc in pending_docs:
    mid = str(doc["_id"])
    if mid in output_ids:
        already_done.append(doc)
    else:
        truly_stuck.append(doc)

print(f"  ✅ Déjà dans OUTPUT (juste pas sync)  : {len(already_done)}")
print(f"  ⚠️  Vraiment perdus (à ré-annoter)    : {len(truly_stuck)}")

# ── 3. Réparer ceux qui sont dans OUTPUT mais pas sync dans INPUT ────────
if already_done:
    print(f"\n  🔧 Réparation des {len(already_done)} déjà annotés...")
    fixed = 0
    for doc in already_done:
        res = coll.update_one(
            {"_id": doc["_id"]},
            {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
        )
        if res.modified_count > 0:
            fixed += 1
    print(f"  ✅ {fixed} documents resynchronisés dans INPUT")

# ── 4. Remettre les vraiment perdus en libre + exporter CSV ─────────────
if truly_stuck:
    print(f"\n  📤 Export des {len(truly_stuck)} perdus pour ré-annotation...")

    # Remettre à None (libérer le pending)
    stuck_ids = [d["_id"] for d in truly_stuck]
    coll.update_many(
        {"_id": {"$in": stuck_ids}},
        {"$unset": {"pending_date": ""},
         "$set": {config.FLAG_COL: "pending"}}  # garder pending jusqu'à export
    )

    # Marquer comme pending avec nouvelle date
    timestamp  = datetime.now().strftime('%Y%m%d_%H%M%S')
    OUTPUT_CSV = f"recover_pending_{timestamp}.csv"

    rows = [{"mongo_id": str(d["_id"]), "normalized_arabert": d[config.TEXT_COL]} for d in truly_stuck]
    df   = pd.DataFrame(rows)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)

    print(f"  ✅ CSV → {OUTPUT_CSV}  ({len(df)} lignes)")
    print(f"\n  ➡️  Copie {OUTPUT_CSV} dans Gemini pour ré-annoter")
    print(f"      puis : python mark_as_done_atlas.py {OUTPUT_CSV}")

# ── 5. Résumé final ──────────────────────────────────────────────────────
pending_restants = coll.count_documents({config.FLAG_COL: "pending"})
print(f"\n  📊 Pending restants après nettoyage : {pending_restants}")

client.close()
print("\n" + "─" * 60)
print("  ✅ RÉCUPÉRATION TERMINÉE")
print("─" * 60)