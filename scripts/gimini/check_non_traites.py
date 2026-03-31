#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_non_traites.py – Trouve les commentaires du CSV pas encore dans MongoDB
Usage : python check_non_traites.py <fichier.csv>
"""

import sys, os, csv
import pandas as pd
from pymongo import MongoClient
import config

if len(sys.argv) < 2:
    print("❌ Usage : python check_non_traites.py <fichier.csv>")
    sys.exit(1)

CSV_FILE = sys.argv[1]
if not os.path.exists(CSV_FILE):
    print(f"❌ Fichier non trouvé : {CSV_FILE}")
    sys.exit(1)

print("─" * 60)
print("  🔍 VÉRIFICATION DES COMMENTAIRES NON TRAITÉS")
print("─" * 60)

# ── 1. Lire le CSV ──────────────────────────────────────────────────────
try:
    df = pd.read_csv(
        CSV_FILE,
        encoding="utf-8-sig",
        quotechar='"',
        engine='python',
        on_bad_lines='skip',
        dtype=str
    )
    df.columns = df.columns.str.strip()
    print(f"  ✅ CSV lu : {len(df)} lignes")
    print(f"  📋 Colonnes : {list(df.columns)}")
except Exception as e:
    print(f"❌ Erreur lecture CSV : {e}")
    sys.exit(1)

# Vérifier colonne mongo_id
if "mongo_id" not in df.columns:
    print("❌ Colonne 'mongo_id' introuvable dans le CSV")
    sys.exit(1)

# Nettoyer les mongo_id
df["mongo_id"] = df["mongo_id"].str.strip()
df = df[df["mongo_id"].notna() & (df["mongo_id"] != "")]
ids_csv = set(df["mongo_id"].tolist())
print(f"  📊 mongo_id valides dans CSV : {len(ids_csv)}")

# ── 2. Connexion MongoDB ────────────────────────────────────────────────
try:
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command('ping')
    db          = client[config.DB_NAME]
    output_coll = db[config.OUTPUT_COLL]
    input_coll  = db[config.INPUT_COLL]
    print(f"  ✅ Connecté à {config.DB_NAME}")
except Exception as e:
    print(f"❌ Connexion échouée : {e}")
    sys.exit(1)

# ── 3. Récupérer les mongo_id déjà dans OUTPUT ─────────────────────────
ids_in_output = set(
    doc["mongo_id"]
    for doc in output_coll.find(
        {"mongo_id": {"$in": list(ids_csv)}},
        {"mongo_id": 1, "_id": 0}
    )
    if doc.get("mongo_id")
)

# ── 4. Calculer la différence ───────────────────────────────────────────
non_traites = ids_csv - ids_in_output
deja_traites = ids_csv & ids_in_output

print(f"\n  📊 Résultats :")
print(f"  ✅ Déjà dans OUTPUT  : {len(deja_traites)}")
print(f"  ❌ Pas encore traités : {len(non_traites)}")

if len(non_traites) == 0:
    print("\n  🎉 Tous les commentaires du CSV sont déjà dans la base !")
    client.close()
    sys.exit(0)

# ── 5. Extraire les lignes non traitées ────────────────────────────────
df_non_traites = df[df["mongo_id"].isin(non_traites)].copy()

print(f"\n  📄 Aperçu des non traités :")
print(f"  {'mongo_id':<35} {'texte (50 chars)':>20}")
print(f"  {'─'*35} {'─'*20}")
for _, row in df_non_traites.head(10).iterrows():
    texte = str(row.get("normalized_arabert", ""))[:50]
    print(f"  {str(row['mongo_id']):<35} {texte}")

if len(df_non_traites) > 10:
    print(f"  ... et {len(df_non_traites) - 10} autres")

# ── 6. Sauvegarder les non traités dans un nouveau CSV ─────────────────
output_csv = CSV_FILE.replace(".csv", "_non_traites.csv")
df_non_traites.to_csv(
    output_csv,
    index=False,
    encoding="utf-8-sig",
    quoting=csv.QUOTE_ALL
)
print(f"\n  💾 Sauvegardé → {output_csv} ({len(df_non_traites)} lignes)")
print(f"  ➡️  Tu peux envoyer ce fichier à Gemini pour annoter les manquants")

client.close()
print("\n  ✅ VÉRIFICATION TERMINÉE")