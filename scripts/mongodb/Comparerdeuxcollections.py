#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comparer deux collections MongoDB :
- dataset_unifie_label
- commentaires_normalises

Identifier les documents de commentaires_normalises qui n'ont pas de correspondance
dans dataset_unifie_label, en se basant sur le champ "commentaire_client_original".
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ================= CONFIGURATION =================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLL_REF = "dataset_unifie_sans_doublons"        # collection de référence (celle qui doit contenir tous les commentaires)
COLL_TEST = "dataset_unifie"    # collection à tester (on cherche ce qui manque dans REF)
FIELD = "normalized_arabert"    # champ utilisé pour la comparaison (texte du commentaire)

# Optionnel : champ d'identifiant unique pour éviter les doublons (ex: "_id")
USE_ID_FIELD = False                     # mettre True si on veut comparer par _id au lieu du texte
ID_FIELD = "_id"
# ================= CONNEXION =================
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]

try:
    client.admin.command('ping')
    print("✅ Connexion MongoDB établie.")
except ConnectionFailure:
    print("❌ Impossible de se connecter à MongoDB.")
    exit(1)

# ================= RÉCUPÉRATION DES DONNÉES =================
print(f"\n📥 Chargement des '{FIELD}' depuis {COLL_REF}...")
ref_values = set()
for doc in db[COLL_REF].find({}, {FIELD: 1}):
    val = doc.get(FIELD)
    if val:
        ref_values.add(val.strip())
print(f"   → {len(ref_values)} valeurs uniques dans {COLL_REF}")

print(f"\n📥 Chargement des '{FIELD}' depuis {COLL_TEST}...")
test_values = {}
missing = []
for doc in db[COLL_TEST].find({}, {FIELD: 1, "_id": 1}):
    val = doc.get(FIELD)
    if val:
        val_clean = val.strip()
        test_values[val_clean] = doc["_id"]   # stocker l'ID pour référence
        if val_clean not in ref_values:
            missing.append({
                "_id": doc["_id"],
                FIELD: val_clean
            })
print(f"   → {len(test_values)} documents uniques dans {COLL_TEST}")

# ================= RÉSULTATS =================
print("\n" + "="*70)
print(f"📊 COMPARAISON : {COLL_TEST} vs {COLL_REF}")
print("="*70)
print(f"Total dans {COLL_REF} (valeurs uniques) : {len(ref_values)}")
print(f"Total dans {COLL_TEST} (valeurs uniques) : {len(test_values)}")
print(f"Commentaires présents dans {COLL_TEST} mais ABSENTS de {COLL_REF} : {len(missing)}")

if missing:
    print("\n🔍 Liste des commentaires manquants (premiers 20) :")
    for i, item in enumerate(missing[:20], 1):
        print(f"{i:3}. [{item['_id']}] {item[FIELD][:100]}...")
    if len(missing) > 20:
        print(f"   ... et {len(missing)-20} autres.")

    # Option : exporter dans un fichier
    import csv
    output_file = "commentaires_manquants_bin_unifienoroui_unifiatals.csv"
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["_id", FIELD])
        writer.writeheader()
        writer.writerows(missing)
    print(f"\n💾 Liste complète exportée dans '{output_file}'")
else:
    print("\n✅ Aucun commentaire manquant : les deux collections sont cohérentes.")

client.close()