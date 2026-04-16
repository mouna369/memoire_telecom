#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ajoute les champs manquants (label, score, confidence, reason, annoté) aux commentaires
dans commentaires_normalises en utilisant le fichier CSV et la correspondance par _id
"""

from pymongo import MongoClient, UpdateOne
import csv
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
TARGET_COLL = "commentaires_normalises"
CSV_FILE = "commentaire_avec_score.csv"  # Votre fichier CSV
LOG_FILE = "mise_a_jour_log.txt"

# ============================================================
# CONNEXION
# ============================================================
print("🔌 Connexion à MongoDB...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Vérifier que la collection existe
if TARGET_COLL not in db.list_collection_names():
    print(f"❌ La collection '{TARGET_COLL}' n'existe pas.")
    client.close()
    exit(1)

collection = db[TARGET_COLL]

# ============================================================
# LECTURE DU FICHIER CSV
# ============================================================
print(f"\n📥 Lecture du fichier CSV: {CSV_FILE}")

csv_data = []
with open(CSV_FILE, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        csv_data.append(row)

print(f"   {len(csv_data)} lignes lues")

# Afficher les colonnes disponibles
if csv_data:
    print(f"   Colonnes trouvées: {list(csv_data[0].keys())}")

# ============================================================
# PRÉPARATION DES MISES À JOUR
# ============================================================
print("\n🔄 Recherche des correspondances par _id...")

bulk_ops = []
updated = 0
not_found = 0
not_found_list = []
BATCH_SIZE = 500

# Statistiques
stats = {
    "total": len(csv_data),
    "avec_score_negatif": 0,
    "avec_score_positif": 0,
    "avec_score_neutre": 0,
    "avec_annote_true": 0,
    "avec_annote_false": 0
}

for row in csv_data:
    doc_id = row.get('_id', '').strip()
    
    if not doc_id:
        not_found += 1
        not_found_list.append({"erreur": "id_vide", "ligne": row})
        continue
    
    # Préparer les champs à mettre à jour
    update_fields = {}
    
    # label
    if row.get('label'):
        update_fields['label'] = row['label']
        if row['label'] == 'positif':
            stats["avec_score_positif"] += 1
        elif row['label'] == 'negatif':
            stats["avec_score_negatif"] += 1
        elif row['label'] == 'neutre':
            stats["avec_score_neutre"] += 1
    
    # score (convertir en float)
    if row.get('score'):
        try:
            update_fields['score'] = float(row['score'])
        except:
            update_fields['score'] = row['score']
    
    # confidence (convertir en float)
    if row.get('confidence'):
        try:
            update_fields['confidence'] = float(row['confidence'])
        except:
            update_fields['confidence'] = row['confidence']
    
    # reason
    if row.get('reason'):
        update_fields['reason'] = row['reason']
    
    # ⚠️ CHAMP ANNOTÉ - IMPORTANT !
    if row.get('annoté'):
        # Si c'est une string "True"/"False" ou booléen
        if row['annoté'] in ['True', 'true', '1', 'yes', 'oui']:
            update_fields['annoté'] = True
            stats["avec_annote_true"] += 1
        elif row['annoté'] in ['False', 'false', '0', 'no', 'non']:
            update_fields['annoté'] = False
            stats["avec_annote_false"] += 1
        else:
            update_fields['annoté'] = bool(row['annoté'])
    
    # Ajouter la date de mise à jour
    update_fields['date_mise_a_jour_manuel'] = datetime.now()
    
    if not update_fields:
        not_found += 1
        not_found_list.append({"_id": doc_id, "erreur": "aucun_champ_a_mettre_a_jour"})
        continue
    
    # Vérifier si le document existe dans MongoDB
    existing_doc = collection.find_one({"_id": doc_id}, {"_id": 1})
    
    if existing_doc:
        bulk_ops.append(
            UpdateOne(
                {"_id": doc_id},
                {"$set": update_fields}
            )
        )
        updated += 1
        
        if updated % 500 == 0:
            print(f"   {updated} documents préparés...")
    else:
        not_found += 1
        not_found_list.append({
            "_id": doc_id,
            "Commentaire_Client": row.get('normalized_arabert', '')[:50],
            "label": row.get('label'),
            "score": row.get('score'),
            "erreur": "id_non_trouve_dans_mongodb"
        })

# ============================================================
# EXÉCUTION DES MISES À JOUR PAR LOTS
# ============================================================
print(f"\n💾 Exécution des mises à jour...")

if bulk_ops:
    executed = 0
    for i in range(0, len(bulk_ops), BATCH_SIZE):
        batch = bulk_ops[i:i+BATCH_SIZE]
        result = collection.bulk_write(batch, ordered=False)
        executed += len(batch)
        print(f"   Lot {i//BATCH_SIZE + 1}: {len(batch)} documents mis à jour")
    
    print(f"\n   ✅ {executed} documents mis à jour avec succès")
else:
    print("   ⚠️ Aucune mise à jour à effectuer")

# ============================================================
# RÉSUMÉ
# ============================================================
print("\n" + "=" * 70)
print("📊 RÉSULTATS DE LA MISE À JOUR")
print("=" * 70)
print(f"   Total lignes dans CSV        : {stats['total']}")
print(f"   ✅ Documents mis à jour      : {updated}")
print(f"   ❌ Documents non trouvés     : {not_found}")
print(f"\n📈 Répartition des labels:")
print(f"   Positifs : {stats['avec_score_positif']}")
print(f"   Négatifs : {stats['avec_score_negatif']}")
print(f"   Neutres  : {stats['avec_score_neutre']}")
print(f"\n🏷️  Champ annoté:")
print(f"   annoté=True  : {stats['avec_annote_true']}")
print(f"   annoté=False : {stats['avec_annote_false']}")

# ============================================================
# SAUVEGARDE DES NON TROUVÉS
# ============================================================
if not_found_list:
    output_file = "ids_non_trouves.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["_id", "Commentaire_Client", "label", "score", "erreur"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in not_found_list:
            writer.writerow({
                "_id": item.get('_id', ''),
                "Commentaire_Client": item.get('Commentaire_Client', '')[:100],
                "label": item.get('label', ''),
                "score": item.get('score', ''),
                "erreur": item.get('erreur', '')
            })
    print(f"\n💾 {len(not_found_list)} IDs non trouvés sauvegardés dans '{output_file}'")

# ============================================================
# VÉRIFICATION FINALE
# ============================================================
print("\n🔍 VÉRIFICATION FINALE")

# Compter les documents avec et sans score
total = collection.count_documents({})
with_score = collection.count_documents({"score": {"$exists": True}})
without_score = collection.count_documents({"score": {"$exists": False}})
with_label = collection.count_documents({"label": {"$exists": True}})
with_annote = collection.count_documents({"annoté": {"$exists": True}})

print(f"\n   Collection {TARGET_COLL}:")
print(f"   Total documents      : {total}")
print(f"   Avec score           : {with_score}")
print(f"   Sans score           : {without_score}")
print(f"   Avec label           : {with_label}")
print(f"   Avec annoté          : {with_annote}")

# Vérifier un exemple spécifique
print("\n🔍 Vérification d'un document spécifique :")
test_id = "69de5f8d45b81689600c8596"  # Premier de votre liste
test_doc = collection.find_one({"_id": test_id})
if test_doc:
    print(f"   _id: {test_id}")
    print(f"   label: {test_doc.get('label')}")
    print(f"   score: {test_doc.get('score')}")
    print(f"   confidence: {test_doc.get('confidence')}")
    print(f"   reason: {test_doc.get('reason', '')[:80]}...")
    print(f"   annoté: {test_doc.get('annoté')}")
else:
    print(f"   ❌ Document {test_id} non trouvé")

# ============================================================
# SAUVEGARDE DU LOG
# ============================================================
with open(LOG_FILE, 'w', encoding='utf-8') as f:
    f.write(f"Mise à jour effectuée le {datetime.now()}\n")
    f.write(f"Total CSV: {stats['total']}\n")
    f.write(f"Mis à jour: {updated}\n")
    f.write(f"Non trouvés: {not_found}\n")
    f.write(f"Positifs: {stats['avec_score_positif']}\n")
    f.write(f"Négatifs: {stats['avec_score_negatif']}\n")
    f.write(f"Neutres: {stats['avec_score_neutre']}\n")
    f.write(f"annoté=True: {stats['avec_annote_true']}\n")
    f.write(f"annoté=False: {stats['avec_annote_false']}\n")

print(f"\n📝 Log sauvegardé dans '{LOG_FILE}'")

client.close()
print("\n🔒 Connexion fermée.")