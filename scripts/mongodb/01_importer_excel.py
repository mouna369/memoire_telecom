#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT: Importer les fichiers Excel dans MongoDB
À exécuter UNE SEULE fois au début
"""

import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os
import glob

print("="*60)
print("📥 IMPORT DES FICHIERS EXCEL VERS MONGODB")
print("="*60)

# 1. Connexion à MongoDB
print("\n🔌 Connexion à MongoDB...")
try:
    client = MongoClient('localhost', 27018, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("✅ Connecté à MongoDB")
except Exception as e:
    print(f"❌ Erreur: {e}")
    exit(1)

# 2. Créer la base et la collection
db = client['telecom_algerie']
collection = db['commentaires_bruts']

# 3. Chercher les fichiers Excel
print("\n🔍 Recherche des fichiers Excel...")
fichiers = glob.glob("/home/mouna/projet_telecom/donnees/brutes/*.xlsx")

if not fichiers:
    print("❌ Aucun fichier trouvé dans donnees/brutes/")
    print("📁 Vérifie le dossier")
    exit(1)

print(f"✅ {len(fichiers)} fichier(s) trouvé(s):")
for f in fichiers:
    print(f"   - {os.path.basename(f)}")


# 4. Importer chaque fichier
total = 0
for fichier in fichiers:
    print(f"\n📄 Traitement de: {os.path.basename(fichier)}")
    
    # Lire l'Excel
    df = pd.read_excel(fichier, header=1)
    print(f"   📊 {len(df)} lignes lues")
    
    # Convertir en documents
    documents = []
    for idx, row in df.iterrows():
        doc = {
            'Commentaire_Client': str(row.get('Commentaire Client', '')),
            'commentaire_moderateur': str(row.get('Commentaire Modérateur', '')), 
            'date': str(row.get('Date', '')),
            'source': str(row.get('Réseau Social', '')),
            'moderateur': str(row.get('Modérateur', '')),
            'metadata': {
                'fichier': os.path.basename(fichier),
                'ligne': idx + 2,
                'date_import': datetime.now()
            },
            'statut': 'brut'
        }
        documents.append(doc)
    
    # Insérer dans MongoDB
    resultat = collection.insert_many(documents)
    print(f"   ✅ {len(resultat.inserted_ids)} documents insérés")
    total += len(resultat.inserted_ids)

# 5. Résumé
print("\n" + "="*60)
print("📊 RÉSUMÉ FINAL")
print("="*60)
print(f"✅ Total importé: {total} commentaires")
print(f"📁 Dans MongoDB: telecom_algerie.commentaires_bruts")
print("="*60)
