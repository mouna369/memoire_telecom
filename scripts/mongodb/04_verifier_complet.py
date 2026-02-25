#!/usr/bin/env python3
from pymongo import MongoClient
from datetime import datetime

print("="*60)
print("🔍 VÉRIFICATION COMPLÈTE DE MONGODB")
print("="*60)

# Connexion
client = MongoClient('localhost', 27017)
db = client['telecom_algerie']
collection = db['commentaires_bruts']

# 1. Total général
total = collection.count_documents({})
print(f"\n📊 TOTAL: {total} commentaires")

# 2. Répartition par fichier
print("\n📁 RÉPARTITION PAR FICHIER SOURCE:")
pipeline_fichiers = [
    {"$group": {"_id": "$metadata.fichier", "count": {"$sum": 1}}}
]
for doc in collection.aggregate(pipeline_fichiers):
    print(f"   {doc['_id']}: {doc['count']}")

# 3. Répartition par source (réseau social)
print("\n📱 RÉPARTITION PAR SOURCE:")
pipeline_source = [
    {"$group": {"_id": "$source", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
for doc in collection.aggregate(pipeline_source):
    print(f"   {doc['_id']}: {doc['count']}")

# 4. Afficher 3 exemples
print("\n📝 3 PREMIERS COMMENTAIRES:")
for doc in collection.find().limit(3):
    print(f"\n   📄 Fichier: {doc['metadata']['fichier']}")
    print(f"   💬 Texte: {doc['texte_original'][:100]}...")
    print(f"   📅 Date: {doc['date']}")
    print(f"   📱 Source: {doc['source']}")

# 5. Statistiques rapides
print("\n📊 STATISTIQUES:")
print(f"   🔹 Commentaires avec date: {collection.count_documents({'date': {'$ne': ''}})}")
print(f"   🔹 Commentaires sans date: {collection.count_documents({'date': ''})}")
print(f"   🔹 Commentaires Facebook: {collection.count_documents({'source': 'Facebook'})}")
print(f"   🔹 Commentaires Twitter: {collection.count_documents({'source': 'Twitter'})}")

print("\n" + "="*60)
print("✅ VÉRIFICATION TERMINÉE")
print("="*60)
