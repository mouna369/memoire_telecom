#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
count_emojis_remaining.py
=========================
Compte les emojis restants dans la colonne 'normalized_arabert' 
de la collection 'comments_labeled' sur MongoDB Atlas.
"""

import re
from pymongo import MongoClient
from configuration import MONGO_URI, DB_NAME_ATLAS  # Import depuis ton fichier config

# ============================================================
# CONFIGURATION
# ============================================================
COLLECTION_NAME = "commentaires_normalises"
TARGET_COLUMN = "normalized_arabert"

# Pattern Emoji complet (identique à text_cleaner.py v2.1)
# Couvre Unicode 13+ (🫣, 🪄, etc.) et les séquences ZWJ
_EMOJI_RANGES = (
    '\U0001F300-\U0001F64F'
    '\U0001F680-\U0001F6FF'
    '\u2600-\u26FF'
    '\u2700-\u27BF'
    '\U0001F900-\U0001F9FF'
    '\U0001FA00-\U0001FA6F'
    '\U0001FA70-\U0001FAFF'
    '\U0001FB00-\U0001FBFF'
    '\u2300-\u23FF'
    '\u2B50-\u2B55'
)

EMOJI_PATTERN = re.compile(
    f'[{_EMOJI_RANGES}\uFE0F\u200D]'
    r'(?:[\U0001F3FB-\U0001F3FF])?'   # Modificateurs de teinte
    r'(?:\u200D[^\u200D]+)*',         # Séquences ZWJ
    flags=re.UNICODE
)

print("=" * 80)
print("🔢 COMPTEUR D'EMOJIS RESTANTS DANS 'normalized_arabert'")
print("=" * 80)
print(f"   Collection : {COLLECTION_NAME}")
print(f"   Colonne    : {TARGET_COLUMN}")
print("=" * 80)

try:
    # Connexion à Atlas
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command('ping')
    db = client[DB_NAME_ATLAS]
    collection = db[COLLECTION_NAME]
    print("✅ Connecté à MongoDB Atlas")

    # Vérifier si la collection existe
    if COLLECTION_NAME not in db.list_collection_names():
        print(f"❌ Erreur : La collection '{COLLECTION_NAME}' n'existe pas.")
        client.close()
        exit(1)

    # Compter le total de documents
    total_docs = collection.count_documents({})
    print(f"📊 Total de documents dans la collection : {total_docs}")

    if total_docs == 0:
        print("⚠️ La collection est vide.")
        client.close()
        exit(0)

    print("\n🔍 Analyse en cours... (cela peut prendre quelques secondes)")

    # Variables de comptage
    docs_with_emojis = 0
    total_emojis_found = 0
    sample_emojis = []  # Pour afficher quelques exemples

    # Parcours efficace avec projection (on ne charge que la colonne utile)
    cursor = collection.find({}, {TARGET_COLUMN: 1, "_id": 0})

    for i, doc in enumerate(cursor):
        text = doc.get(TARGET_COLUMN, "")
        if not text:
            continue
        
        # Trouver tous les emojis dans le texte
        emojis = EMOJI_PATTERN.findall(text)
        
        if emojis:
            docs_with_emojis += 1
            count = len(emojis)
            total_emojis_found += count
            
            # Garder quelques exemples pour le rapport (max 5)
            if len(sample_emojis) < 5:
                sample_emojis.append({
                    "text_snippet": text[:50],
                    "emojis_found": emojis
                })

    # Affichage des résultats
    print("\n" + "=" * 80)
    print("📈 RÉSULTATS DE L'ANALYSE")
    print("=" * 80)
    print(f"   • Documents analysés       : {total_docs}")
    print(f"   • Documents AVEC emojis    : {docs_with_emojis}")
    print(f"   • Documents SANS emojis    : {total_docs - docs_with_emojis}")
    print(f"   • Pourcentage contaminé    : {(docs_with_emojis/total_docs*100):.2f}%")
    print(f"   • Nombre TOTAL d'emojis    : {total_emojis_found}")
    
    if sample_emojis:
        print(f"\n🧐 EXEMPLES D'EMOJIS TROUVÉS :")
        for i, ex in enumerate(sample_emojis, 1):
            emojis_str = " ".join(ex['emojis_found'])
            print(f"   {i}. Texte : \"{ex['text_snippet']}...\"")
            print(f"      Emojis détectés : {emojis_str}")

    print("=" * 80)
    
    if total_emojis_found == 0:
        print("✨ SUCCÈS : Aucun emoji trouvé ! La colonne est propre.")
    else:
        print("⚠️ ATTENTION : Des emojis sont encore présents.")
        print("   → Conseil : Exécute le script 'clean_labeled_data.py' pour les supprimer.")

except Exception as e:
    print(f"\n❌ Erreur critique : {e}")
    import traceback
    traceback.print_exc()

finally:
    if 'client' in locals():
        client.close()
        print("\n🔌 Connexion fermée.")