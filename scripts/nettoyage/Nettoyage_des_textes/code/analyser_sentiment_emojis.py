#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyser_sentiment_emojis.py – Analyse et attribue un sentiment aux emojis
"""

from pymongo import MongoClient
import re
from datetime import datetime
from collections import Counter

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

MONGO_HOST = 'localhost'
MONGO_PORT = 27018
DB_NAME = 'telecom_algerie'
COLLECTION_NAME = 'commentaires_normalises'

# ═══════════════════════════════════════════════════════════════════════════
# DICTIONNAIRE DES SENTIMENTS PAR EMOJI
# ═══════════════════════════════════════════════════════════════════════════

EMOJI_SENTIMENT_MAP = {
    # Émoticônes positives 😊
    "😊": "positif",
    "😍": "positif",
    "🥰": "positif",
    "😘": "positif",
    "😎": "positif",
    "🤗": "positif",
    "🙂": "positif",
    "👍": "positif",
    "👏": "positif",
    "❤️": "positif",
    "💙": "positif",
    "💚": "positif",
    "💛": "positif",
    "🧡": "positif",
    "💜": "positif",
    "🎉": "positif",
    "✨": "positif",
    "⭐": "positif",
    "🔥": "positif",
    "💪": "positif",
    "🙏": "positif",
    "🤝": "positif",
    "😄": "positif",
    "😃": "positif",
    "😁": "positif",
    "😆": "positif",
    "😂": "positif",  # rire - positif
    "🤣": "positif",  # rire - positif
    "😉": "positif",
    "😜": "positif",
    "🤪": "positif",
    "🥳": "positif",
    
    # Émoticônes négatives 😢
    "😢": "negatif",
    "😭": "negatif",
    "😤": "negatif",
    "😠": "negatif",
    "😡": "negatif",
    "🤬": "negatif",
    "👎": "negatif",
    "💔": "negatif",
    "😞": "negatif",
    "😔": "negatif",
    "😕": "negatif",
    "😟": "negatif",
    "😣": "negatif",
    "😖": "negatif",
    "😫": "negatif",
    "😩": "negatif",
    "🥺": "negatif",
    "😪": "negatif",
    "😴": "negatif",
    "🤢": "negatif",
    "🤮": "negatif",
    "🥴": "negatif",
    "😠": "negatif",
    "😰": "negatif",
    "😨": "negatif",
    "😱": "negatif",
    "😳": "negatif",
    "😓": "negatif",
    
    # Émoticônes neutres 😐
    "😐": "neutre",
    "😑": "neutre",
    "😶": "neutre",
    "🤔": "neutre",
    "🤨": "neutre",
    "🧐": "neutre",
    "😏": "neutre",
    "😒": "neutre",
    "🙄": "neutre",
    "😮": "neutre",
    "😯": "neutre",
    "😲": "neutre",
    "🤯": "neutre",
    "🥱": "neutre",
}

def get_sentiment_for_emoji(emoji):
    """
    Retourne le sentiment pour un emoji spécifique
    """
    return EMOJI_SENTIMENT_MAP.get(emoji, "neutre")

# ═══════════════════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════

def analyser_et_corriger_sentiments():
    """
    Analyse les emojis et corrige le champ emojis_sentiment
    """
    
    print("=" * 80)
    print("🎭 ANALYSE ET CORRECTION DES SENTIMENTS DES EMOJIS")
    print("=" * 80)
    print(f"Base: {DB_NAME}")
    print(f"Collection: {COLLECTION_NAME}")
    print("=" * 80)
    
    client = None
    
    try:
        # Connexion
        print("\n🔌 Connexion à MongoDB local...")
        client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ Connexion réussie")
        
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Compter les documents à corriger
        print("\n📊 Comptage des documents à corriger...")
        
        # Documents avec "emoji" dans emojis_sentiment
        docs_with_emoji_text = collection.count_documents({"emojis_sentiment": "emoji"})
        print(f"   - Documents avec 'emoji' dans emojis_sentiment: {docs_with_emoji_text}")
        
        # Documents avec tableau vide
        docs_with_empty = collection.count_documents({"emojis_sentiment": []})
        print(f"   - Documents avec tableau vide: {docs_with_empty}")
        
        # Documents avec emojis_originaux non vides mais sentiment vide
        docs_with_emojis_no_sentiment = collection.count_documents({
            "emojis_originaux": {"$exists": True, "$ne": None, "$not": {"$size": 0}},
            "$or": [
                {"emojis_sentiment": {"$exists": False}},
                {"emojis_sentiment": []},
                {"emojis_sentiment": None}
            ]
        })
        print(f"   - Documents avec emojis mais sentiment manquant: {docs_with_emojis_no_sentiment}")
        
        total_a_corriger = docs_with_emoji_text + docs_with_emojis_no_sentiment
        print(f"\n📊 Total à corriger: {total_a_corriger}")
        
        if total_a_corriger == 0:
            print("\n✅ Aucun document à corriger!")
            return
        
        # Afficher quelques exemples
        print("\n📝 Exemples de documents à corriger:")
        
        # Exemple avec "emoji"
        sample_emoji = collection.find_one({"emojis_sentiment": "emoji"})
        if sample_emoji:
            print(f"\n   Type 1 - 'emoji' dans sentiment:")
            print(f"      ID: {sample_emoji['_id']}")
            print(f"      emojis_originaux: {sample_emoji.get('emojis_originaux', [])}")
            print(f"      emojis_sentiment (avant): {sample_emoji.get('emojis_sentiment')}")
        
        # Exemple avec tableau vide
        sample_empty = collection.find_one({"emojis_sentiment": []})
        if sample_empty:
            print(f"\n   Type 2 - tableau vide dans sentiment:")
            print(f"      ID: {sample_empty['_id']}")
            print(f"      emojis_originaux: {sample_empty.get('emojis_originaux', [])}")
            print(f"      emojis_sentiment (avant): {sample_empty.get('emojis_sentiment')}")
        
        # Demander confirmation
        print("\n⚠️  Cette opération va analyser chaque emoji et attribuer un sentiment")
        print("   (positif, négatif ou neutre) basé sur le dictionnaire")
        reponse = input("\nVoulez-vous continuer? (oui/non): ")
        
        if reponse.lower() != 'oui':
            print("❌ Opération annulée")
            return
        
        # Traitement
        print("\n🔄 Analyse et correction en cours...")
        
        count = 0
        modified_count = 0
        sentiment_stats = Counter()
        
        # Critères de recherche
        query = {
            "emojis_originaux": {"$exists": True, "$ne": None, "$not": {"$size": 0}},
            "$or": [
                {"emojis_sentiment": "emoji"},
                {"emojis_sentiment": {"$exists": False}},
                {"emojis_sentiment": []},
                {"emojis_sentiment": None}
            ]
        }
        
        for doc in collection.find(query):
            emojis_originaux = doc.get("emojis_originaux", [])
            nouveaux_sentiments = []
            
            # Analyser chaque emoji
            for emoji in emojis_originaux:
                sentiment = get_sentiment_for_emoji(emoji)
                nouveaux_sentiments.append(sentiment)
                sentiment_stats[sentiment] += 1
            
            # Mettre à jour le document
            if nouveaux_sentiments:
                collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "emojis_sentiment": nouveaux_sentiments,
                            "emojis_analyse_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "emojis_analyse_faite": True
                        }
                    }
                )
                modified_count += 1
            
            count += 1
            if count % 500 == 0:
                print(f"   ✅ {count}/{total_a_corriger} documents traités...")
        
        print(f"\n✅ Correction terminée!")
        print(f"   - Documents traités: {count}")
        print(f"   - Documents modifiés: {modified_count}")
        
        # Afficher les statistiques des sentiments
        print(f"\n📊 Distribution des sentiments (nombre d'emojis):")
        for sentiment, nb in sentiment_stats.most_common():
            pourcentage = (nb / sum(sentiment_stats.values()) * 100) if sentiment_stats else 0
            print(f"   - {sentiment}: {nb} ({pourcentage:.1f}%)")
        
        # Vérification finale
        print("\n🔍 VÉRIFICATION FINALE:")
        
        # Vérifier qu'il n'y a plus de "emoji"
        still_has_emoji = collection.count_documents({"emojis_sentiment": "emoji"})
        print(f"   - Documents avec 'emoji': {still_has_emoji}")
        
        # Vérifier les tableaux vides
        still_empty = collection.count_documents({"emojis_sentiment": []})
        print(f"   - Documents avec tableau vide: {still_empty}")
        
        # Afficher un exemple corrigé
        corrected_sample = collection.find_one({"emojis_analyse_faite": True})
        if corrected_sample:
            print(f"\n📝 EXEMPLE DE DOCUMENT CORRIGÉ:")
            print(f"   ID: {corrected_sample['_id']}")
            print(f"   emojis_originaux: {corrected_sample.get('emojis_originaux', [])}")
            print(f"   emojis_sentiment (après): {corrected_sample.get('emojis_sentiment', [])}")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client:
            client.close()
            print("\n🔌 Connexion fermée")

# ═══════════════════════════════════════════════════════════════════════════
# FONCTION DE TEST
# ═══════════════════════════════════════════════════════════════════════════

def tester_sentiment_emojis():
    """
    Teste l'analyse de sentiment sur un seul document
    """
    print("=" * 80)
    print("🧪 TEST D'ANALYSE DE SENTIMENT SUR UN SEUL DOCUMENT")
    print("=" * 80)
    
    client = None
    
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Trouver un document avec des emojis
        test_doc = collection.find_one({
            "emojis_originaux": {"$exists": True, "$ne": None, "$not": {"$size": 0}}
        })
        
        if not test_doc:
            print("❌ Aucun document avec emojis trouvé")
            return
        
        print("\n📝 DOCUMENT:")
        print(f"   ID: {test_doc['_id']}")
        print(f"   emojis_originaux: {test_doc.get('emojis_originaux', [])}")
        print(f"   emojis_sentiment (actuel): {test_doc.get('emojis_sentiment', [])}")
        
        # Analyser
        emojis = test_doc.get("emojis_originaux", [])
        nouveaux_sentiments = []
        
        print(f"\n🔍 Analyse des emojis:")
        for emoji in emojis:
            sentiment = get_sentiment_for_emoji(emoji)
            nouveaux_sentiments.append(sentiment)
            print(f"   {emoji} → {sentiment}")
        
        print(f"\n📝 NOUVEAU SENTIMENT:")
        print(f"   {nouveaux_sentiments}")
        
        reponse = input("\n✅ Appliquer cette correction à ce document? (oui/non): ")
        
        if reponse.lower() == "oui":
            collection.update_one(
                {"_id": test_doc["_id"]},
                {
                    "$set": {
                        "emojis_sentiment": nouveaux_sentiments,
                        "emojis_analyse_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "emojis_analyse_faite": True
                    }
                }
            )
            print("✅ Correction appliquée!")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
    finally:
        if client:
            client.close()

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "="*80)
    print("📌 MENU")
    print("="*80)
    print("1. Tester l'analyse de sentiment sur un seul document")
    print("2. Analyser et corriger TOUS les documents")
    print("3. Afficher les statistiques détaillées")
    print("="*80)
    
    choix = input("\nVotre choix (1, 2 ou 3): ")
    
    if choix == "1":
        tester_sentiment_emojis()
    elif choix == "2":
        analyser_et_corriger_sentiments()
    elif choix == "3":
        # Option pour afficher les stats
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        print("\n📊 STATISTIQUES DÉTAILLÉES:")
        
        total = collection.count_documents({})
        print(f"\n   Total documents: {total}")
        
        # Types de emojis_sentiment
        with_emoji_text = collection.count_documents({"emojis_sentiment": "emoji"})
        with_empty = collection.count_documents({"emojis_sentiment": []})
        with_null = collection.count_documents({"emojis_sentiment": None})
        with_array = collection.count_documents({
            "emojis_sentiment": {"$type": "array"},
            "emojis_sentiment": {"$ne": []}
        })
        
        print(f"\n   Répartition de emojis_sentiment:")
        print(f"      - 'emoji' (texte): {with_emoji_text}")
        print(f"      - [] (vide): {with_empty}")
        print(f"      - null: {with_null}")
        print(f"      - Tableau non vide: {with_array}")
        
        # emojis_originaux
        with_originaux = collection.count_documents({
            "emojis_originaux": {"$exists": True, "$ne": None, "$not": {"$size": 0}}
        })
        print(f"\n   Documents avec emojis_originaux: {with_originaux}")
        
        client.close()
    else:
        print("❌ Choix invalide")