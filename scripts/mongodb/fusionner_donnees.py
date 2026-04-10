#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fusionner_donnees_atlas.py
Fusionne les collections directement dans MongoDB Atlas
"""

from pymongo import MongoClient
from configuration import MONGO_URI, DB_NAME_ATLAS  # ← Importer seulement ce qui existe
import sys

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATI1  QON
# ═══════════════════════════════════════════════════════════════════════════

# Noms des collections
BRUTS_COLL = "commentaires_bruts"
NORM_COLL = "commentaires_normalises"
LABELED_COLL = "comments_labeled"
DEST_COLL = "analyse_finale_unifiee"

# ═══════════════════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def get_atlas_client():
    """Connexion à MongoDB Atlas"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        print("✅ Connexion à MongoDB Atlas établie")
        return client
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        sys.exit(1)

def fusionner_donnees():
    """Fusionne les données des 3 collections"""
    
    print("=" * 80)
    print("🔄 FUSION DES DONNÉES DANS MONGODB ATLAS")
    print("=" * 80)
    
    client = get_atlas_client()
    
    try:
        # Utiliser la base de données
        db = client["telecom_algerie_new"]  # Nom direct
        
        print(f"✅ Connecté à la base : telecom_algerie_new")
        
        # Collections sources
        bruts = db[BRUTS_COLL]
        norm = db[NORM_COLL]
        labeled = db[LABELED_COLL]
        
        # Collection destination
        dest = db[DEST_COLL]
        
        # Supprimer l'ancienne collection si elle existe
        if DEST_COLL in db.list_collection_names():
            print(f"🗑️  Suppression de l'ancienne collection {DEST_COLL}")
            dest.drop()
        
        # Compter les documents
        total_bruts = bruts.count_documents({})
        total_norm = norm.count_documents({})
        total_labeled = labeled.count_documents({})
        
        print(f"\n📊 STATISTIQUES SOURCES:")
        print(f"   - commentaires_bruts: {total_bruts} documents")
        print(f"   - commentaires_normalises: {total_norm} documents")
        print(f"   - comments_labeled: {total_labeled} documents")
        
        # Fusion avec aggregation pipeline
        print(f"\n🔄 Fusion en cours...")
        
        pipeline = [
            # Étape 1: Lier avec commentaires_normalises
            {
                "$lookup": {
                    "from": NORM_COLL,
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "normalise"
                }
            },
            # Étape 2: Dérouler le tableau
            {
                "$unwind": {
                    "path": "$normalise",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # Étape 3: Lier avec comments_labeled
            {
                "$lookup": {
                    "from": LABELED_COLL,
                    "localField": "_id",
                    "foreignField": "mongo_id",
                    "as": "label"
                }
            },
            # Étape 4: Dérouler le tableau
            {
                "$unwind": {
                    "path": "$label",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # Étape 5: Projeter les champs finaux
            {
                "$project": {
                    "_id": 1,
                    "commentaire_original": "$Commentaire_Client",
                    "commentaire_normalise": "$normalise.Commentaire_Client",
                    "normalized_arabert": "$normalise.normalized_arabert",
                    "normalized_full": "$normalise.normalized_full",
                    "label": "$label.label",
                    "score": "$label.score",
                    "confidence": "$label.confidence",
                    "reason": "$label.reason",
                    "date": 1,
                    "source": 1,
                    "moderateur": 1,
                    "commentaire_moderateur": 1,
                    "metadata": 1,
                    "statut": 1,
                    "labeled": "$normalise.labeled",
                    "labeled_date": "$normalise.labeled_date",
                    "pending_date": "$normalise.pending_date",
                    "emojis_originaux": "$normalise.emojis_originaux",
                    "emojis_sentiment": "$normalise.emojis_sentiment",
                    "date_fusion": {
                        "$literal": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
            },
            # Étape 6: Écrire dans la collection destination
            {
                "$out": DEST_COLL
            }
        ]
        
        # Exécuter la fusion
        bruts.aggregate(pipeline)
        
        print("✅ Fusion terminée!")
        
        # Vérification
        total_dest = dest.count_documents({})
        print(f"\n📊 STATISTIQUES FINALES:")
        print(f"   - Documents fusionnés: {total_dest}")
        
        # Afficher un exemple
        sample = dest.find_one()
        if sample:
            print(f"\n📝 EXEMPLE DE DOCUMENT FUSIONNÉ:")
            print(f"   - ID: {sample.get('_id')}")
            print(f"   - Label: {sample.get('label', 'Non labellisé')}")
            print(f"   - Commentaire: {sample.get('commentaire_original', 'N/A')[:100]}...")
        
        print("\n✨ Opération terminée avec succès sur Atlas !")
        
    except Exception as e:
        print(f"❌ Erreur critique : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        client.close()
        print("🔌 Connexion fermée.")

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Importer datetime pour l'utiliser dans le pipeline
    from datetime import datetime
    fusionner_donnees()