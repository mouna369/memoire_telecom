#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_collections_links.py – Vérifie les relations entre les collections via _id
"""

from pymongo import MongoClient
from config import MONGO_URI
from bson import ObjectId

def verify_collections_links():
    """
    Vérifie les relations entre les collections
    """
    # Connexion à MongoDB Atlas
    print("🔌 Connexion à MongoDB Atlas...")
    atlas_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    atlas_client.admin.command('ping')
    atlas_db = atlas_client['telecom_algerie_new']
    print("  ✅ Atlas connecté\n")
    
    # Collections
    bruts = atlas_db['commentaires_bruts']
    norm = atlas_db['commentaires_normalises']
    labeled = atlas_db['comments_labeled']
    
    # Statistiques
    total_bruts = bruts.count_documents({})
    total_norm = norm.count_documents({})
    total_labeled = labeled.count_documents({})
    
    print("="*60)
    print("📊 STATISTIQUES GÉNÉRALES")
    print("="*60)
    print(f"commentaires_bruts:        {total_bruts} documents")
    print(f"commentaires_normalises:   {total_norm} documents")
    print(f"comments_labeled:          {total_labeled} documents")
    
    # 1. Vérifier la relation commentaires_bruts -> commentaires_normalises
    print("\n" + "="*60)
    print("🔗 RELATION 1: commentaires_bruts → commentaires_normalises")
    print("="*60)
    print("Relation: commentaires_normalises._id (string) = commentaires_bruts._id (converti en string)")
    
    # Prendre un échantillon de commentaires bruts
    sample_bruts = list(bruts.find().limit(5))
    
    print("\n📝 Échantillon de correspondances:")
    linked_count = 0
    
    for brut in sample_bruts:
        brut_id = brut['_id']
        brut_id_str = str(brut_id)
        
        # Chercher dans commentaires_normalises
        norm_doc = norm.find_one({'_id': brut_id_str})
        
        if norm_doc:
            linked_count += 1
            print(f"✅ Brut ID: {brut_id}")
            print(f"   → Normalisé ID: {norm_doc['_id']}")
            print(f"   → Commentaire: {norm_doc.get('Commentaire_Client', 'N/A')[:50]}...")
            print()
        else:
            print(f"❌ Brut ID: {brut_id} - Aucun document normalisé trouvé")
            print()
    
    # Statistiques globales
    print("\n📊 Statistiques de la relation:")
    # Compter le nombre de documents normalisés qui ont un _id correspondant à un brut
    # Pour cela, on prend les premiers 1000 bruts pour vérifier rapidement
    brut_ids_str = [str(doc['_id']) for doc in bruts.find().limit(1000)]
    norm_with_match = norm.count_documents({'_id': {'$in': brut_ids_str}})
    
    print(f"   Sur 1000 commentaires bruts: {norm_with_match} ont un document normalisé correspondant")
    print(f"   Taux de correspondance: {norm_with_match/1000*100:.1f}%")
    
    # 2. Vérifier la relation commentaires_normalises -> comments_labeled
    print("\n" + "="*60)
    print("🔗 RELATION 2: commentaires_normalises → comments_labeled")
    print("="*60)
    print("Relation: comments_labeled.mongo_id (string) = commentaires_normalises._id (string)")
    
    # Prendre un échantillon de commentaires normalisés
    sample_norm = list(norm.find().limit(5))
    
    print("\n📝 Échantillon de correspondances:")
    labeled_count = 0
    
    for norm_doc in sample_norm:
        norm_id = norm_doc['_id']
        
        # Chercher dans comments_labeled
        labeled_doc = labeled.find_one({'mongo_id': norm_id})
        
        if labeled_doc:
            labeled_count += 1
            print(f"✅ Normalisé ID: {norm_id}")
            print(f"   → Label: {labeled_doc.get('label', 'N/A')}")
            print(f"   → Score: {labeled_doc.get('score', 'N/A')}")
            print(f"   → Confidence: {labeled_doc.get('confidence', 'N/A')}")
            print()
        else:
            print(f"❌ Normalisé ID: {norm_id} - Aucun document labellisé trouvé")
            print()
    
    # Statistiques globales
    print("\n📊 Statistiques de la relation:")
    # Compter le nombre de documents labellisés qui ont un mongo_id existant dans normalisés
    norm_ids = [doc['_id'] for doc in norm.find().limit(1000)]
    labeled_with_match = labeled.count_documents({'mongo_id': {'$in': norm_ids}})
    
    print(f"   Sur 1000 commentaires normalisés: {labeled_with_match} ont un label correspondant")
    print(f"   Taux de correspondance: {labeled_with_match/1000*100:.1f}%")
    
    # 3. Vérification complète des chaînes de relations
    print("\n" + "="*60)
    print("🔗 VÉRIFICATION DE LA CHAÎNE COMPLÈTE")
    print("="*60)
    
    # Prendre un commentaire brut aléatoire
    random_brut = bruts.aggregate([{'$sample': {'size': 1}}]).next()
    brut_id = random_brut['_id']
    brut_id_str = str(brut_id)
    
    print(f"\n📝 Exemple de chaîne complète pour un commentaire:")
    print(f"1. Commentaire brut ID: {brut_id}")
    print(f"   Texte: {random_brut.get('Commentaire_Client', 'N/A')[:100]}...")
    
    # Chercher le normalisé
    norm_doc = norm.find_one({'_id': brut_id_str})
    if norm_doc:
        print(f"\n2. Commentaire normalisé ID: {norm_doc['_id']}")
        print(f"   Texte normalisé: {norm_doc.get('Commentaire_Client', 'N/A')[:100]}...")
        
        # Chercher le labellisé
        labeled_doc = labeled.find_one({'mongo_id': brut_id_str})
        if labeled_doc:
            print(f"\n3. Commentaire labellisé:")
            print(f"   Label: {labeled_doc.get('label', 'N/A')}")
            print(f"   Score: {labeled_doc.get('score', 'N/A')}")
            print(f"   Confidence: {labeled_doc.get('confidence', 'N/A')}")
            print(f"   Raison: {labeled_doc.get('reason', 'N/A')}")
            print("\n✅ Chaîne complète trouvée!")
        else:
            print("\n❌ Aucun label trouvé pour ce commentaire")
    else:
        print("\n❌ Aucun document normalisé trouvé pour ce commentaire")
    
    # 4. Vérifier l'intégrité des données
    print("\n" + "="*60)
    print("🔍 VÉRIFICATION DE L'INTÉGRITÉ")
    print("="*60)
    
    # Vérifier si tous les normalisés ont un brut correspondant
    print("\nVérification 1: Tous les commentaires normalisés ont-ils un brut correspondant?")
    norm_ids_list = [doc['_id'] for doc in norm.find().limit(500)]
    # Convertir les string IDs en ObjectId pour chercher dans bruts
    norm_as_objectid = []
    for nid in norm_ids_list:
        try:
            norm_as_objectid.append(ObjectId(nid))
        except:
            pass
    
    bruts_found = bruts.count_documents({'_id': {'$in': norm_as_objectid}})
    print(f"   Sur 500 normalisés: {bruts_found} ont un brut correspondant")
    
    # Vérifier si tous les labellisés ont un normalisé correspondant
    print("\nVérification 2: Tous les commentaires labellisés ont-ils un normalisé correspondant?")
    labeled_ids = [doc['mongo_id'] for doc in labeled.find().limit(500) if 'mongo_id' in doc]
    norm_found = norm.count_documents({'_id': {'$in': labeled_ids}})
    print(f"   Sur 500 labellisés: {norm_found} ont un normalisé correspondant")
    
    # Vérifier les doublons potentiels
    print("\nVérification 3: Présence de doublons?")
    norm_duplicates = norm.aggregate([
        {'$group': {'_id': '$_id', 'count': {'$sum': 1}}},
        {'$match': {'count': {'$gt': 1}}},
        {'$count': 'duplicates'}
    ])
    norm_dup_count = list(norm_duplicates)
    print(f"   Doublons dans commentaires_normalises: {norm_dup_count[0]['duplicates'] if norm_dup_count else 0}")
    
    labeled_duplicates = labeled.aggregate([
        {'$group': {'_id': '$mongo_id', 'count': {'$sum': 1}}},
        {'$match': {'count': {'$gt': 1}}},
        {'$count': 'duplicates'}
    ])
    labeled_dup_count = list(labeled_duplicates)
    print(f"   Doublons dans comments_labeled: {labeled_dup_count[0]['duplicates'] if labeled_dup_count else 0}")
    
    # Fermeture de la connexion
    atlas_client.close()
    print("\n" + "="*60)
    print("✅ Vérification terminée!")

if __name__ == "__main__":
    verify_collections_links()