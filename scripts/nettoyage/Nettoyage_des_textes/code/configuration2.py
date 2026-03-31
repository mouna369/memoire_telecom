#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config.py – Configuration partagée pour MongoDB Atlas
"""

from pymongo import MongoClient

# ══════════════════════════════════════
# MONGODB ATLAS CONNECTION
# ══════════════════════════════════════
MONGO_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# ══════════════════════════════════════
# BASE DE DONNÉES
# ══════════════════════════════════════
DB_NAME = "telecom_algerie"
DB_NAME_ATLAS = "telecom_algerie_new"  # ← Ajouter cette ligne
telecom_algerie_new = DB_NAME_ATLAS     # ← Ajouter cette ligne

INPUT_COLL = "commentaires_normalises"
OUTPUT_COLL = "comments_labeled"  # Collection pour les annotations

# ══════════════════════════════════════
# CHAMPS
# ══════════════════════════════════════
TEXT_COL = "normalized_arabert"  # Champ contenant le texte à annoter
FLAG_COL = "labeled"             # Champ pour marquer comme traité

# ══════════════════════════════════════
# PARAMÈTRES
# ══════════════════════════════════════
BATCH_SIZE = 250 # Nombre de commentaires par batch

# ══════════════════════════════════════
# FONCTIONS UTILITAIRES
# ══════════════════════════════════════

def get_atlas_client():
    """
    Retourne un client MongoDB connecté à Atlas
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        # Tester la connexion
        client.admin.command('ping')
        print("✅ Connexion à MongoDB Atlas établie")
        return client
    except Exception as e:
        print(f"❌ Erreur de connexion à MongoDB Atlas: {e}")
        raise

def get_local_client():
    """
    Retourne un client MongoDB connecté en local
    """
    try:
        client = MongoClient('localhost', 27018)
        # Tester la connexion
        client.admin.command('ping')
        print("✅ Connexion à MongoDB local établie")
        return client
    except Exception as e:
        print(f"❌ Erreur de connexion à MongoDB local: {e}")
        raise