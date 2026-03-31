#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config.py – Configuration partagée pour MongoDB Atlas
"""

# ══════════════════════════════════════
# MONGODB ATLAS CONNECTION
# ══════════════════════════════════════
MONGO_URI = "mongodb://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net:27017,ac-1ksfahb-shard-00-01.gejzu4a.mongodb.net:27017,ac-1ksfahb-shard-00-02.gejzu4a.mongodb.net:27017/?ssl=true&replicaSet=atlas-mdnqx7-shard-0&authSource=admin&appName=Cluster0"

# ══════════════════════════════════════
# BASE DE DONNÉES
# ══════════════════════════════════════
DB_NAME = "telecom_algerie"
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