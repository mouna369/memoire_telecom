#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script pour créer la structure de dossiers du projet Telecom NLP
À exécuter UNE SEULE fois au début du projet
"""

import os
import sys

def creer_structure_projet():
    """
    Crée l'arborescence complète du projet
    """
    
    # 📂 Récupérer le chemin courant (là où on exécute le script)
    chemin_base = os.getcwd()
    print(f"📁 Création de la structure dans : {chemin_base}")
    
    # 📋 Définition de tous les dossiers à créer
    dossiers = [
        # Dossier principal des données
        "donnees",
        "donnees/brutes",           # Vos fichiers Excel/CSV originaux
        "donnees/transformees",      # Données nettoyées
        "donnees/resultats",         # Résultats des analyses
        
        # Scripts Python
        "scripts",
        "scripts/nettoyage",         # Scripts de nettoyage NLP
        "scripts/analyse",            # Scripts d'analyse
        "scripts/utils",              # Utilitaires
        
        # Notebooks Jupyter (pour exploration)
        "notebooks",
        
        # Modèles entraînés
        "modeles",
        
        # Dashboard (plus tard)
        "dashboard",
        
        # Documentation
        "docs",
        
        # Tests unitaires
        "tests",
        
        # Sorties et logs
        "outputs",
        "outputs/graphiques",
        "outputs/rapports",
        "outputs/logs"
    ]
    
    # 📦 Créer chaque dossier
    dossiers_crees = 0
    dossiers_existants = 0
    
    for dossier in dossiers:
        chemin_dossier = os.path.join(chemin_base, dossier)
        
        if not os.path.exists(chemin_dossier):
            os.makedirs(chemin_dossier)
            print(f"  ✅ Créé : {dossier}")
            dossiers_crees += 1
        else:
            print(f"  ⏩ Existe déjà : {dossier}")
            dossiers_existants += 1
    
    print(f"\n📊 RÉSUMÉ :")
    print(f"  - {dossiers_crees} nouveaux dossiers créés")
    print(f"  - {dossiers_existants} dossiers existants")
    
    return dossiers_crees + dossiers_existants

def creer_fichiers_readme():
    """
    Crée des fichiers README.md dans chaque dossier pour expliquer leur contenu
    """
    
    readmes = {
        "donnees": "# 📂 Données du projet\n\nCe dossier contient toutes les données.\n\n- **brutes/** : Fichiers originaux fournis par l'entreprise\n- **transformees/** : Données après nettoyage NLP\n- **resultats/** : Sorties des analyses",
        
        "scripts": "# 🐍 Scripts Python\n\n- **nettoyage/** : Prétraitement NLP\n- **analyse/** : Analyses (sentiment, topics...)\n- **utils/** : Fonctions utilitaires",
        
        "notebooks": "# 📓 Notebooks Jupyter\n\nPour l'exploration interactive des données et tests.",
        
        "modeles": "# 🤖 Modèles entraînés\n\nModèles sauvegardés après entraînement.",
        
        "docs": "# 📚 Documentation\n\nRapports, notes, documentation du projet.",
        
        "tests": "# 🧪 Tests unitaires\n\nTests pour valider le code.",
        
        "outputs": "# 📊 Sorties\n\n- **graphiques/** : Visualisations\n- **rapports/** : Rapports d'analyse\n- **logs/** : Fichiers de log"
    }
    
    for dossier, contenu in readmes.items():
        chemin_readme = os.path.join(os.getcwd(), dossier, "README.md")
        if not os.path.exists(chemin_readme):
            with open(chemin_readme, 'w', encoding='utf-8') as f:
                f.write(contenu)
            print(f"  ✅ README créé : {dossier}/README.md")
        else:
            print(f"  ⏩ README existe déjà : {dossier}/README.md")

def creer_fichiers_base():
    """
    Crée des fichiers de base pour démarrer
    """
    
    fichiers = [
        ("scripts/nettoyage/preprocessing.py", 
         """# Script de prétraitement NLP
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import re

def nettoyer_texte(texte):
    '''Nettoie les commentaires en dialecte algérien'''
    if texte is None:
        return ""
    # Mettre en minuscules
    texte = texte.lower()
    # Supprimer les URLs
    texte = re.sub(r'http\S+', '', texte)
    # Supprimer les mentions @
    texte = re.sub(r'@\w+', '', texte)
    # Supprimer la ponctuation
    texte = re.sub(r'[^\w\s]', '', texte)
    return texte.strip()

# UDF Spark
nettoyage_udf = udf(nettoyer_texte, "string")

def preprocess_df(df, colonne_texte):
    '''Applique le nettoyage à un DataFrame Spark'''
    return df.withColumn("texte_nettoye", nettoyage_udf(col(colonne_texte)))
"""),
        
        ("scripts/analyse/sentiment.py",
         """# Analyse de sentiment
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

def preparer_features(df, colonne_texte):
    '''Prépare les features pour l'analyse de sentiment'''
    tokenizer = Tokenizer(inputCol=colonne_texte, outputCol="mots")
    df_tokenized = tokenizer.transform(df)
    
    hashingTF = HashingTF(inputCol="mots", outputCol="tf", numFeatures=10000)
    df_tf = hashingTF.transform(df_tokenized)
    
    idf = IDF(inputCol="tf", outputCol="features")
    df_features = idf.fit(df_tf).transform(df_tf)
    
    return df_features
"""),
        
        ("scripts/utils/chargement.py",
         """# Utilitaires de chargement
import pandas as pd
from pyspark.sql import SparkSession

def charger_excel(spark, chemin_fichier):
    '''Charge un fichier Excel en Spark DataFrame'''
    pandas_df = pd.read_excel(chemin_fichier)
    return spark.createDataFrame(pandas_df)

def charger_csv(spark, chemin_pattern):
    '''Charge des fichiers CSV en Spark DataFrame'''
    return spark.read.option("header", "true").csv(chemin_pattern)
"""),
        
        ("requirements.txt",
         """pyspark>=3.5.0
pandas>=2.0.0
openpyxl>=3.1.0
matplotlib>=3.7.0
seaborn>=0.12.0
jupyter>=1.0.0
findspark>=2.0.0
"""),
        
        (".gitignore",
         """# Environnement virtuel
venv/
env/
ENV/

# Données (trop volumineuses pour Git)
donnees/
*.csv
*.xlsx
*.parquet

# Notebooks
.ipynb_checkpoints/
*.ipynb

# Outputs
outputs/
logs/

# Python
__pycache__/
*.pyc
*.pyo
*.pyd
*.so

# IDE
.vscode/
.idea/
""")
    ]
    
    for chemin_fichier, contenu in fichiers:
        chemin_complet = os.path.join(os.getcwd(), chemin_fichier)
        dossier = os.path.dirname(chemin_complet)
        
        # Créer le dossier si nécessaire
        if not os.path.exists(dossier):
            os.makedirs(dossier)
        
        # Créer le fichier s'il n'existe pas
        if not os.path.exists(chemin_complet):
            with open(chemin_complet, 'w', encoding='utf-8') as f:
                f.write(contenu)
            print(f"  ✅ Fichier créé : {chemin_fichier}")
        else:
            print(f"  ⏩ Fichier existe déjà : {chemin_fichier}")

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🚀 CRÉATION DE LA STRUCTURE DU PROJET TELECOM NLP")
    print("=" * 60)
    
    # 1. Créer les dossiers
    print("\n📁 Création des dossiers...")
    total_dossiers = creer_structure_projet()
    
    # 2. Créer les README
    print("\n📝 Création des fichiers README...")
    creer_fichiers_readme()
    
    # 3. Créer les fichiers de base
    print("\n📄 Création des fichiers de base...")
    creer_fichiers_base()
    
    print("\n" + "=" * 60)
    print("✅ STRUCTURE CRÉÉE AVEC SUCCÈS !")
    print("=" * 60)
    print("\n📂 Votre projet est organisé comme suit :")
    os.system("tree -L 2" if os.name != 'nt' else "dir")
    
    print("\n🎯 PROCHAINES ÉTAPES :")
    print("  1. Placez votre fichier Social-Media-Analytics.xlsx dans donnees/brutes/")
    print("  2. Installez les dépendances : pip install -r requirements.txt")
    print("  3. Commencez par scripts/nettoyage/preprocessing.py")
    print("\nBonne chance ! 🚀")

if __name__ == "__main__":
    main()