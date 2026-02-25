#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyse de la fréquence des mots clés dans les commentaires
Lecture DIRECTEMENT depuis MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, regexp_replace, length, udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer
import matplotlib.pyplot as plt
from collections import Counter
import pandas as pd
from pymongo import MongoClient
import re

print("="*70)
print("📊 ANALYSE DE FRÉQUENCE DES MOTS CLÉS (depuis MongoDB)")
print("="*70)

# 1. Récupérer les données de MongoDB
print("\n📂 Connexion à MongoDB...")
try:
    client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    db = client['telecom_algerie']
    collection = db['commentaires_bruts']
    
    total_mongo = collection.count_documents({})
    print(f"✅ Connecté à MongoDB - {total_mongo} documents trouvés")
    
    # Récupérer les données
    print("⏳ Chargement des données...")
    data = list(collection.find())
    print(f"✅ {len(data)} documents récupérés")
    
except Exception as e:
    print(f"❌ Erreur de connexion MongoDB: {e}")
    exit(1)

# 2. Créer pandas DataFrame
print("\n🐼 Conversion en pandas...")
df_pandas = pd.DataFrame(data)

# Enlever l'ID MongoDB
if '_id' in df_pandas.columns:
    df_pandas = df_pandas.drop('_id', axis=1)

print(f"✅ pandas DataFrame: {len(df_pandas)} lignes")
print(f"📋 Colonnes disponibles: {list(df_pandas.columns)}")

# 3. Initialiser Spark
print("\n⚡ Démarrage de Spark...")
spark = SparkSession.builder \
    .appName("Analyse_Frequence_Mots") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print(f"✅ Spark démarré (version: {spark.version})")

# 4. Convertir en Spark DataFrame
print("\n🔄 Conversion en Spark DataFrame...")
df = spark.createDataFrame(df_pandas)
print(f"✅ Spark DataFrame: {df.count()} lignes")

# 5. Identifier la colonne de texte
colonne_texte = None
for col_name in df.columns:
    if 'texte' in col_name.lower() or 'commentaire' in col_name.lower():
        colonne_texte = col_name
        break

if colonne_texte is None:
    # Chercher parmi les colonnes disponibles
    print("📋 Colonnes disponibles:")
    for c in df.columns:
        print(f"   - {c}")
    colonne_texte = df.columns[0]  # Première colonne par défaut

print(f"📝 Colonne analysée: {colonne_texte}")

# 6. NETTOYAGE LÉGER pour l'analyse
print("\n🧹 Préparation du texte...")

# Fonction de nettoyage
def nettoyer_texte(texte):
    if texte is None:
        return ""
    texte = str(texte)
    # Enlever URLs
    texte = re.sub(r'http\S+|www\S+', '', texte)
    # Enlever mentions
    texte = re.sub(r'@\w+', '', texte)
    # Garder lettres arabes/françaises
    texte = re.sub(r'[^\w\s\u0600-\u06FFa-zA-Z]', ' ', texte)
    # Mettre en minuscules pour le français
    texte = texte.lower()
    # Normaliser espaces
    texte = re.sub(r'\s+', ' ', texte).strip()
    return texte

# Appliquer le nettoyage avec UDF
nettoyage_udf = udf(nettoyer_texte, StringType())
df_clean = df.withColumn("texte_propre", nettoyage_udf(col(colonne_texte)))

# Enlever les lignes vides
df_clean = df_clean.filter(length(col("texte_propre")) > 3)

nb_commentaires = df_clean.count()
print(f"✅ {nb_commentaires} commentaires après nettoyage léger")

# 7. TOKENISATION
print("\n🔪 Tokenisation...")
tokenizer = Tokenizer(inputCol="texte_propre", outputCol="mots")
df_tokens = tokenizer.transform(df_clean)

# 8. EXTRAIRE TOUS LES MOTS
print("📥 Extraction de tous les mots...")
tous_mots = df_tokens.select(explode("mots").alias("mot")).collect()
liste_mots = [row.mot for row in tous_mots if row.mot and len(row.mot) > 1]

print(f"📊 Total de mots: {len(liste_mots)}")
print(f"📊 Mots uniques: {len(set(liste_mots))}")

# 9. COMPTER LES FRÉQUENCES
print("\n🔢 Calcul des fréquences...")
frequence = Counter(liste_mots)

# Top 50 mots
top_50 = frequence.most_common(50)

# 10. AFFICHER LES RÉSULTATS
print("\n" + "="*70)
print("🏆 TOP 20 MOTS LES PLUS FRÉQUENTS")
print("="*70)
print(f"{'Rang':<5} {'Mot':<30} {'Fréquence':<10} {'Pourcentage':<10}")
print("-"*60)

total_mots = len(liste_mots)
for i, (mot, count) in enumerate(top_50[:20], 1):
    pourcentage = (count / total_mots) * 100
    print(f"{i:<5} {mot:<30} {count:<10} {pourcentage:.2f}%")

# 11. SAUVEGARDER DANS DES FICHIERS
print("\n💾 Sauvegarde des résultats...")

# Créer le dossier si nécessaire
import os
os.makedirs("donnees/resultats", exist_ok=True)

# Sauvegarder en CSV
df_top = pd.DataFrame(top_50, columns=['mot', 'frequence'])
df_top.to_csv("donnees/resultats/top_mots.csv", index=False, encoding='utf-8-sig')
print("✅ Fichier CSV créé: donnees/resultats/top_mots.csv")

# Sauvegarder en texte lisible
with open("donnees/resultats/analyse_frequence.txt", "w", encoding="utf-8") as f:
    f.write("="*70 + "\n")
    f.write("ANALYSE DE FRÉQUENCE DES MOTS CLÉS\n")
    f.write("="*70 + "\n\n")
    f.write(f"Source: MongoDB (telecom_algerie.commentaires_bruts)\n")
    f.write(f"Total commentaires analysés: {nb_commentaires}\n")
    f.write(f"Total mots: {total_mots}\n")
    f.write(f"Mots uniques: {len(set(liste_mots))}\n\n")
    f.write("TOP 50 MOTS:\n")
    f.write("-"*60 + "\n")
    for i, (mot, count) in enumerate(top_50[:50], 1):
        pourcentage = (count / total_mots) * 100
        f.write(f"{i:3d}. {mot:<30} {count:6d} ({pourcentage:.2f}%)\n")

print("✅ Rapport texte créé: donnees/resultats/analyse_frequence.txt")

# 12. ANALYSE PAR SOURCE
print("\n📱 Analyse par source:")
if 'source' in df_clean.columns:
    df_source = df_clean.groupBy("source").count().orderBy(col("count").desc())
    df_source.show()
    
    # Sauvegarder aussi
    df_source_pd = df_source.toPandas()
    df_source_pd.to_csv("donnees/resultats/repartition_source.csv", index=False, encoding='utf-8-sig')
    print("✅ Répartition par source sauvegardée")
else:
    print("⚠️ Colonne 'source' non trouvée")

# 13. AFFICHER QUELQUES EXEMPLES
print("\n📝 Exemples de commentaires:")
df_clean.select(colonne_texte, "texte_propre").show(5, truncate=60)

print("\n" + "="*70)
print("🎉 ANALYSE TERMINÉE AVEC SUCCÈS !")
print("="*70)
print("\nFichiers créés dans 'donnees/resultats/':")
print("   📄 top_mots.csv")
print("   📄 analyse_frequence.txt")
print("   📄 repartition_source.csv")

spark.stop()
print("\n✅ Spark arrêté")