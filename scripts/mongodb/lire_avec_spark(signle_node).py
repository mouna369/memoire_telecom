#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script pour lire les données MongoDB avec Spark
"""

from pyspark.sql import SparkSession
from pymongo import MongoClient
import pandas as pd
import time

print("="*60)
print("🚀 CONNEXION SPARK → MONGODB")
print("="*60)

# 1. Récupérer les données de MongoDB
print("\n📂 Récupération depuis MongoDB...")
try:
    client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    db = client['telecom_algerie']
    collection = db['commentaires_bruts']
    
    # Compter d'abord
    total = collection.count_documents({})
    print(f"📊 MongoDB contient {total} commentaires")
    
    # Récupérer les données (par lots pour éviter la mémoire)
    print("⏳ Chargement des données...")
    data = list(collection.find())
    print(f"✅ {len(data)} documents récupérés")
    
except Exception as e:
    print(f"❌ Erreur de connexion MongoDB: {e}")
    exit(1)

# 2. Convertir en pandas DataFrame
print("\n🐼 Conversion en pandas...")
df_pandas = pd.DataFrame(data)

# Enlever l'ID MongoDB (non nécessaire pour Spark)
if '_id' in df_pandas.columns:
    df_pandas = df_pandas.drop('_id', axis=1)

print(f"✅ pandas DataFrame: {len(df_pandas)} lignes")
print(f"📋 Colonnes: {list(df_pandas.columns)}")

# 3. Initialiser Spark
print("\n⚡ Démarrage de Spark...")
spark = SparkSession.builder \
    .appName("MongoDB_vers_Spark") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print(f"✅ Spark démarré (version: {spark.version})")

# 4. Convertir en Spark DataFrame
print("\n🔄 Conversion en Spark DataFrame...")
df_spark = spark.createDataFrame(df_pandas)
print(f"✅ Spark DataFrame: {df_spark.count()} lignes")

# 5. Afficher un aperçu
print("\n📊 Aperçu des 5 premières lignes:")
df_spark.show(5, truncate=50)

# 6. Statistiques rapides
print("\n📈 Statistiques:")
print(f"   • Commentaires Facebook: {df_spark.filter(df_spark.source == 'Facebook').count()}")
print(f"   • Commentaires Twitter: {df_spark.filter(df_spark.source == 'Twitter').count()}")
print(f"   • Commentaires avec date: {df_spark.filter(df_spark.date != '').count()}")

# 7. Sauvegarder en Parquet (pour la suite)
print("\n💾 Sauvegarde en Parquet...")
output_path = "donnees/transformees/depuis_mongodb.parquet"
df_spark.write.mode("overwrite").parquet(output_path)
print(f"✅ Données sauvegardées dans: {output_path}")

print("\n" + "="*60)
print("🎉 SUCCÈS ! Les données sont prêtes dans Spark !")
print("="*60)

# 8. Petit test de requête Spark
print("\n🔍 Test: Répartition par source:")
df_spark.groupBy("source").count().show()

spark.stop()
print("\n✅ Spark arrêté")