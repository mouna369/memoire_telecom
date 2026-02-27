#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script pour lire MongoDB avec Spark multi-node et commencer le traitement
"""

from pyspark.sql import SparkSession
from pymongo import MongoClient
import pandas as pd
import os

print("="*60)
print("🚀 CONNEXION MONGODB → SPARK MULTI-NODE")
print("="*60)

# 1. Connexion au cluster Spark multi-node
print("\n⚡ Connexion au cluster Spark...")
spark = SparkSession.builder \
    .appName("MongoDB_Spark_Cluster") \
    .master("spark://localhost:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print(f"✅ Connecté au cluster Spark")
print(f"   • Version: {spark.version}")
print(f"   • Master: {spark.sparkContext.master}")
print(f"   • Application ID: {spark.sparkContext.applicationId}")

# 2. Lecture depuis MongoDB
print("\n📂 Connexion à MongoDB...")
client = MongoClient('localhost', 27018, serverSelectionTimeoutMS=5000)
db = client['telecom_algerie']
collection = db['commentaires_bruts']

# Vérifier le nombre de documents
total_mongo = collection.count_documents({})
print(f"📊 MongoDB contient {total_mongo} documents")

# Charger les données
print("⏳ Chargement des données depuis MongoDB...")
data = list(collection.find())
print(f"✅ {len(data)} documents récupérés")

# 3. Conversion en pandas DataFrame
print("\n🐼 Conversion en pandas...")
df_pandas = pd.DataFrame(data)

# Supprimer l'ID MongoDB
if '_id' in df_pandas.columns:
    df_pandas = df_pandas.drop('_id', axis=1)

print(f"✅ pandas DataFrame: {len(df_pandas)} lignes")
print(f"📋 Colonnes disponibles: {list(df_pandas.columns)}")

# 4. Conversion en Spark DataFrame (distribué sur le cluster)
print("\n🔄 Conversion en Spark DataFrame...")
df_spark = spark.createDataFrame(df_pandas)
print(f"✅ Spark DataFrame: {df_spark.count()} lignes")
print(f"✅ Les données sont distribuées sur le cluster !")

# 5. Afficher un aperçu
print("\n📊 Aperçu des 5 premières lignes:")
df_spark.show(5, truncate=50)

# 6. Premières analyses (déjà distribuées !)
from pyspark.sql.functions import spark_partition_id, udf
from pyspark.sql.types import StringType
import socket

# 1. On crée une fonction qui récupère le nom du conteneur
def get_hostname():
    return socket.gethostname()

# 2. On enregistre cette fonction pour que Spark puisse l'utiliser sur les Workers
udf_get_hostname = udf(get_hostname, StringType())

print("\n🔍 Vérification de l'identité du travailleur (Worker ID)...")

# 3. On ajoute une colonne qui montre quel worker traite quelle ligne
df_verification = df_spark.withColumn("worker_name", udf_get_hostname()) \
                          .withColumn("partition_id", spark_partition_id())

# 4. On affiche les résultats
df_verification.select("source", "worker_name", "partition_id").show(10)

# 5. On compte combien de lignes chaque worker a traité
print("📊 Répartition du travail par Worker :")
df_verification.groupBy("worker_name").count().show()

# 7. Sauvegarder en Parquet (dans /tmp d'abord)
print("\n💾 Sauvegarde des données...")
tmp_path = "/tmp/donnees_mongodb.parquet"

# Supprimer l'ancien dossier s'il existe
import shutil
if os.path.exists(tmp_path):
    shutil.rmtree(tmp_path)

# Sauvegarder avec Spark (une seule partition pour éviter les problèmes)
df_spark.coalesce(1).write.mode("overwrite").parquet(tmp_path)
print(f"✅ Données sauvegardées dans: {tmp_path}")

# Copier vers le dossier du projet
final_path = "/home/mouna/projet_telecom/donnees/transformees/donnees_mongodb.parquet"
if os.path.exists(final_path):
    shutil.rmtree(final_path)

# Copier avec les permissions
os.system(f"cp -r {tmp_path} {final_path}")
os.system(f"chmod -R 755 {final_path}")
print(f"✅ Données copiées vers: {final_path}")

print("\n" + "="*60)
print("🎉 CONNEXION RÉUSSIE !")
print("="*60)


spark.stop()
print("\n✅ Session Spark terminée")

