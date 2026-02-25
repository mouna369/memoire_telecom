# scripts/mongodb/01_importer_excel.py (version finale)
from pyspark.sql import SparkSession
from pymongo import MongoClient
import pandas as pd
import os
import shutil
import time

print("="*60)
print("🚀 LECTURE MONGODB → SPARK CLUSTER")
print("="*60)

# 1. Vérifier le dossier de sortie
output_dir = "/home/mouna/projet_telecom/donnees/transformees"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"📁 Dossier créé: {output_dir}")
else:
    print(f"📁 Dossier existant: {output_dir}")
    print(f"📁 Permissions: {oct(os.stat(output_dir).st_mode)[-3:]}")

# 2. Connexion au cluster
print("\n⚡ Connexion au cluster Spark...")
spark = SparkSession.builder \
    .appName("Nettoyage_Telecom") \
    .master("spark://localhost:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print(f"✅ Spark connecté - Version: {spark.version}")

# 3. Lecture depuis MongoDB
print("\n📂 Connexion à MongoDB...")
client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
db = client['telecom_algerie']
collection = db['commentaires_bruts']

total_mongo = collection.count_documents({})
print(f"📊 MongoDB contient {total_mongo} documents")

print("⏳ Chargement des données...")
data = list(collection.find())
print(f"✅ {len(data)} documents récupérés")

# 4. Conversion pandas
print("\n🐼 Conversion en pandas...")
df_pandas = pd.DataFrame(data)
if '_id' in df_pandas.columns:
    df_pandas = df_pandas.drop('_id', axis=1)
print(f"✅ pandas DataFrame: {len(df_pandas)} lignes")
print(f"📋 Colonnes: {list(df_pandas.columns)}")

# 5. Conversion Spark
print("\n🔄 Conversion en Spark DataFrame...")
df = spark.createDataFrame(df_pandas)
nb_lignes = df.count()
print(f"✅ Spark DataFrame: {nb_lignes} lignes")

# 6. Sauvegarde
print("\n💾 Sauvegarde en Parquet...")
output_path = f"{output_dir}/donnees_brutes.parquet"

# Supprimer l'ancien dossier s'il existe
if os.path.exists(output_path):
    print(f"🗑️ Suppression de l'ancien dossier: {output_path}")
    shutil.rmtree(output_path)

# Sauvegarde avec moins de partitions pour éviter les erreurs
df.coalesce(1).write.mode("overwrite").parquet(output_path)
print(f"✅ Données sauvegardées dans: {output_path}")

# 7. Vérification
print("\n🔍 Vérification de la sauvegarde...")
if os.path.exists(output_path):
    taille = sum(os.path.getsize(os.path.join(dirpath, filename)) 
                 for dirpath, _, filenames in os.walk(output_path) 
                 for filename in filenames)
    print(f"✅ Dossier créé: {output_path}")
    print(f"📊 Taille: {taille / 1024 / 1024:.2f} Mo")
else:
    print(f"❌ Erreur: le dossier n'a pas été créé!")

print("\n" + "="*60)
print("🎉 TRAITEMENT TERMINÉ AVEC SUCCÈS !")
print("="*60)

spark.stop()