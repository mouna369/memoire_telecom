import os
import sys
from pyspark.sql import SparkSession
from pymongo import MongoClient

# --- CONFIGURATION ANTI-ERREUR VERSION ---
# On force Spark à ignorer la petite différence de version entre 3.10 et 3.12
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Cette ligne est la clé : elle empêche Spark de paniquer pour la version
os.environ['PYSPARK_IGNORE_VERSION_MISMATCH'] = '1'

print("--- 🚀 DÉBUT DU TEST PFE ---")

# 1. Test MongoDB
try:
    client = MongoClient('mongodb://localhost:27018/')
    db = client['pfe_telecom']
    db.test_connection.insert_one({"status": "ça marche!"})
    print("✅ MongoDB : Connexion réussie !")
except Exception as e:
    print(f"❌ MongoDB : Erreur -> {e}")

# 2. Test Spark
try:
    spark = SparkSession.builder \
        .appName("Test_Mouna_Final") \
        .master("spark://localhost:7077") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    
    print("✅ Spark : Cluster connecté !")
    
    # Création du DataFrame
    df = spark.createDataFrame([("Mouna", 27000), ("Succès", 100)], ["Nom", "Score"])
    df.show()
    
    spark.stop()
    print("--- 🏁 TEST TERMINÉ AVEC SUCCÈS ---")
except Exception as e:
    print(f"❌ Spark : Erreur -> {e}")
