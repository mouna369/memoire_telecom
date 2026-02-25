# # test_chargement.py - VERSION FINALE CORRIGÉE
# from pyspark.sql import SparkSession
# import pandas as pd

# print("🚀 Démarrage...")

# # Initialiser Spark
# spark = SparkSession.builder \
#     .appName("Test_Telecom") \
#     .master("local[*]") \
#     .getOrCreate()

# print("✅ Spark démarré")

# # Charger votre Excel avec header=1 (2ème ligne = noms des colonnes)
# print("📂 Chargement du fichier Excel...")
# chemin_fichier = "../donnees/brutes/Social-Media-Analytics.xlsx"

# # 🔴 CORRECTION : Ajouter header=1
# pandas_df = pd.read_excel(chemin_fichier, header=1)
# print(f"✅ {len(pandas_df)} lignes chargées avec pandas")

# # Afficher les noms des colonnes (maintenant corrects)
# print(f"\n📋 Noms des colonnes : {list(pandas_df.columns)}")

# # Afficher la PREMIÈRE ligne (ligne 0)
# print("\n📌 PREMIÈRE ligne (index 0) :")
# print(pandas_df.iloc[0])

# # Afficher la DERNIÈRE ligne
# print("\n📌 DERNIÈRE ligne (index -1) :")
# print(pandas_df.iloc[-1])

# # Afficher le nombre total de lignes
# print(f"\n📊 Total : {len(pandas_df)} lignes (de 0 à {len(pandas_df)-1})")

# # Convertir en Spark
# df = spark.createDataFrame(pandas_df)
# print(f"✅ {df.count()} lignes dans Spark")

# # Afficher les premières lignes
# print("\n📊 Aperçu des données (premières lignes) :")
# df.show(5, truncate=50)

# # Afficher les dernières lignes en Spark
# print("\n📊 Aperçu des données (dernières lignes) :")
# df.tail(5)  # Affiche les 5 dernières lignes

# # Maintenant les colonnes sont correctes, on peut faire des analyses
# print("\n📈 Distribution par réseau social :")
# df.groupBy(pandas_df.columns[1]).count().show()  # Utilise le nom réel de la colonne

# print("\n🎉 Tout est prêt !")
# spark.stop()
import os
import sys
from pyspark.sql import SparkSession
import pandas as pd

# --- CONFIGURATION MULTI-NODE FIXE ---
# 1. Le chemin pour TON PC (Driver)
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable 

# 2. Le chemin pour le CONTENEUR DOCKER (Worker)
# Dans le conteneur, Python est simplement dans /usr/bin/python3
os.environ['PYSPARK_PYTHON'] = 'python3' 

# 3. On ignore toujours le petit décalage 3.10 / 3.12
os.environ['PYSPARK_IGNORE_VERSION_MISMATCH'] = '1'

print("🚀 Démarrage du mode Multi-node (Correction des chemins)...")

# Initialiser Spark
spark = SparkSession.builder \
    .appName("Test_Telecom_MultiNode") \
    .master("spark://localhost:7077") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()
print("✅ Spark connecté au cluster Master")

# Charger votre Excel
print("📂 Chargement du fichier Excel...")
chemin_fichier = "../donnees/brutes/Social-Media-Analytics1.xlsx"

try:
    pandas_df = pd.read_excel(chemin_fichier, header=1)
    print(f"✅ {len(pandas_df)} lignes chargées avec pandas")

    # Convertir en Spark (C'est ici que le travail est envoyé au Worker Docker)
    print("⚙️ Distribution des données vers les Workers...")
    df = spark.createDataFrame(pandas_df)
    
    print(f"✅ {df.count()} lignes distribuées dans Spark")

    # Aperçu
    print("\n📊 Aperçu des données via le Cluster :")
    df.show(5, truncate=50)

    # Distribution par réseau social (Calcul fait par le Worker)
    print("\n📈 Distribution par réseau social (Calcul distribué) :")
    colonne_reseau = pandas_df.columns[1]
    df.groupBy(colonne_reseau).count().show()

except Exception as e:
    print(f"❌ Erreur lors du traitement : {e}")

print("\n🎉 Test Multi-node terminé !")
spark.stop()