import os
import sys

# --- FORCER LES VERSIONS AVANT TOUT IMPORT SPARK ---
# On dit au worker d'utiliser son propre python
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
# On dit au driver d'utiliser ton environnement virtuel actuel
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# LA CLÉ : On ignore le décalage 3.10 vs 3.12
os.environ['PYSPARK_IGNORE_VERSION_MISMATCH'] = '1'

import socket
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def check_worker_identity(x):
    """Cette fonction s'exécute directement sur les Workers Docker"""
    return f"Calculé par le Worker Docker ID: {socket.gethostname()}"

def main():
    print("🚀 Initialisation du Cluster Spark Multi-node...")
    
    try:
        # Initialisation de la session vers le Master Docker
        spark = SparkSession.builder \
            .appName("Mouna_PFE_MultiNode_Check") \
            .master("spark://localhost:7077") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()

       # Ou change le niveau de log
        spark.sparkContext.setLogLevel("ERROR")
        print("✅ Connexion au Master réussie !")

        # 1. Chargement d'un échantillon du fichier Excel
        chemin_fichier = "../donnees/brutes/Social-Media-Analytics1.xlsx"
        print(f"📂 Lecture du fichier : {chemin_fichier}")
        
        pdf = pd.read_excel(chemin_fichier, header=1).head(50) # On prend 50 lignes
        
        # 2. Conversion en DataFrame Spark (Distribution vers le cluster)
        df = spark.createDataFrame(pdf)
        
        # 3. Création de l'UDF (User Defined Function) pour tracer le calcul
        worker_id_udf = udf(check_worker_identity, StringType())

        # 4. Exécution du test de distribution
        print("⚙️ Envoi des tâches aux Workers...")
        df_resultat = df.withColumn("Provenance_Calcul", worker_id_udf(df.columns[0]))

        # 5. Affichage des résultats
        print("\n" + "="*50)
        print("📊 RÉSULTAT DU TEST D'ARCHITECTURE")
        print("="*50)
        
        # On affiche les machines distinctes qui ont travaillé
        df_resultat.select("Provenance_Calcul").distinct().show(truncate=False)

        print("💡 Si vous voyez un ID comme '4f9e729a1b', c'est le conteneur Docker.")
        print("💡 Si vous voyez 'DESKTOP-ULUGMCN', le calcul est resté sur votre PC.")
        print("="*50)

    except Exception as e:
        print(f"❌ Erreur lors du test : {e}")
    
    finally:
        if 'spark' in locals():
            spark.stop()
            print("\n🏁 Session Spark fermée.")

if __name__ == "__main__":
    main()