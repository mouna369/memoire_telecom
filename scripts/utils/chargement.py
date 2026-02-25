# Utilitaires de chargement
import pandas as pd
from pyspark.sql import SparkSession

def charger_excel(spark, chemin_fichier):
    '''Charge un fichier Excel en Spark DataFrame'''
    pandas_df = pd.read_excel(chemin_fichier)
    return spark.createDataFrame(pandas_df)

def charger_csv(spark, chemin_pattern):
    '''Charge des fichiers CSV en Spark DataFrame'''
    return spark.read.option("header", "true").csv(chemin_pattern)
