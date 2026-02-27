# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# # scripts/nettoyage/01_supprimer_urls_multinode.py - VERSION CORRIGÉE

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf, spark_partition_id
# from pyspark.sql.types import StringType, IntegerType
# import re
# from pymongo import MongoClient
# from datetime import datetime
# import os
# import socket
# import pandas as pd  # 👈 IMPORT MANQUANT AJOUTÉ

# def supprimer_urls(texte):
#     """Supprime les URLs d'un texte - Version améliorée"""
#     if texte is None or not isinstance(texte, str):
#         return texte
    
#     # Patterns améliorés pour détecter tous les types d'URLs
#     patterns = [
#         r'https?://\S+',           # URLs complètes
#         r'www\.\S+',                # www.example.com
#         r'https?://(?:\s|$)',       # https:// seul suivi d'espace ou fin
#         r'https?://$',              # https:// en fin de chaîne
#         r'\bhttps?://\b',           # https:// comme mot isolé
#         r'http://(?:\s|$)',         # http:// seul
#         r'http://$'                 # http:// en fin de chaîne
#     ]
    
#     texte_propre = texte
#     for pattern in patterns:
#         texte_propre = re.sub(pattern, '', texte_propre, flags=re.IGNORECASE)
    
#     # Supprimer les espaces multiples
#     texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
#     return texte_propre if texte_propre else None

# def detecter_urls(texte):
#     """Détecte si un texte contient des URLs"""
#     if texte is None or not isinstance(texte, str):
#         return 0
    
#     patterns = [
#         r'https?://',
#         r'www\.',
#         r'https?://(?:\s|$)',
#         r'https?://$'
#     ]
    
#     for pattern in patterns:
#         if re.search(pattern, texte, re.IGNORECASE):
#             return 1
#     return 0

# def compter_urls(texte):
#     """Compte le nombre d'URLs dans un texte"""
#     if texte is None or not isinstance(texte, str):
#         return 0
    
#     pattern = r'https?://\S+|www\.\S+|https?://(?:\s|$)|https?://$'
#     return len(re.findall(pattern, texte, re.IGNORECASE))

# def get_worker_name():
#     """Retourne le nom du worker"""
#     return socket.gethostname()

# print("="*70)
# print("🔍 ÉTAPE 1 : SUPPRESSION DES URLS - MODE MULTI-NODE")
# print("="*70)

# # 1. CONNEXION À MONGODB
# print("\n📂 Connexion à MongoDB...")
# try:
#     client = MongoClient('localhost', 27018)
#     db = client['telecom_algerie']
#     collection_source = db['commentaires_bruts']
#     total_docs = collection_source.count_documents({})
#     print(f"✅ Connexion MongoDB réussie")
#     print(f"📊 Collection source: {total_docs} documents")
# except Exception as e:
#     print(f"❌ Erreur de connexion MongoDB: {e}")
#     exit(1)

# # 2. CONNEXION AU CLUSTER SPARK
# print("\n⚡ Connexion au cluster Spark multi-node...")

# spark = SparkSession.builder \
#     .appName("Suppression_URLs_MultiNode") \
#     .master("spark://localhost:7077") \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
#     .getOrCreate()

# print("✅ Cluster Spark multi-node connecté")

# # 3. CHARGER LES DONNÉES AVEC PYMONGO
# print("\n📥 Chargement des données avec PyMongo...")

# # Charger tous les documents
# print("   Récupération des documents...")
# data = list(collection_source.find({}))
# print(f"   📊 {len(data)} documents chargés")

# # Convertir les ObjectId en string
# print("   🔄 Conversion des ObjectId...")
# for doc in data:
#     doc['_id'] = str(doc['_id'])

# # Créer DataFrame Spark
# print("   📊 Création du DataFrame Spark...")
# df_spark = spark.createDataFrame(data)
# total_lignes = df_spark.count()
# print(f"✅ {total_lignes} documents chargés dans Spark")

# # 4. IDENTIFIER LES WORKERS
# print("\n🔍 RÉPARTITION SUR LES WORKERS:")

# worker_udf = udf(get_worker_name, StringType())

# df_with_workers = df_spark \
#     .withColumn("partition_id", spark_partition_id()) \
#     .withColumn("worker_name", worker_udf())

# print("   Distribution des données:")
# df_with_workers.groupBy("worker_name", "partition_id").count().show()

# # 5. ENREGISTRER LES UDF
# print("\n🔄 Enregistrement des fonctions...")
# supprimer_urls_udf = udf(supprimer_urls, StringType())
# detecter_urls_udf = udf(detecter_urls, IntegerType())
# compter_urls_udf = udf(compter_urls, IntegerType())

# # 6. ANALYSE AVANT NETTOYAGE
# print("\n🔎 ANALYSE : Recherche des URLs...")

# df_analyse = df_with_workers \
#     .withColumn("urls_avant", detecter_urls_udf(col("Commentaire_Client"))) \
#     .withColumn("nb_urls_avant", compter_urls_udf(col("Commentaire_Client")))

# total = df_analyse.count()
# avec_urls_avant = df_analyse.filter(col("urls_avant") == 1).count()
# total_urls = df_analyse.agg({"nb_urls_avant": "sum"}).collect()[0][0] or 0

# print(f"\n📊 STATISTIQUES AVANT NETTOYAGE:")
# print(f"   ┌────────────────────────────────────┐")
# print(f"   │ Total documents        : {total:<15} │")
# print(f"   │ Documents avec URLs    : {avec_urls_avant:<15} │")
# print(f"   │ URLs détectées         : {total_urls:<15} │")
# print(f"   │ Pourcentage            : {(avec_urls_avant/total*100):<15.2f}% │")
# print(f"   └────────────────────────────────────┘")

# # 7. NETTOYAGE
# print("\n🧹 SUPPRESSION DES URLS EN COURS...")

# df_nettoye = df_analyse \
#     .withColumn("Commentaire_Client_propre", supprimer_urls_udf(col("Commentaire_Client"))) \
#     .withColumn("commentaire_moderateur_propre", supprimer_urls_udf(col("commentaire_moderateur"))) \
#     .withColumn("urls_apres", detecter_urls_udf(col("Commentaire_Client_propre")))

# # 8. STATISTIQUES APRÈS NETTOYAGE
# avec_urls_apres = df_nettoye.filter(col("urls_apres") == 1).count()
# supprimees = avec_urls_avant - avec_urls_apres
# taux = (supprimees / avec_urls_avant * 100) if avec_urls_avant > 0 else 0

# print(f"\n📊 STATISTIQUES APRÈS NETTOYAGE:")
# print(f"   ┌────────────────────────────────────┐")
# print(f"   │ Documents avec URLs avant : {avec_urls_avant:<15} │")
# print(f"   │ Documents avec URLs après : {avec_urls_apres:<15} │")
# print(f"   │ URLs supprimées           : {supprimees:<15} │")
# print(f"   │ Taux de succès            : {taux:<15.2f}% │")
# print(f"   └────────────────────────────────────┘")

# # 9. STATISTIQUES PAR WORKER
# print("\n📊 PERFORMANCE PAR WORKER:")
# worker_perf = df_nettoye.groupBy("worker_name").agg(
#     {"urls_avant": "sum", "urls_apres": "sum", "Commentaire_Client": "count"}
# ).withColumnRenamed("sum(urls_avant)", "urls_trouvees") \
#  .withColumnRenamed("sum(urls_apres)", "urls_restantes") \
#  .withColumnRenamed("count(Commentaire_Client)", "documents")

# worker_perf.show()

# # 10. PRÉPARATION POUR MONGODB
# print("\n💾 PRÉPARATION POUR SAUVEGARDE...")

# df_final = df_nettoye.select(
#     "_id",
#     col("Commentaire_Client_propre").alias("Commentaire_Client"),
#     col("commentaire_moderateur_propre").alias("commentaire_moderateur"),
#     "date",
#     "source",
#     "moderateur",
#     "metadata",
#     "statut"
# )

# # 11. SAUVEGARDE DANS MONGODB
# print("\n📁 SAUVEGARDE DANS MONGODB...")

# # Convertir en Pandas par lots pour éviter les problèmes de mémoire
# print("   🔄 Conversion en Pandas...")
# df_pandas = df_final.toPandas()
# print(f"   ✅ {len(df_pandas)} lignes converties")

# # Collection destination
# collection_dest = db['commentaires_sans_urls_multinode']
# collection_dest.delete_many({})
# print("   🧹 Collection destination vidée")

# # Insérer par lots
# print("   📥 Insertion par lots...")
# batch_size = 500
# total_batches = (len(df_pandas) + batch_size - 1) // batch_size

# for i in range(0, len(df_pandas), batch_size):
#     batch_num = i//batch_size + 1
#     batch = df_pandas.iloc[i:i+batch_size].to_dict('records')
    
#     # Convertir les NaN en None pour MongoDB
#     for doc in batch:
#         for key, value in doc.items():
#             if pd.isna(value):  # 👈 MAINTENANT PANDAS EST IMPORTÉ
#                 doc[key] = None
    
#     collection_dest.insert_many(batch)
#     print(f"   ✓ Lot {batch_num}/{total_batches}: {len(batch)} documents")

# print(f"\n✅ {len(df_pandas)} documents sauvegardés dans 'commentaires_sans_urls_multinode'")

# # 12. VÉRIFICATION FINALE
# print("\n🔎 VÉRIFICATION FINALE...")

# # Vérifier avec différents patterns
# patterns_verif = [
#     r'https?://\S+',
#     r'www\.\S+',
#     r'https?://(?:\s|$)',
#     r'https?://$'
# ]

# print("\n   Vérification pattern par pattern:")
# for pattern in patterns_verif:
#     count = collection_dest.count_documents({
#         "Commentaire_Client": {"$regex": pattern, "$options": "i"}
#     })
#     print(f"   • {pattern[:20]}...: {count} documents")

# # Vérification globale
# urls_restantes = collection_dest.count_documents({
#     "Commentaire_Client": {"$regex": "https?://|www\.", "$options": "i"}
# })

# print(f"\n📊 RÉSULTAT FINAL:")
# print(f"   • Documents avec URLs restantes: {urls_restantes}")
# if urls_restantes == 0:
#     print("   ✅ SUCCÈS : Toutes les URLs ont été supprimées !")
# else:
#     print(f"   ⚠️ ATTENTION : {urls_restantes} URLs restantes")

# # 13. RAPPORT
# print("\n📄 CRÉATION DU RAPPORT...")

# rapport = f"""
# {"="*70}
# RAPPORT DE SUPPRESSION DES URLS - MODE MULTI-NODE
# {"="*70}

# Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Mode : Multi-node Spark Cluster

# 📊 STATISTIQUES GLOBALES:
#    • Total documents traités    : {total}
#    • Documents avec URLs (avant): {avec_urls_avant}
#    • URLs détectées (avant)     : {total_urls}
#    • Documents avec URLs (après): {avec_urls_apres}
#    • URLs supprimées            : {supprimees}
#    • Taux de succès             : {taux:.2f}%

# 📁 STOCKAGE:
#    • Collection source      : telecom_algerie.commentaires_bruts
#    • Collection destination : telecom_algerie.commentaires_sans_urls_multinode
#    • Documents sauvegardés  : {len(df_pandas)}

# 🔍 VÉRIFICATION FINALE:
#    • URLs restantes détectées : {urls_restantes}
#    • Statut : {"✅ SUCCÈS" if urls_restantes == 0 else "⚠️ ÉCHEC"}

# ⚡ DISTRIBUTION:
#    • Workers utilisés : {df_with_workers.select("worker_name").distinct().count()}
#    • Voir détails dans les logs pour la répartition
# """

# # Sauvegarder le rapport
# os.makedirs("donnees/resultats", exist_ok=True)
# rapport_path = "donnees/resultats/rapport_urls_multinode.txt"
# with open(rapport_path, "w", encoding="utf-8") as f:
#     f.write(rapport)
# print(f"✅ Rapport sauvegardé: {rapport_path}")

# # 14. RÉSUMÉ FINAL
# print("\n" + "="*70)
# print("📊 RÉSUMÉ FINAL - MODE MULTI-NODE")
# print("="*70)
# print(f"📥 Documents traités    : {total}")
# print(f"🔗 URLs détectées       : {total_urls}")
# print(f"📝 Documents avec URLs  : {avec_urls_avant}")
# print(f"✅ URLs supprimées      : {supprimees}")
# print(f"📈 Taux de succès       : {taux:.2f}%")
# print(f"\n📁 Collection MongoDB:")
# print(f"   • telecom_algerie.commentaires_sans_urls_multinode")
# print("="*70)

# print("\n🎉 SUPPRESSION DES URLS TERMINÉE EN MODE MULTI-NODE !")

# # Fermer les connexions
# spark.stop()
# client.close()
# print("\n🔌 Connexions fermées")




# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# # scripts/nettoyage/01_supprimer_urls_multinode.py - VERSION AVEC MESURE DE TEMPS

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf, spark_partition_id
# from pyspark.sql.types import StringType, IntegerType
# import re
# from pymongo import MongoClient
# from datetime import datetime
# import os
# import socket
# import pandas as pd
# import time  # 👈 POUR MESURER LE TEMPS

# def supprimer_urls(texte):
#     """Supprime les URLs d'un texte - Version améliorée"""
#     if texte is None or not isinstance(texte, str):
#         return texte
    
#     # Patterns améliorés pour détecter tous les types d'URLs
#     patterns = [
#         r'https?://\S+',           # URLs complètes
#         r'www\.\S+',                # www.example.com
#         r'https?://(?:\s|$)',       # https:// seul suivi d'espace ou fin
#         r'https?://$',              # https:// en fin de chaîne
#         r'\bhttps?://\b',           # https:// comme mot isolé
#         r'http://(?:\s|$)',         # http:// seul
#         r'http://$'                 # http:// en fin de chaîne
#     ]
    
#     texte_propre = texte
#     for pattern in patterns:
#         texte_propre = re.sub(pattern, '', texte_propre, flags=re.IGNORECASE)
    
#     # Supprimer les espaces multiples
#     texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
#     return texte_propre if texte_propre else None

# def detecter_urls(texte):
#     """Détecte si un texte contient des URLs"""
#     if texte is None or not isinstance(texte, str):
#         return 0
    
#     patterns = [
#         r'https?://',
#         r'www\.',
#         r'https?://(?:\s|$)',
#         r'https?://$'
#     ]
    
#     for pattern in patterns:
#         if re.search(pattern, texte, re.IGNORECASE):
#             return 1
#     return 0

# def compter_urls(texte):
#     """Compte le nombre d'URLs dans un texte"""
#     if texte is None or not isinstance(texte, str):
#         return 0
    
#     pattern = r'https?://\S+|www\.\S+|https?://(?:\s|$)|https?://$'
#     return len(re.findall(pattern, texte, re.IGNORECASE))

# def get_worker_name():
#     """Retourne le nom du worker"""
#     return socket.gethostname()

# # 📊 DÉBUT DU CHRONOMÈTRAGE GLOBAL
# temps_debut_global = time.time()

# print("="*70)
# print("🔍 ÉTAPE 1 : SUPPRESSION DES URLS - MODE MULTI-NODE")
# print("="*70)

# # 1. CONNEXION À MONGODB
# print("\n📂 Connexion à MongoDB...")
# try:
#     client = MongoClient('localhost', 27018)
#     db = client['telecom_algerie']
#     collection_source = db['commentaires_bruts']
#     total_docs = collection_source.count_documents({})
#     print(f"✅ Connexion MongoDB réussie")
#     print(f"📊 Collection source: {total_docs} documents")
# except Exception as e:
#     print(f"❌ Erreur de connexion MongoDB: {e}")
#     exit(1)

# # 2. CONNEXION AU CLUSTER SPARK
# print("\n⚡ Connexion au cluster Spark multi-node...")
# temps_debut_spark = time.time()

# # spark = SparkSession.builder \
# #     .appName("Suppression_URLs_MultiNode") \
# #     .master("spark://localhost:7077") \
# #     .config("spark.executor.memory", "2g") \
# #     .config("spark.executor.cores", "2") \
# #     .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
# #     .getOrCreate()

# spark = SparkSession.builder \
#     .appName("Suppression_URLs_MultiNode") \
#     .master("spark://spark-master:7077") \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "12") \
#     .getOrCreate()

# temps_fin_spark = time.time()
# print(f"✅ Cluster Spark multi-node connecté en {temps_fin_spark - temps_debut_spark:.2f} secondes")

# # 3. CHARGER LES DONNÉES AVEC PYMONGO
# print("\n📥 Chargement des données avec PyMongo...")
# temps_debut_chargement = time.time()

# # Charger tous les documents
# print("   Récupération des documents...")
# data = list(collection_source.find({}))
# print(f"   📊 {len(data)} documents chargés")

# # Convertir les ObjectId en string
# print("   🔄 Conversion des ObjectId...")
# for doc in data:
#     doc['_id'] = str(doc['_id'])

# # Créer DataFrame Spark
# print("   📊 Création du DataFrame Spark...")
# df_spark = spark.createDataFrame(data)
# total_lignes = df_spark.count()

# temps_fin_chargement = time.time()
# print(f"✅ {total_lignes} documents chargés dans Spark en {temps_fin_chargement - temps_debut_chargement:.2f} secondes")

# # 4. IDENTIFIER LES WORKERS
# print("\n🔍 RÉPARTITION SUR LES WORKERS:")

# worker_udf = udf(get_worker_name, StringType())

# df_with_workers = df_spark \
#     .withColumn("partition_id", spark_partition_id()) \
#     .withColumn("worker_name", worker_udf())

# print("   Distribution des données:")
# df_with_workers.groupBy("worker_name", "partition_id").count().show()

# # 5. ENREGISTRER LES UDF
# print("\n🔄 Enregistrement des fonctions...")
# supprimer_urls_udf = udf(supprimer_urls, StringType())
# detecter_urls_udf = udf(detecter_urls, IntegerType())
# compter_urls_udf = udf(compter_urls, IntegerType())

# # 6. ANALYSE AVANT NETTOYAGE
# print("\n🔎 ANALYSE : Recherche des URLs...")
# temps_debut_analyse = time.time()

# df_analyse = df_with_workers \
#     .withColumn("urls_avant", detecter_urls_udf(col("Commentaire_Client"))) \
#     .withColumn("nb_urls_avant", compter_urls_udf(col("Commentaire_Client")))

# total = df_analyse.count()
# avec_urls_avant = df_analyse.filter(col("urls_avant") == 1).count()
# total_urls = df_analyse.agg({"nb_urls_avant": "sum"}).collect()[0][0] or 0

# temps_fin_analyse = time.time()
# print(f"\n📊 STATISTIQUES AVANT NETTOYAGE (analyse en {temps_fin_analyse - temps_debut_analyse:.2f}s):")
# print(f"   ┌────────────────────────────────────┐")
# print(f"   │ Total documents        : {total:<15} │")
# print(f"   │ Documents avec URLs    : {avec_urls_avant:<15} │")
# print(f"   │ URLs détectées         : {total_urls:<15} │")
# print(f"   │ Pourcentage            : {(avec_urls_avant/total*100):<15.2f}% │")
# print(f"   └────────────────────────────────────┘")

# # 7. NETTOYAGE
# print("\n🧹 SUPPRESSION DES URLS EN COURS...")
# temps_debut_nettoyage = time.time()

# df_nettoye = df_analyse \
#     .withColumn("Commentaire_Client_propre", supprimer_urls_udf(col("Commentaire_Client"))) \
#     .withColumn("commentaire_moderateur_propre", supprimer_urls_udf(col("commentaire_moderateur"))) \
#     .withColumn("urls_apres", detecter_urls_udf(col("Commentaire_Client_propre")))

# # Forcer l'exécution des transformations
# df_nettoye.cache().count()

# temps_fin_nettoyage = time.time()
# print(f"✅ Nettoyage terminé en {temps_fin_nettoyage - temps_debut_nettoyage:.2f} secondes")

# # 8. STATISTIQUES APRÈS NETTOYAGE
# avec_urls_apres = df_nettoye.filter(col("urls_apres") == 1).count()
# supprimees = avec_urls_avant - avec_urls_apres
# taux = (supprimees / avec_urls_avant * 100) if avec_urls_avant > 0 else 0

# print(f"\n📊 STATISTIQUES APRÈS NETTOYAGE:")
# print(f"   ┌────────────────────────────────────┐")
# print(f"   │ Documents avec URLs avant : {avec_urls_avant:<15} │")
# print(f"   │ Documents avec URLs après : {avec_urls_apres:<15} │")
# print(f"   │ URLs supprimées           : {supprimees:<15} │")
# print(f"   │ Taux de succès            : {taux:<15.2f}% │")
# print(f"   └────────────────────────────────────┘")

# # 9. STATISTIQUES PAR WORKER
# print("\n📊 PERFORMANCE PAR WORKER:")
# worker_perf = df_nettoye.groupBy("worker_name").agg(
#     {"urls_avant": "sum", "urls_apres": "sum", "Commentaire_Client": "count"}
# ).withColumnRenamed("sum(urls_avant)", "urls_trouvees") \
#  .withColumnRenamed("sum(urls_apres)", "urls_restantes") \
#  .withColumnRenamed("count(Commentaire_Client)", "documents")

# worker_perf.show()

# # 10. PRÉPARATION POUR MONGODB
# print("\n💾 PRÉPARATION POUR SAUVEGARDE...")
# temps_debut_preparation = time.time()

# df_final = df_nettoye.select(
#     "_id",
#     col("Commentaire_Client_propre").alias("Commentaire_Client"),
#     col("commentaire_moderateur_propre").alias("commentaire_moderateur"),
#     "date",
#     "source",
#     "moderateur",
#     "metadata",
#     "statut"
# )

# temps_fin_preparation = time.time()
# print(f"✅ Préparation terminée en {temps_fin_preparation - temps_debut_preparation:.2f} secondes")

# # 11. SAUVEGARDE DANS MONGODB
# print("\n📁 SAUVEGARDE DANS MONGODB...")
# temps_debut_sauvegarde = time.time()

# # Convertir en Pandas
# print("   🔄 Conversion en Pandas...")
# df_pandas = df_final.toPandas()
# print(f"   ✅ {len(df_pandas)} lignes converties")

# # Collection destination
# collection_dest = db['commentaires_sans_urls_multinode2']
# collection_dest.delete_many({})
# print("   🧹 Collection destination vidée")

# # Insérer par lots
# print("   📥 Insertion par lots...")
# batch_size = 500
# total_batches = (len(df_pandas) + batch_size - 1) // batch_size

# for i in range(0, len(df_pandas), batch_size):
#     batch_num = i//batch_size + 1
#     batch = df_pandas.iloc[i:i+batch_size].to_dict('records')
    
#     # Convertir les NaN en None pour MongoDB
#     for doc in batch:
#         for key, value in doc.items():
#             if pd.isna(value):
#                 doc[key] = None
    
#     collection_dest.insert_many(batch)
#     print(f"   ✓ Lot {batch_num}/{total_batches}: {len(batch)} documents")

# temps_fin_sauvegarde = time.time()
# print(f"\n✅ {len(df_pandas)} documents sauvegardés dans 'commentaires_sans_urls_multinode2'")
# print(f"   ⏱️  Temps de sauvegarde: {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f} secondes")

# # 12. VÉRIFICATION FINALE
# print("\n🔎 VÉRIFICATION FINALE...")
# temps_debut_verification = time.time()

# # Vérifier avec différents patterns
# patterns_verif = [
#     r'https?://\S+',
#     r'www\.\S+',
#     r'https?://(?:\s|$)',
#     r'https?://$'
# ]

# print("\n   Vérification pattern par pattern:")
# for pattern in patterns_verif:
#     count = collection_dest.count_documents({
#         "Commentaire_Client": {"$regex": pattern, "$options": "i"}
#     })
#     print(f"   • {pattern[:20]}...: {count} documents")

# # Vérification globale
# urls_restantes = collection_dest.count_documents({
#     "Commentaire_Client": {"$regex": "https?://|www\.", "$options": "i"}
# })

# temps_fin_verification = time.time()
# print(f"\n📊 RÉSULTAT FINAL (vérification en {temps_fin_verification - temps_debut_verification:.2f}s):")
# print(f"   • Documents avec URLs restantes: {urls_restantes}")
# if urls_restantes == 0:
#     print("   ✅ SUCCÈS : Toutes les URLs ont été supprimées !")
# else:
#     print(f"   ⚠️ ATTENTION : {urls_restantes} URLs restantes")

# # 🏁 FIN DU CHRONOMÈTRAGE GLOBAL
# temps_fin_global = time.time()
# temps_total = temps_fin_global - temps_debut_global

# # 13. RAPPORT AVEC TEMPS D'EXÉCUTION
# print("\n📄 CRÉATION DU RAPPORT...")

# rapport = f"""
# {"="*70}
# RAPPORT DE SUPPRESSION DES URLS - MODE MULTI-NODE
# {"="*70}

# Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Mode : Multi-node Spark Cluster

# ⏱️  TEMPS D'EXÉCUTION:
#    • Connexion Spark        : {temps_fin_spark - temps_debut_spark:.2f}s
#    • Chargement données     : {temps_fin_chargement - temps_debut_chargement:.2f}s
#    • Analyse des URLs       : {temps_fin_analyse - temps_debut_analyse:.2f}s
#    • Nettoyage URLs         : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s
#    • Préparation DataFrame  : {temps_fin_preparation - temps_debut_preparation:.2f}s
#    • Sauvegarde MongoDB     : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s
#    • Vérification           : {temps_fin_verification - temps_debut_verification:.2f}s
#    • ──────────────────────────────────
#    • TEMPS TOTAL            : {temps_total:.2f}s
#    • Documents par seconde  : {total / temps_total:.2f} doc/s

# 📊 STATISTIQUES GLOBALES:
#    • Total documents traités    : {total}
#    • Documents avec URLs (avant): {avec_urls_avant}
#    • URLs détectées (avant)     : {total_urls}
#    • Documents avec URLs (après): {avec_urls_apres}
#    • URLs supprimées            : {supprimees}
#    • Taux de succès             : {taux:.2f}%

# 📁 STOCKAGE:
#    • Collection source      : telecom_algerie.commentaires_bruts
#    • Collection destination : telecom_algerie.commentaires_sans_urls_multinode2
#    • Documents sauvegardés  : {len(df_pandas)}

# 🔍 VÉRIFICATION FINALE:
#    • URLs restantes détectées : {urls_restantes}
#    • Statut : {"✅ SUCCÈS" if urls_restantes == 0 else "⚠️ ÉCHEC"}

# ⚡ DISTRIBUTION:
#    • Workers utilisés : {df_with_workers.select("worker_name").distinct().count()}
# """

# # Sauvegarder le rapport
# os.makedirs("donnees/resultats", exist_ok=True)
# rapport_path = "donnees/resultats/rapport_urls_multinode2.txt"
# with open(rapport_path, "w", encoding="utf-8") as f:
#     f.write(rapport)
# print(f"✅ Rapport sauvegardé: {rapport_path}")

# # 14. RÉSUMÉ FINAL AVEC TEMPS
# print("\n" + "="*70)
# print("📊 RÉSUMÉ FINAL - MODE MULTI-NODE")
# print("="*70)
# print(f"📥 Documents traités    : {total}")
# print(f"🔗 URLs détectées       : {total_urls}")
# print(f"📝 Documents avec URLs  : {avec_urls_avant}")
# print(f"✅ URLs supprimées      : {supprimees}")
# print(f"📈 Taux de succès       : {taux:.2f}%")
# print(f"\n⏱️  TEMPS D'EXÉCUTION:")
# print(f"   • Chargement : {temps_fin_chargement - temps_debut_chargement:.2f}s")
# print(f"   • Nettoyage  : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s")
# print(f"   • Sauvegarde : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s")
# print(f"   • TOTAL      : {temps_total:.2f}s")
# print(f"   • Vitesse    : {total / temps_total:.2f} docs/s")
# print(f"\n📁 Collection MongoDB:")
# print(f"   • telecom_algerie.commentaires_sans_urls_multinode2")
# print("="*70)

# print("\n🎉 SUPPRESSION DES URLS TERMINÉE EN MODE MULTI-NODE !")

# # Fermer les connexions
# spark.stop()
# client.close()
# print("\n🔌 Connexions fermées")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/nettoyage/01_supprimer_urls_multinode.py - VERSION AVEC MESURE DE TEMPS

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, spark_partition_id
from pyspark.sql.types import StringType, IntegerType
import re
from pymongo import MongoClient
from datetime import datetime
import os
import socket
import pandas as pd
import time  # 👈 POUR MESURER LE TEMPS

def supprimer_urls(texte):
    """Supprime les URLs d'un texte - Version améliorée"""
    if texte is None or not isinstance(texte, str):
        return texte
    
    # Patterns améliorés pour détecter tous les types d'URLs
    patterns = [
        r'https?://\S+',           # URLs complètes
        r'www\.\S+',                # www.example.com
        r'https?://(?:\s|$)',       # https:// seul suivi d'espace ou fin
        r'https?://$',              # https:// en fin de chaîne
        r'\bhttps?://\b',           # https:// comme mot isolé
        r'http://(?:\s|$)',         # http:// seul
        r'http://$'                 # http:// en fin de chaîne
    ]
    
    texte_propre = texte
    for pattern in patterns:
        texte_propre = re.sub(pattern, '', texte_propre, flags=re.IGNORECASE)
    
    # Supprimer les espaces multiples
    texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
    return texte_propre if texte_propre else None

def supprimer_at(texte):
    """Supprime les caractères @ d'un texte"""
    if texte is None or not isinstance(texte, str):
        return texte
    
    # Supprimer tous les @
    texte_propre = re.sub(r'@', '', texte)
    
    # Supprimer les espaces multiples
    texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
    return texte_propre if texte_propre else None

def detecter_urls(texte):
    """Détecte si un texte contient des URLs"""
    if texte is None or not isinstance(texte, str):
        return 0
    
    patterns = [
        r'https?://',
        r'www\.',
        r'https?://(?:\s|$)',
        r'https?://$'
    ]
    
    for pattern in patterns:
        if re.search(pattern, texte, re.IGNORECASE):
            return 1
    return 0

def detecter_at(texte):
    """Détecte si un texte contient des @"""
    if texte is None or not isinstance(texte, str):
        return 0
    
    return 1 if re.search(r'@', texte) else 0

def compter_at(texte):
    """Compte le nombre de @ dans un texte"""
    if texte is None or not isinstance(texte, str):
        return 0
    
    return len(re.findall(r'@', texte))

def compter_urls(texte):
    """Compte le nombre d'URLs dans un texte"""
    if texte is None or not isinstance(texte, str):
        return 0
    
    pattern = r'https?://\S+|www\.\S+|https?://(?:\s|$)|https?://$'
    return len(re.findall(pattern, texte, re.IGNORECASE))

def get_worker_name():
    """Retourne le nom du worker"""
    return socket.gethostname()

# 📊 DÉBUT DU CHRONOMÈTRAGE GLOBAL
temps_debut_global = time.time()

print("="*70)
print("🔍 ÉTAPE 1 : SUPPRESSION DES URLS ET DES @ - MODE MULTI-NODE")
print("="*70)

# 1. CONNEXION À MONGODB
print("\n📂 Connexion à MongoDB...")
try:
    client = MongoClient('localhost', 27018)
    db = client['telecom_algerie']
    collection_source = db['commentaires_bruts']
    total_docs = collection_source.count_documents({})
    print(f"✅ Connexion MongoDB réussie")
    print(f"📊 Collection source: {total_docs} documents")
except Exception as e:
    print(f"❌ Erreur de connexion MongoDB: {e}")
    exit(1)

# 2. CONNEXION AU CLUSTER SPARK
print("\n⚡ Connexion au cluster Spark multi-node...")
temps_debut_spark = time.time()

spark = SparkSession.builder \
    .appName("Suppression_URLs_MultiNode") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "12") \
    .getOrCreate()

temps_fin_spark = time.time()
print(f"✅ Cluster Spark multi-node connecté en {temps_fin_spark - temps_debut_spark:.2f} secondes")

# 3. CHARGER LES DONNÉES AVEC PYMONGO
print("\n📥 Chargement des données avec PyMongo...")
temps_debut_chargement = time.time()

# Charger tous les documents
print("   Récupération des documents...")
data = list(collection_source.find({}))
print(f"   📊 {len(data)} documents chargés")

# Convertir les ObjectId en string
print("   🔄 Conversion des ObjectId...")
for doc in data:
    doc['_id'] = str(doc['_id'])

# Créer DataFrame Spark
print("   📊 Création du DataFrame Spark...")
df_spark = spark.createDataFrame(data)
total_lignes = df_spark.count()

temps_fin_chargement = time.time()
print(f"✅ {total_lignes} documents chargés dans Spark en {temps_fin_chargement - temps_debut_chargement:.2f} secondes")

# 4. IDENTIFIER LES WORKERS
print("\n🔍 RÉPARTITION SUR LES WORKERS:")

worker_udf = udf(get_worker_name, StringType())

df_with_workers = df_spark \
    .withColumn("partition_id", spark_partition_id()) \
    .withColumn("worker_name", worker_udf())

print("   Distribution des données:")
df_with_workers.groupBy("worker_name", "partition_id").count().show()

# 5. ENREGISTRER LES UDF
print("\n🔄 Enregistrement des fonctions...")
supprimer_urls_udf = udf(supprimer_urls, StringType())
supprimer_at_udf = udf(supprimer_at, StringType())
detecter_urls_udf = udf(detecter_urls, IntegerType())
detecter_at_udf = udf(detecter_at, IntegerType())
compter_urls_udf = udf(compter_urls, IntegerType())
compter_at_udf = udf(compter_at, IntegerType())

# 6. ANALYSE AVANT NETTOYAGE
print("\n🔎 ANALYSE : Recherche des URLs et des @...")
temps_debut_analyse = time.time()

df_analyse = df_with_workers \
    .withColumn("urls_avant", detecter_urls_udf(col("Commentaire_Client"))) \
    .withColumn("nb_urls_avant", compter_urls_udf(col("Commentaire_Client"))) \
    .withColumn("at_avant", detecter_at_udf(col("Commentaire_Client"))) \
    .withColumn("nb_at_avant", compter_at_udf(col("Commentaire_Client")))

total = df_analyse.count()
avec_urls_avant = df_analyse.filter(col("urls_avant") == 1).count()
total_urls = df_analyse.agg({"nb_urls_avant": "sum"}).collect()[0][0] or 0
avec_at_avant = df_analyse.filter(col("at_avant") == 1).count()
total_at = df_analyse.agg({"nb_at_avant": "sum"}).collect()[0][0] or 0

temps_fin_analyse = time.time()
print(f"\n📊 STATISTIQUES AVANT NETTOYAGE (analyse en {temps_fin_analyse - temps_debut_analyse:.2f}s):")
print(f"   ┌────────────────────────────────────┐")
print(f"   │ Total documents        : {total:<15} │")
print(f"   │ Documents avec URLs    : {avec_urls_avant:<15} │")
print(f"   │ URLs détectées         : {total_urls:<15} │")
print(f"   │ Documents avec @       : {avec_at_avant:<15} │")
print(f"   │ @ détectés             : {total_at:<15} │")
print(f"   │ Pourcentage URLs       : {(avec_urls_avant/total*100):<15.2f}% │")
print(f"   │ Pourcentage @          : {(avec_at_avant/total*100):<15.2f}% │")
print(f"   └────────────────────────────────────┘")

# 7. NETTOYAGE
print("\n🧹 SUPPRESSION DES URLS ET DES @ EN COURS...")
temps_debut_nettoyage = time.time()

# Appliquer d'abord la suppression des URLs, puis la suppression des @
df_nettoye = df_analyse \
    .withColumn("Commentaire_Client_sans_urls", supprimer_urls_udf(col("Commentaire_Client"))) \
    .withColumn("commentaire_moderateur_sans_urls", supprimer_urls_udf(col("commentaire_moderateur"))) \
    .withColumn("Commentaire_Client_propre", supprimer_at_udf(col("Commentaire_Client_sans_urls"))) \
    .withColumn("commentaire_moderateur_propre", supprimer_at_udf(col("commentaire_moderateur_sans_urls"))) \
    .withColumn("urls_apres", detecter_urls_udf(col("Commentaire_Client_propre"))) \
    .withColumn("at_apres", detecter_at_udf(col("Commentaire_Client_propre")))

# Forcer l'exécution des transformations
df_nettoye.cache().count()

temps_fin_nettoyage = time.time()
print(f"✅ Nettoyage terminé en {temps_fin_nettoyage - temps_debut_nettoyage:.2f} secondes")

# 8. STATISTIQUES APRÈS NETTOYAGE
avec_urls_apres = df_nettoye.filter(col("urls_apres") == 1).count()
avec_at_apres = df_nettoye.filter(col("at_apres") == 1).count()
supprimees_urls = avec_urls_avant - avec_urls_apres
supprimees_at = avec_at_avant - avec_at_apres
taux_urls = (supprimees_urls / avec_urls_avant * 100) if avec_urls_avant > 0 else 0
taux_at = (supprimees_at / avec_at_avant * 100) if avec_at_avant > 0 else 0

print(f"\n📊 STATISTIQUES APRÈS NETTOYAGE:")
print(f"   ┌────────────────────────────────────┐")
print(f"   │ URLs avant            : {avec_urls_avant:<15} │")
print(f"   │ URLs après            : {avec_urls_apres:<15} │")
print(f"   │ URLs supprimées       : {supprimees_urls:<15} │")
print(f"   │ Taux succès URLs      : {taux_urls:<15.2f}% │")
print(f"   │ @ avant               : {avec_at_avant:<15} │")
print(f"   │ @ après               : {avec_at_apres:<15} │")
print(f"   │ @ supprimés           : {supprimees_at:<15} │")
print(f"   │ Taux succès @         : {taux_at:<15.2f}% │")
print(f"   └────────────────────────────────────┘")

# 9. STATISTIQUES PAR WORKER
print("\n📊 PERFORMANCE PAR WORKER:")
worker_perf = df_nettoye.groupBy("worker_name").agg(
    {"urls_avant": "sum", 
     "urls_apres": "sum",
     "at_avant": "sum",
     "at_apres": "sum",
     "Commentaire_Client": "count"}
).withColumnRenamed("sum(urls_avant)", "urls_trouvees") \
 .withColumnRenamed("sum(urls_apres)", "urls_restantes") \
 .withColumnRenamed("sum(at_avant)", "at_trouves") \
 .withColumnRenamed("sum(at_apres)", "at_restants") \
 .withColumnRenamed("count(Commentaire_Client)", "documents")

worker_perf.show()

# 10. PRÉPARATION POUR MONGODB
print("\n💾 PRÉPARATION POUR SAUVEGARDE...")
temps_debut_preparation = time.time()

df_final = df_nettoye.select(
    "_id",
    col("Commentaire_Client_propre").alias("Commentaire_Client"),
    col("commentaire_moderateur_propre").alias("commentaire_moderateur"),
    "date",
    "source",
    "moderateur",
    "metadata",
    "statut"
)

temps_fin_preparation = time.time()
print(f"✅ Préparation terminée en {temps_fin_preparation - temps_debut_preparation:.2f} secondes")

# 11. SAUVEGARDE DANS MONGODB
print("\n📁 SAUVEGARDE DANS MONGODB...")
temps_debut_sauvegarde = time.time()

# Convertir en Pandas
print("   🔄 Conversion en Pandas...")
df_pandas = df_final.toPandas()
print(f"   ✅ {len(df_pandas)} lignes converties")

# Collection destination
collection_dest = db['commentaires_sans_urls_multinode2']
collection_dest.delete_many({})
print("   🧹 Collection destination vidée")

# Insérer par lots
print("   📥 Insertion par lots...")
batch_size = 500
total_batches = (len(df_pandas) + batch_size - 1) // batch_size

for i in range(0, len(df_pandas), batch_size):
    batch_num = i//batch_size + 1
    batch = df_pandas.iloc[i:i+batch_size].to_dict('records')
    
    # Convertir les NaN en None pour MongoDB
    for doc in batch:
        for key, value in doc.items():
            if pd.isna(value):
                doc[key] = None
    
    collection_dest.insert_many(batch)
    print(f"   ✓ Lot {batch_num}/{total_batches}: {len(batch)} documents")

temps_fin_sauvegarde = time.time()
print(f"\n✅ {len(df_pandas)} documents sauvegardés dans 'commentaires_sans_urls_multinode2'")
print(f"   ⏱️  Temps de sauvegarde: {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f} secondes")

# 12. VÉRIFICATION FINALE
print("\n🔎 VÉRIFICATION FINALE...")
temps_debut_verification = time.time()

# Vérifier avec différents patterns
patterns_verif_urls = [
    r'https?://\S+',
    r'www\.\S+',
    r'https?://(?:\s|$)',
    r'https?://$'
]

print("\n   Vérification URLs pattern par pattern:")
for pattern in patterns_verif_urls:
    count = collection_dest.count_documents({
        "Commentaire_Client": {"$regex": pattern, "$options": "i"}
    })
    print(f"   • {pattern[:20]}...: {count} documents")

# Vérification des @
count_at = collection_dest.count_documents({
    "Commentaire_Client": {"$regex": "@", "$options": "i"}
})
print(f"\n   • Vérification @: {count_at} documents avec @")

# Vérification globale
urls_restantes = collection_dest.count_documents({
    "Commentaire_Client": {"$regex": "https?://|www\.", "$options": "i"}
})

temps_fin_verification = time.time()
print(f"\n📊 RÉSULTAT FINAL (vérification en {temps_fin_verification - temps_debut_verification:.2f}s):")
print(f"   • Documents avec URLs restantes: {urls_restantes}")
print(f"   • Documents avec @ restants: {count_at}")
if urls_restantes == 0 and count_at == 0:
    print("   ✅ SUCCÈS : Toutes les URLs et tous les @ ont été supprimés !")
else:
    print(f"   ⚠️ ATTENTION : {urls_restantes} URLs restantes, {count_at} @ restants")

# 🏁 FIN DU CHRONOMÈTRAGE GLOBAL
temps_fin_global = time.time()
temps_total = temps_fin_global - temps_debut_global

# 13. RAPPORT AVEC TEMPS D'EXÉCUTION
print("\n📄 CRÉATION DU RAPPORT...")

rapport = f"""
{"="*70}
RAPPORT DE SUPPRESSION DES URLS ET DES @ - MODE MULTI-NODE
{"="*70}

Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode : Multi-node Spark Cluster

⏱️  TEMPS D'EXÉCUTION:
   • Connexion Spark        : {temps_fin_spark - temps_debut_spark:.2f}s
   • Chargement données     : {temps_fin_chargement - temps_debut_chargement:.2f}s
   • Analyse des données    : {temps_fin_analyse - temps_debut_analyse:.2f}s
   • Nettoyage              : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s
   • Préparation DataFrame  : {temps_fin_preparation - temps_debut_preparation:.2f}s
   • Sauvegarde MongoDB     : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s
   • Vérification           : {temps_fin_verification - temps_debut_verification:.2f}s
   • ──────────────────────────────────
   • TEMPS TOTAL            : {temps_total:.2f}s
   • Documents par seconde  : {total / temps_total:.2f} doc/s

📊 STATISTIQUES GLOBALES:
   • Total documents traités    : {total}
   
   📍 URLs:
   • Documents avec URLs (avant): {avec_urls_avant}
   • URLs détectées (avant)     : {total_urls}
   • Documents avec URLs (après): {avec_urls_apres}
   • URLs supprimées            : {supprimees_urls}
   • Taux de succès URLs        : {taux_urls:.2f}%
   
   📍 @ (arobase):
   • Documents avec @ (avant)   : {avec_at_avant}
   • @ détectés (avant)         : {total_at}
   • Documents avec @ (après)   : {avec_at_apres}
   • @ supprimés                : {supprimees_at}
   • Taux de succès @           : {taux_at:.2f}%

📁 STOCKAGE:
   • Collection source      : telecom_algerie.commentaires_bruts
   • Collection destination : telecom_algerie.commentaires_sans_urls_multinode2
   • Documents sauvegardés  : {len(df_pandas)}

🔍 VÉRIFICATION FINALE:
   • URLs restantes détectées : {urls_restantes}
   • @ restants détectés      : {count_at}
   • Statut : {"✅ SUCCÈS" if (urls_restantes == 0 and count_at == 0) else "⚠️ ÉCHEC"}

⚡ DISTRIBUTION:
   • Workers utilisés : {df_with_workers.select("worker_name").distinct().count()}
"""

# Sauvegarder le rapport
os.makedirs("donnees/resultats", exist_ok=True)
rapport_path = "donnees/resultats/rapport_urls_multinode2.txt"
with open(rapport_path, "w", encoding="utf-8") as f:
    f.write(rapport)
print(f"✅ Rapport sauvegardé: {rapport_path}")

# 14. RÉSUMÉ FINAL AVEC TEMPS
print("\n" + "="*70)
print("📊 RÉSUMÉ FINAL - MODE MULTI-NODE")
print("="*70)
print(f"📥 Documents traités    : {total}")
print(f"\n📍 URLs:")
print(f"   • Détectées : {total_urls}")
print(f"   • Supprimées: {supprimees_urls}")
print(f"   • Taux      : {taux_urls:.2f}%")
print(f"\n📍 @ (arobase):")
print(f"   • Détectés  : {total_at}")
print(f"   • Supprimés : {supprimees_at}")
print(f"   • Taux      : {taux_at:.2f}%")
print(f"\n⏱️  TEMPS D'EXÉCUTION:")
print(f"   • Chargement : {temps_fin_chargement - temps_debut_chargement:.2f}s")
print(f"   • Nettoyage  : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s")
print(f"   • Sauvegarde : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s")
print(f"   • TOTAL      : {temps_total:.2f}s")
print(f"   • Vitesse    : {total / temps_total:.2f} docs/s")
print(f"\n📁 Collection MongoDB:")
print(f"   • telecom_algerie.commentaires_sans_urls_multinode2")
print("="*70)

print("\n🎉 SUPPRESSION DES URLS ET DES @ TERMINÉE EN MODE MULTI-NODE !")

# Fermer les connexions
spark.stop()
client.close()
print("\n🔌 Connexions fermées")