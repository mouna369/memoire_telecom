

# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-

# # # scripts/nettoyage/01_supprimer_urls_multinode.py - VERSION AVEC MESURE DE TEMPS

# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, udf, spark_partition_id
# # from pyspark.sql.types import StringType, IntegerType
# # import re
# # from pymongo import MongoClient
# # from datetime import datetime
# # import os
# # import socket
# # import pandas as pd
# # import time  # 👈 POUR MESURER LE TEMPS

# # def supprimer_urls(texte):
# #     """Supprime les URLs d'un texte - Version améliorée"""
# #     if texte is None or not isinstance(texte, str):
# #         return texte
    
# #     # Patterns améliorés pour détecter tous les types d'URLs
# #     patterns = [
# #         r'https?://\S+',           # URLs complètes
# #         r'www\.\S+',                # www.example.com
# #         r'https?://(?:\s|$)',       # https:// seul suivi d'espace ou fin
# #         r'https?://$',              # https:// en fin de chaîne
# #         r'\bhttps?://\b',           # https:// comme mot isolé
# #         r'http://(?:\s|$)',         # http:// seul
# #         r'http://$'                 # http:// en fin de chaîne
# #     ]
    
# #     texte_propre = texte
# #     for pattern in patterns:
# #         texte_propre = re.sub(pattern, '', texte_propre, flags=re.IGNORECASE)
    
# #     # Supprimer les espaces multiples
# #     texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
# #     return texte_propre if texte_propre else None

# # def supprimer_at(texte):
# #     """Supprime les caractères @ d'un texte"""
# #     if texte is None or not isinstance(texte, str):
# #         return texte
    
# #     # Supprimer tous les @
# #     texte_propre = re.sub(r'@', '', texte)
    
# #     # Supprimer les espaces multiples
# #     texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
# #     return texte_propre if texte_propre else None

# # def detecter_urls(texte):
# #     """Détecte si un texte contient des URLs"""
# #     if texte is None or not isinstance(texte, str):
# #         return 0
    
# #     patterns = [
# #         r'https?://',
# #         r'www\.',
# #         r'https?://(?:\s|$)',
# #         r'https?://$'
# #     ]
    
# #     for pattern in patterns:
# #         if re.search(pattern, texte, re.IGNORECASE):
# #             return 1
# #     return 0

# # def detecter_at(texte):
# #     """Détecte si un texte contient des @"""
# #     if texte is None or not isinstance(texte, str):
# #         return 0
    
# #     return 1 if re.search(r'@', texte) else 0

# # def compter_at(texte):
# #     """Compte le nombre de @ dans un texte"""
# #     if texte is None or not isinstance(texte, str):
# #         return 0
    
# #     return len(re.findall(r'@', texte))

# # def compter_urls(texte):
# #     """Compte le nombre d'URLs dans un texte"""
# #     if texte is None or not isinstance(texte, str):
# #         return 0
    
# #     pattern = r'https?://\S+|www\.\S+|https?://(?:\s|$)|https?://$'
# #     return len(re.findall(pattern, texte, re.IGNORECASE))

# # def get_worker_name():
# #     """Retourne le nom du worker"""
# #     return socket.gethostname()

# # # 📊 DÉBUT DU CHRONOMÈTRAGE GLOBAL
# # temps_debut_global = time.time()

# # print("="*70)
# # print("🔍 ÉTAPE 1 : SUPPRESSION DES URLS ET DES @ - MODE MULTI-NODE")
# # print("="*70)

# # # 1. CONNEXION À MONGODB
# # print("\n📂 Connexion à MongoDB...")
# # try:
# #     client = MongoClient('localhost', 27018)
# #     db = client['telecom_algerie']
# #     collection_source = db['commentaires_bruts']
# #     total_docs = collection_source.count_documents({})
# #     print(f"✅ Connexion MongoDB réussie")
# #     print(f"📊 Collection source: {total_docs} documents")
# # except Exception as e:
# #     print(f"❌ Erreur de connexion MongoDB: {e}")
# #     exit(1)

# # # 2. CONNEXION AU CLUSTER SPARK
# # print("\n⚡ Connexion au cluster Spark multi-node...")
# # temps_debut_spark = time.time()

# # spark = SparkSession.builder \
# #     .appName("Suppression_URLs_MultiNode") \
# #     .master("spark://spark-master:7077") \
# #     .config("spark.executor.memory", "2g") \
# #     .config("spark.executor.cores", "12") \
# #     .getOrCreate()

# # temps_fin_spark = time.time()
# # print(f"✅ Cluster Spark multi-node connecté en {temps_fin_spark - temps_debut_spark:.2f} secondes")

# # # 3. CHARGER LES DONNÉES AVEC PYMONGO
# # print("\n📥 Chargement des données avec PyMongo...")
# # temps_debut_chargement = time.time()

# # # Charger tous les documents
# # print("   Récupération des documents...")
# # data = list(collection_source.find({}))
# # print(f"   📊 {len(data)} documents chargés")

# # # Convertir les ObjectId en string
# # print("   🔄 Conversion des ObjectId...")
# # for doc in data:
# #     doc['_id'] = str(doc['_id'])

# # # Créer DataFrame Spark
# # print("   📊 Création du DataFrame Spark...")
# # df_spark = spark.createDataFrame(data)
# # total_lignes = df_spark.count()

# # temps_fin_chargement = time.time()
# # print(f"✅ {total_lignes} documents chargés dans Spark en {temps_fin_chargement - temps_debut_chargement:.2f} secondes")

# # # 4. IDENTIFIER LES WORKERS
# # print("\n🔍 RÉPARTITION SUR LES WORKERS:")

# # worker_udf = udf(get_worker_name, StringType())

# # df_with_workers = df_spark \
# #     .withColumn("partition_id", spark_partition_id()) \
# #     .withColumn("worker_name", worker_udf())

# # print("   Distribution des données:")
# # df_with_workers.groupBy("worker_name", "partition_id").count().show()

# # # 5. ENREGISTRER LES UDF
# # print("\n🔄 Enregistrement des fonctions...")
# # supprimer_urls_udf = udf(supprimer_urls, StringType())
# # supprimer_at_udf = udf(supprimer_at, StringType())
# # detecter_urls_udf = udf(detecter_urls, IntegerType())
# # detecter_at_udf = udf(detecter_at, IntegerType())
# # compter_urls_udf = udf(compter_urls, IntegerType())
# # compter_at_udf = udf(compter_at, IntegerType())

# # # 6. ANALYSE AVANT NETTOYAGE
# # print("\n🔎 ANALYSE : Recherche des URLs et des @...")
# # temps_debut_analyse = time.time()

# # df_analyse = df_with_workers \
# #     .withColumn("urls_avant", detecter_urls_udf(col("Commentaire_Client"))) \
# #     .withColumn("nb_urls_avant", compter_urls_udf(col("Commentaire_Client"))) \
# #     .withColumn("at_avant", detecter_at_udf(col("Commentaire_Client"))) \
# #     .withColumn("nb_at_avant", compter_at_udf(col("Commentaire_Client")))

# # total = df_analyse.count()
# # avec_urls_avant = df_analyse.filter(col("urls_avant") == 1).count()
# # total_urls = df_analyse.agg({"nb_urls_avant": "sum"}).collect()[0][0] or 0
# # avec_at_avant = df_analyse.filter(col("at_avant") == 1).count()
# # total_at = df_analyse.agg({"nb_at_avant": "sum"}).collect()[0][0] or 0

# # temps_fin_analyse = time.time()
# # print(f"\n📊 STATISTIQUES AVANT NETTOYAGE (analyse en {temps_fin_analyse - temps_debut_analyse:.2f}s):")
# # print(f"   ┌────────────────────────────────────┐")
# # print(f"   │ Total documents        : {total:<15} │")
# # print(f"   │ Documents avec URLs    : {avec_urls_avant:<15} │")
# # print(f"   │ URLs détectées         : {total_urls:<15} │")
# # print(f"   │ Documents avec @       : {avec_at_avant:<15} │")
# # print(f"   │ @ détectés             : {total_at:<15} │")
# # print(f"   │ Pourcentage URLs       : {(avec_urls_avant/total*100):<15.2f}% │")
# # print(f"   │ Pourcentage @          : {(avec_at_avant/total*100):<15.2f}% │")
# # print(f"   └────────────────────────────────────┘")

# # # 7. NETTOYAGE
# # print("\n🧹 SUPPRESSION DES URLS ET DES @ EN COURS...")
# # temps_debut_nettoyage = time.time()

# # # Appliquer d'abord la suppression des URLs, puis la suppression des @
# # df_nettoye = df_analyse \
# #     .withColumn("Commentaire_Client_sans_urls", supprimer_urls_udf(col("Commentaire_Client"))) \
# #     .withColumn("commentaire_moderateur_sans_urls", supprimer_urls_udf(col("commentaire_moderateur"))) \
# #     .withColumn("Commentaire_Client_propre", supprimer_at_udf(col("Commentaire_Client_sans_urls"))) \
# #     .withColumn("commentaire_moderateur_propre", supprimer_at_udf(col("commentaire_moderateur_sans_urls"))) \
# #     .withColumn("urls_apres", detecter_urls_udf(col("Commentaire_Client_propre"))) \
# #     .withColumn("at_apres", detecter_at_udf(col("Commentaire_Client_propre")))

# # # Forcer l'exécution des transformations
# # df_nettoye.cache().count()

# # temps_fin_nettoyage = time.time()
# # print(f"✅ Nettoyage terminé en {temps_fin_nettoyage - temps_debut_nettoyage:.2f} secondes")

# # # 8. STATISTIQUES APRÈS NETTOYAGE
# # avec_urls_apres = df_nettoye.filter(col("urls_apres") == 1).count()
# # avec_at_apres = df_nettoye.filter(col("at_apres") == 1).count()
# # supprimees_urls = avec_urls_avant - avec_urls_apres
# # supprimees_at = avec_at_avant - avec_at_apres
# # taux_urls = (supprimees_urls / avec_urls_avant * 100) if avec_urls_avant > 0 else 0
# # taux_at = (supprimees_at / avec_at_avant * 100) if avec_at_avant > 0 else 0

# # print(f"\n📊 STATISTIQUES APRÈS NETTOYAGE:")
# # print(f"   ┌────────────────────────────────────┐")
# # print(f"   │ URLs avant            : {avec_urls_avant:<15} │")
# # print(f"   │ URLs après            : {avec_urls_apres:<15} │")
# # print(f"   │ URLs supprimées       : {supprimees_urls:<15} │")
# # print(f"   │ Taux succès URLs      : {taux_urls:<15.2f}% │")
# # print(f"   │ @ avant               : {avec_at_avant:<15} │")
# # print(f"   │ @ après               : {avec_at_apres:<15} │")
# # print(f"   │ @ supprimés           : {supprimees_at:<15} │")
# # print(f"   │ Taux succès @         : {taux_at:<15.2f}% │")
# # print(f"   └────────────────────────────────────┘")

# # # 9. STATISTIQUES PAR WORKER
# # print("\n📊 PERFORMANCE PAR WORKER:")
# # worker_perf = df_nettoye.groupBy("worker_name").agg(
# #     {"urls_avant": "sum", 
# #      "urls_apres": "sum",
# #      "at_avant": "sum",
# #      "at_apres": "sum",
# #      "Commentaire_Client": "count"}
# # ).withColumnRenamed("sum(urls_avant)", "urls_trouvees") \
# #  .withColumnRenamed("sum(urls_apres)", "urls_restantes") \
# #  .withColumnRenamed("sum(at_avant)", "at_trouves") \
# #  .withColumnRenamed("sum(at_apres)", "at_restants") \
# #  .withColumnRenamed("count(Commentaire_Client)", "documents")

# # worker_perf.show()

# # # 10. PRÉPARATION POUR MONGODB
# # print("\n💾 PRÉPARATION POUR SAUVEGARDE...")
# # temps_debut_preparation = time.time()

# # df_final = df_nettoye.select(
# #     "_id",
# #     col("Commentaire_Client_propre").alias("Commentaire_Client"),
# #     col("commentaire_moderateur_propre").alias("commentaire_moderateur"),
# #     "date",
# #     "source",
# #     "moderateur",
# #     "metadata",
# #     "statut"
# # )

# # temps_fin_preparation = time.time()
# # print(f"✅ Préparation terminée en {temps_fin_preparation - temps_debut_preparation:.2f} secondes")

# # # 11. SAUVEGARDE DANS MONGODB
# # print("\n📁 SAUVEGARDE DANS MONGODB...")
# # temps_debut_sauvegarde = time.time()

# # # Convertir en Pandas
# # print("   🔄 Conversion en Pandas...")
# # df_pandas = df_final.toPandas()
# # print(f"   ✅ {len(df_pandas)} lignes converties")

# # # Collection destination
# # collection_dest = db['commentaires_sans_urls_multinode2']
# # collection_dest.delete_many({})
# # print("   🧹 Collection destination vidée")

# # # Insérer par lots
# # print("   📥 Insertion par lots...")
# # batch_size = 500
# # total_batches = (len(df_pandas) + batch_size - 1) // batch_size

# # for i in range(0, len(df_pandas), batch_size):
# #     batch_num = i//batch_size + 1
# #     batch = df_pandas.iloc[i:i+batch_size].to_dict('records')
    
# #     # Convertir les NaN en None pour MongoDB
# #     for doc in batch:
# #         for key, value in doc.items():
# #             if pd.isna(value):
# #                 doc[key] = None
    
# #     collection_dest.insert_many(batch)
# #     print(f"   ✓ Lot {batch_num}/{total_batches}: {len(batch)} documents")

# # temps_fin_sauvegarde = time.time()
# # print(f"\n✅ {len(df_pandas)} documents sauvegardés dans 'commentaires_sans_urls_multinode2'")
# # print(f"   ⏱️  Temps de sauvegarde: {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f} secondes")

# # # 12. VÉRIFICATION FINALE
# # print("\n🔎 VÉRIFICATION FINALE...")
# # temps_debut_verification = time.time()

# # # Vérifier avec différents patterns
# # patterns_verif_urls = [
# #     r'https?://\S+',
# #     r'www\.\S+',
# #     r'https?://(?:\s|$)',
# #     r'https?://$'
# # ]

# # print("\n   Vérification URLs pattern par pattern:")
# # for pattern in patterns_verif_urls:
# #     count = collection_dest.count_documents({
# #         "Commentaire_Client": {"$regex": pattern, "$options": "i"}
# #     })
# #     print(f"   • {pattern[:20]}...: {count} documents")

# # # Vérification des @
# # count_at = collection_dest.count_documents({
# #     "Commentaire_Client": {"$regex": "@", "$options": "i"}
# # })
# # print(f"\n   • Vérification @: {count_at} documents avec @")

# # # Vérification globale
# # urls_restantes = collection_dest.count_documents({
# #     "Commentaire_Client": {"$regex": "https?://|www\.", "$options": "i"}
# # })

# # temps_fin_verification = time.time()
# # print(f"\n📊 RÉSULTAT FINAL (vérification en {temps_fin_verification - temps_debut_verification:.2f}s):")
# # print(f"   • Documents avec URLs restantes: {urls_restantes}")
# # print(f"   • Documents avec @ restants: {count_at}")
# # if urls_restantes == 0 and count_at == 0:
# #     print("   ✅ SUCCÈS : Toutes les URLs et tous les @ ont été supprimés !")
# # else:
# #     print(f"   ⚠️ ATTENTION : {urls_restantes} URLs restantes, {count_at} @ restants")

# # # 🏁 FIN DU CHRONOMÈTRAGE GLOBAL
# # temps_fin_global = time.time()
# # temps_total = temps_fin_global - temps_debut_global

# # # 13. RAPPORT AVEC TEMPS D'EXÉCUTION
# # print("\n📄 CRÉATION DU RAPPORT...")

# # rapport = f"""
# # {"="*70}
# # RAPPORT DE SUPPRESSION DES URLS ET DES @ - MODE MULTI-NODE
# # {"="*70}

# # Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# # Mode : Multi-node Spark Cluster

# # ⏱️  TEMPS D'EXÉCUTION:
# #    • Connexion Spark        : {temps_fin_spark - temps_debut_spark:.2f}s
# #    • Chargement données     : {temps_fin_chargement - temps_debut_chargement:.2f}s
# #    • Analyse des données    : {temps_fin_analyse - temps_debut_analyse:.2f}s
# #    • Nettoyage              : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s
# #    • Préparation DataFrame  : {temps_fin_preparation - temps_debut_preparation:.2f}s
# #    • Sauvegarde MongoDB     : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s
# #    • Vérification           : {temps_fin_verification - temps_debut_verification:.2f}s
# #    • ──────────────────────────────────
# #    • TEMPS TOTAL            : {temps_total:.2f}s
# #    • Documents par seconde  : {total / temps_total:.2f} doc/s

# # 📊 STATISTIQUES GLOBALES:
# #    • Total documents traités    : {total}
   
# #    📍 URLs:
# #    • Documents avec URLs (avant): {avec_urls_avant}
# #    • URLs détectées (avant)     : {total_urls}
# #    • Documents avec URLs (après): {avec_urls_apres}
# #    • URLs supprimées            : {supprimees_urls}
# #    • Taux de succès URLs        : {taux_urls:.2f}%
   
# #    📍 @ (arobase):
# #    • Documents avec @ (avant)   : {avec_at_avant}
# #    • @ détectés (avant)         : {total_at}
# #    • Documents avec @ (après)   : {avec_at_apres}
# #    • @ supprimés                : {supprimees_at}
# #    • Taux de succès @           : {taux_at:.2f}%

# # 📁 STOCKAGE:
# #    • Collection source      : telecom_algerie.commentaires_bruts
# #    • Collection destination : telecom_algerie.commentaires_sans_urls_multinode2
# #    • Documents sauvegardés  : {len(df_pandas)}

# # 🔍 VÉRIFICATION FINALE:
# #    • URLs restantes détectées : {urls_restantes}
# #    • @ restants détectés      : {count_at}
# #    • Statut : {"✅ SUCCÈS" if (urls_restantes == 0 and count_at == 0) else "⚠️ ÉCHEC"}

# # ⚡ DISTRIBUTION:
# #    • Workers utilisés : {df_with_workers.select("worker_name").distinct().count()}
# # """

# # # Sauvegarder le rapport
# # os.makedirs("donnees/resultats", exist_ok=True)
# # rapport_path = "donnees/resultats/rapport_urls_multinode2.txt"
# # with open(rapport_path, "w", encoding="utf-8") as f:
# #     f.write(rapport)
# # print(f"✅ Rapport sauvegardé: {rapport_path}")

# # # 14. RÉSUMÉ FINAL AVEC TEMPS
# # print("\n" + "="*70)
# # print("📊 RÉSUMÉ FINAL - MODE MULTI-NODE")
# # print("="*70)
# # print(f"📥 Documents traités    : {total}")
# # print(f"\n📍 URLs:")
# # print(f"   • Détectées : {total_urls}")
# # print(f"   • Supprimées: {supprimees_urls}")
# # print(f"   • Taux      : {taux_urls:.2f}%")
# # print(f"\n📍 @ (arobase):")
# # print(f"   • Détectés  : {total_at}")
# # print(f"   • Supprimés : {supprimees_at}")
# # print(f"   • Taux      : {taux_at:.2f}%")
# # print(f"\n⏱️  TEMPS D'EXÉCUTION:")
# # print(f"   • Chargement : {temps_fin_chargement - temps_debut_chargement:.2f}s")
# # print(f"   • Nettoyage  : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s")
# # print(f"   • Sauvegarde : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s")
# # print(f"   • TOTAL      : {temps_total:.2f}s")
# # print(f"   • Vitesse    : {total / temps_total:.2f} docs/s")
# # print(f"\n📁 Collection MongoDB:")
# # print(f"   • telecom_algerie.commentaires_sans_urls_multinode2")
# # print("="*70)

# # print("\n🎉 SUPPRESSION DES URLS ET DES @ TERMINÉE EN MODE MULTI-NODE !")

# # # Fermer les connexions
# # spark.stop()
# # client.close()
# # print("\n🔌 Connexions fermées")

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# # scripts/nettoyage/01_supprimer_urls_multinode_v2.py
# # VERSION VRAIMENT DISTRIBUÉE — mapPartitions
# # Spark 4.1.1 / Scala 2.13 / Java 21
# # Chaque Worker lit ET écrit MongoDB directement

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, regexp_replace, spark_partition_id,
#     trim, when
# )
# from pyspark.sql.functions import sum as spark_sum, count as spark_count
# from pymongo import MongoClient, InsertOne
# from pymongo.errors import BulkWriteError
# from datetime import datetime
# import os
# import time
# import math

# # ============================================================
# # CONFIGURATION CENTRALISÉE
# # ============================================================
# # ✅ Le Driver (WSL) accède à MongoDB via localhost:27018
# MONGO_URI_DRIVER = "mongodb://localhost:27018/"

# # ✅ Les Workers (Docker) accèdent à MongoDB via le nom du conteneur
# #    sur le réseau projet_telecom_spark_network
# MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"

# DB_NAME             = "telecom_algerie"
# COLLECTION_SOURCE   = "commentaires_bruts"
# COLLECTION_DEST     = "commentaires_sans_urls_arobase"
# BATCH_SIZE          = 1000
# SPARK_MASTER        = "spark://spark-master:7077"
# RAPPORT_PATH        = "Rapports/rapport_urls_arobase.txt"

# # Patterns Regex
# PATTERN_URLS        = r'https?://\S*|www\.\S+'
# PATTERN_AT          = r'@'
# PATTERN_ESPACES     = r'\s+'
# PATTERN_DETECTION   = r'https?://|www\.'

# # ============================================================
# # FONCTIONS DISTRIBUÉES (s'exécutent sur les Workers)
# # ============================================================

# def nettoyer_et_ecrire_partition(partition):
#     """
#     ✅ Cette fonction s'exécute sur chaque Worker indépendamment.
    
#     Chaque Worker :
#     1. Nettoie sa portion de données (regex)
#     2. Se connecte directement à MongoDB
#     3. Écrit ses données nettoyées dans MongoDB
#     Sans jamais passer par le Driver !
#     """
#     import re
#     from pymongo import MongoClient, InsertOne
#     from pymongo.errors import BulkWriteError

#     # Patterns compilés une seule fois par partition (performance)
#     re_urls    = re.compile(r'https?://\S+|www\.\S+', re.IGNORECASE)
#     re_at      = re.compile(r'@')
#     re_espaces = re.compile(r'\s+')

#     def nettoyer_texte(texte):
#         if not texte or not isinstance(texte, str):
#             return texte
#         texte = re_urls.sub('', texte)
#         texte = re_at.sub('', texte)
#         texte = re_espaces.sub(' ', texte).strip()
#         return texte if texte else None

#     # Connexion MongoDB depuis le Worker
#     # ✅ Utilise mongodb_pfe:27017 (réseau Docker interne)
#     try:
#         client = MongoClient(
#             "mongodb://mongodb_pfe:27017/",
#             serverSelectionTimeoutMS=5000
#         )
#         db = client["telecom_algerie"]
#         collection = db["commentaires_sans_urls_arobase"]
#     except Exception as e:
#         # Si connexion échoue, on yield les erreurs pour le Driver
#         yield {"_erreur": str(e), "partition": "connexion_failed"}
#         return

#     batch = []
#     docs_traites = 0

#     for row in partition:
#         # Nettoyage des champs texte
#         commentaire_propre = nettoyer_texte(row.get("Commentaire_Client"))
#         moderateur_propre  = nettoyer_texte(row.get("commentaire_moderateur"))

#         doc = {
#             "_id"                    : row.get("_id"),
#             "Commentaire_Client"     : commentaire_propre,
#             "commentaire_moderateur" : moderateur_propre,
#             "date"                   : row.get("date"),
#             "source"                 : row.get("source"),
#             "moderateur"             : row.get("moderateur"),
#             "metadata"               : row.get("metadata"),
#             "statut"                 : row.get("statut"),
#         }

#         # Nettoyer les None/NaN
#         doc_propre = {k: (None if v != v else v) for k, v in doc.items()}
#         batch.append(InsertOne(doc_propre))
#         docs_traites += 1

#         # Écriture par lots depuis le Worker
#         if len(batch) >= 1000:
#             try:
#                 collection.bulk_write(batch, ordered=False)
#             except BulkWriteError:
#                 pass  # Ignorer les doublons éventuels
#             batch = []

#     # Écrire le dernier lot
#     if batch:
#         try:
#             collection.bulk_write(batch, ordered=False)
#         except BulkWriteError:
#             pass

#     client.close()

#     # ✅ Retourner les stats de cette partition au Driver
#     yield {"docs_traites": docs_traites, "statut": "ok"}


# def lire_partition_depuis_mongo(partition_info):
#     """
#     ✅ Chaque Worker lit SA portion depuis MongoDB directement.
    
#     partition_info contient : (skip, limit) — la plage de documents
#     que ce Worker doit traiter.
#     """
#     from pymongo import MongoClient

#     for item in partition_info:
#         skip  = item["skip"]
#         limit = item["limit"]

#         client = MongoClient(
#             "mongodb://mongodb_pfe:27017/",
#             serverSelectionTimeoutMS=5000
#         )
#         db = client["telecom_algerie"]
#         collection = db["commentaires_bruts"]

#         curseur = collection.find(
#             {},
#             {"_id": 1, "Commentaire_Client": 1,
#              "commentaire_moderateur": 1, "date": 1,
#              "source": 1, "moderateur": 1,
#              "metadata": 1, "statut": 1}
#         ).skip(skip).limit(limit)

#         for doc in curseur:
#             doc["_id"] = str(doc["_id"])
#             yield doc

#         client.close()


# # ============================================================
# # DÉBUT DU PIPELINE
# # ============================================================
# temps_debut_global = time.time()

# print("="*70)
# print("🔍 SUPPRESSION DES URLS ET @ — VERSION VRAIMENT DISTRIBUÉE")
# print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# print("="*70)

# # ============================================================
# # 1. CONNEXION MONGODB DRIVER (pour préparer le job)
# # ============================================================
# print("\n📂 Connexion MongoDB (Driver)...")
# try:
#     client_driver = MongoClient(MONGO_URI_DRIVER)
#     db_driver     = client_driver[DB_NAME]
#     coll_source   = db_driver[COLLECTION_SOURCE]
#     total_docs    = coll_source.count_documents({})
#     print(f"✅ MongoDB connecté — {total_docs} documents dans la source")
# except Exception as e:
#     print(f"❌ Erreur MongoDB : {e}")
#     exit(1)

# # ============================================================
# # 2. CONNEXION SPARK
# # ============================================================
# print("\n⚡ Connexion au cluster Spark...")
# temps_debut_spark = time.time()

# spark = SparkSession.builder \
#     .appName("Suppression_URLs_MapPartitions") \
#     .master(SPARK_MASTER) \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.sql.shuffle.partitions", "4") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")
# temps_fin_spark = time.time()
# print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")

# # ============================================================
# # 3. DISTRIBUER LA LECTURE ENTRE LES WORKERS
# #    Le Driver crée juste un RDD de "plages" (skip/limit)
# #    Chaque Worker lit sa plage directement depuis MongoDB
# # ============================================================
# print("\n📥 LECTURE DISTRIBUÉE — Chaque Worker lit sa portion...")
# temps_debut_chargement = time.time()

# NB_WORKERS   = 3  # Adapte selon ton nombre de workers
# docs_par_worker = math.ceil(total_docs / NB_WORKERS)

# # Créer les plages pour chaque Worker
# plages = [
#     {"skip": i * docs_par_worker,
#      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
#     for i in range(NB_WORKERS)
# ]

# print(f"   Distribution :")
# for idx, p in enumerate(plages):
#     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']} docs")

# # ✅ Chaque Worker lit sa portion depuis MongoDB via mapPartitions
# rdd_data = spark.sparkContext \
#     .parallelize(plages, NB_WORKERS) \
#     .mapPartitions(lire_partition_depuis_mongo)

# # Convertir en DataFrame Spark
# df_spark = spark.read.json(rdd_data.map(
#     lambda d: __import__('json').dumps(
#         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
#          for k, v in d.items()}
#     )
# ))

# total_lignes = df_spark.count()
# temps_fin_chargement = time.time()

# print(f"✅ {total_lignes} documents chargés en {temps_fin_chargement - temps_debut_chargement:.2f}s")
# print(f"   Partitions Spark : {df_spark.rdd.getNumPartitions()}")

# # ============================================================
# # 4. ANALYSE AVANT NETTOYAGE (fonctions natives Spark)
# # ============================================================
# print("\n🔎 ANALYSE AVANT NETTOYAGE...")
# temps_debut_analyse = time.time()

# df_analyse = df_spark \
#     .withColumn("a_url",
#         when(col("Commentaire_Client").rlike(PATTERN_DETECTION), 1).otherwise(0)) \
#     .withColumn("a_at",
#         when(col("Commentaire_Client").rlike(PATTERN_AT), 1).otherwise(0))

# stats_avant = df_analyse.agg(
#     spark_count("*").alias("total"),
#     spark_sum("a_url").alias("avec_urls"),
#     spark_sum("a_at").alias("avec_at")
# ).collect()[0]

# total        = stats_avant["total"]
# avec_urls_av = int(stats_avant["avec_urls"] or 0)
# avec_at_av   = int(stats_avant["avec_at"] or 0)

# temps_fin_analyse = time.time()
# print(f"\n📊 AVANT NETTOYAGE (en {temps_fin_analyse - temps_debut_analyse:.2f}s):")
# print(f"   ┌────────────────────────────────────┐")
# print(f"   │ Total documents     : {total:<15} │")
# print(f"   │ Avec URLs           : {avec_urls_av:<15} │")
# print(f"   │ Avec @              : {avec_at_av:<15} │")
# print(f"   │ % URLs              : {(avec_urls_av/total*100):<14.2f}% │")
# print(f"   │ % @                 : {(avec_at_av/total*100):<14.2f}% │")
# print(f"   └────────────────────────────────────┘")

# # ============================================================
# # 5. NETTOYAGE (fonctions natives Spark sur les Workers)
# # ============================================================
# print("\n🧹 NETTOYAGE SUR LES WORKERS...")
# temps_debut_nettoyage = time.time()

# df_nettoye = df_spark \
#     .withColumn("Commentaire_Client",
#         trim(regexp_replace(
#             regexp_replace(col("Commentaire_Client"), PATTERN_URLS, ""),
#             PATTERN_AT, ""))
#     ) \
#     .withColumn("Commentaire_Client",
#         regexp_replace(col("Commentaire_Client"), PATTERN_ESPACES, " ")) \
#     .withColumn("commentaire_moderateur",
#         trim(regexp_replace(
#             regexp_replace(col("commentaire_moderateur"), PATTERN_URLS, ""),
#             PATTERN_AT, ""))
#     ) \
#     .withColumn("commentaire_moderateur",
#         regexp_replace(col("commentaire_moderateur"), PATTERN_ESPACES, " "))

# df_nettoye.cache()

# # Stats après nettoyage
# df_apres = df_nettoye \
#     .withColumn("urls_ap", when(col("Commentaire_Client").rlike(PATTERN_DETECTION), 1).otherwise(0)) \
#     .withColumn("at_ap",   when(col("Commentaire_Client").rlike(PATTERN_AT), 1).otherwise(0))

# stats_apres = df_apres.agg(
#     spark_sum("urls_ap").alias("avec_urls"),
#     spark_sum("at_ap").alias("avec_at")
# ).collect()[0]

# avec_urls_ap = int(stats_apres["avec_urls"] or 0)
# avec_at_ap   = int(stats_apres["avec_at"] or 0)

# temps_fin_nettoyage = time.time()

# taux_urls = ((avec_urls_av - avec_urls_ap) / avec_urls_av * 100) if avec_urls_av > 0 else 100.0
# taux_at   = ((avec_at_av - avec_at_ap) / avec_at_av * 100) if avec_at_av > 0 else 100.0

# print(f"✅ Nettoyage terminé en {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s")
# print(f"\n📊 APRÈS NETTOYAGE:")
# print(f"   ┌────────────────────────────────────┐")
# print(f"   │ URLs supprimées     : {avec_urls_av - avec_urls_ap:<15} │")
# print(f"   │ Taux URLs           : {taux_urls:<14.2f}% │")
# print(f"   │ @ supprimés         : {avec_at_av - avec_at_ap:<15} │")
# print(f"   │ Taux @              : {taux_at:<14.2f}% │")
# print(f"   └────────────────────────────────────┘")

# # ============================================================
# # 6. ÉCRITURE DISTRIBUÉE DANS MONGODB
# #    ✅ Chaque Worker écrit directement sa partition
# #    Le Driver ne touche PAS les données
# # ============================================================
# print("\n💾 ÉCRITURE DISTRIBUÉE — Chaque Worker écrit dans MongoDB...")
# temps_debut_sauvegarde = time.time()

# # Vider la collection destination depuis le Driver
# coll_dest = db_driver[COLLECTION_DEST]
# coll_dest.delete_many({})
# print("   🧹 Collection destination vidée")

# # Sélectionner les colonnes finales
# df_final = df_nettoye.select(
#     "_id", "Commentaire_Client", "commentaire_moderateur",
#     "date", "source", "moderateur", "metadata", "statut"
# )

# # ✅ mapPartitions : chaque Worker écrit sa portion directement
# print("   📤 Workers en train d'écrire dans MongoDB...")
# rdd_stats = df_final.rdd \
#     .map(lambda row: row.asDict()) \
#     .mapPartitions(nettoyer_et_ecrire_partition)

# # Collecter juste les stats (petits objets, pas les données)
# stats_ecriture = rdd_stats.collect()

# total_inseres = sum(
#     s.get("docs_traites", 0)
#     for s in stats_ecriture
#     if s.get("statut") == "ok"
# )
# erreurs = [s for s in stats_ecriture if "_erreur" in s]

# temps_fin_sauvegarde = time.time()

# print(f"✅ {total_inseres} documents écrits en {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s")

# if erreurs:
#     print(f"   ⚠️  {len(erreurs)} erreur(s) détectée(s):")
#     for e in erreurs:
#         print(f"      • {e.get('_erreur')}")

# # ============================================================
# # 7. VÉRIFICATION FINALE (depuis le Driver)
# # ============================================================
# print("\n🔎 VÉRIFICATION FINALE...")
# temps_debut_verif = time.time()

# urls_restantes = coll_dest.count_documents({
#     "Commentaire_Client": {"$regex": PATTERN_DETECTION, "$options": "i"}
# })
# at_restants = coll_dest.count_documents({
#     "Commentaire_Client": {"$regex": "@"}
# })
# total_en_dest = coll_dest.count_documents({})

# temps_fin_verif = time.time()

# print(f"   • Documents en destination : {total_en_dest}")
# print(f"   • URLs restantes           : {urls_restantes}")
# print(f"   • @ restants               : {at_restants}")

# if urls_restantes == 0 and at_restants == 0:
#     print("   ✅ SUCCÈS TOTAL : Toutes les URLs et @ supprimés !")
# else:
#     print(f"   ⚠️  ATTENTION : {urls_restantes} URLs et {at_restants} @ restants")

# import re
# texte = "عندي اشتراك 1،5 جيغا https://"
# resultat = re.sub(r'https?://\S*|www\.\S+', '', texte).strip()
# print(resultat)
# # → "عندي اشتراك 1،5 جيغا"  ✅


# # ============================================================
# # 8. RAPPORT FINAL
# # ============================================================
# temps_fin_global = time.time()
# temps_total = temps_fin_global - temps_debut_global

# rapport = f"""
# {"="*70}
# RAPPORT — SUPPRESSION URLS & @ — VERSION DISTRIBUÉE (mapPartitions)
# {"="*70}
# Date             : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Mode             : Spark 4.1.1 | mapPartitions | Workers → MongoDB direct
# Réseau Docker    : projet_telecom_spark_network
# URI Workers      : {MONGO_URI_WORKERS}

# ⏱️  TEMPS D'EXÉCUTION:
#    • Connexion Spark     : {temps_fin_spark - temps_debut_spark:.2f}s
#    • Chargement données  : {temps_fin_chargement - temps_debut_chargement:.2f}s
#    • Analyse             : {temps_fin_analyse - temps_debut_analyse:.2f}s
#    • Nettoyage           : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s
#    • Écriture MongoDB    : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s
#    • Vérification        : {temps_fin_verif - temps_debut_verif:.2f}s
#    • ─────────────────────────────────
#    • TEMPS TOTAL         : {temps_total:.2f}s
#    • Vitesse             : {total / temps_total:.2f} doc/s

# 📊 RÉSULTATS:
#    • Total documents        : {total}
#    • URLs supprimées        : {avec_urls_av - avec_urls_ap} / {avec_urls_av} ({taux_urls:.2f}%)
#    • @ supprimés            : {avec_at_av - avec_at_ap} / {avec_at_av} ({taux_at:.2f}%)
#    • URLs restantes         : {urls_restantes}
#    • @ restants             : {at_restants}

# 📁 STOCKAGE:
#    • Source      : {DB_NAME}.{COLLECTION_SOURCE}
#    • Destination : {DB_NAME}.{COLLECTION_DEST}
#    • Insérés     : {total_inseres}
#    • Statut      : {"✅ SUCCÈS" if (urls_restantes == 0 and at_restants == 0) else "⚠️ INCOMPLET"}

# 🏗️  ARCHITECTURE:
#    MongoDB ──► Driver (prépare les plages skip/limit)
#                   ↙                    ↘
#              Worker 1              Worker 2
#           (lit + nettoie)       (lit + nettoie)
#                   ↓                    ↓
#              MongoDB               MongoDB
#            (écrit direct)        (écrit direct)
# """

# os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
#     f.write(rapport)
# print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

# # ============================================================
# # RÉSUMÉ CONSOLE
# # ============================================================
# print("\n" + "="*70)
# print("📊 RÉSUMÉ FINAL")
# print("="*70)
# print(f"📥 Documents traités  : {total}")
# print(f"📤 Documents insérés  : {total_inseres}")
# print(f"🗑️  URLs supprimées    : {avec_urls_av - avec_urls_ap} ({taux_urls:.1f}%)")
# print(f"🗑️  @ supprimés        : {avec_at_av - avec_at_ap} ({taux_at:.1f}%)")
# print(f"⏱️  Temps total        : {temps_total:.2f}s")
# print(f"🚀 Vitesse            : {total/temps_total:.0f} docs/s")
# print(f"📁 Collection dest.   : {DB_NAME}.{COLLECTION_DEST}")
# print("="*70)
# print("🎉 PIPELINE DISTRIBUÉ TERMINÉ !")

# # ============================================================
# # FERMETURE PROPRE
# # ============================================================
# df_nettoye.unpersist()
# spark.stop()
# client_driver.close()
# print("🔌 Connexions fermées proprement")


###code djdid ###

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/nettoyage/02_supprimer_hashtags_multinode.py
# ÉTAPE 2 — Extraction + Suppression des Hashtags
# Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, trim, when
)
from pyspark.sql.functions import sum as spark_sum, count as spark_count
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime
import os
import time
import math

# ============================================================
# CONFIGURATION CENTRALISÉE
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
DB_NAME           = "telecom_algerie"

# ✅ Source = résultat de l'étape 1
COLLECTION_SOURCE = "commentaires_sans_urls_arobase"

# ✅ Destination = nouvelle collection étape 2
COLLECTION_DEST   = "commentaires_sans_hashtags"

BATCH_SIZE        = 1000
NB_WORKERS        = 2
SPARK_MASTER      = "spark://spark-master:7077"
RAPPORT_PATH      = "Rapports/rapport_hashtags.txt"

# Patterns Regex
PATTERN_HASHTAG         = r'#\w+'          # Détecte #Djezzy #قطع_الانترنت
PATTERN_HASHTAG_COMPLET = r'#[\w\u0600-\u06FF\u0750-\u077F]+'  # Arabe + Latin
PATTERN_ESPACES         = r'\s+'

# ============================================================
# OPÉRATEURS ET SUJETS CONNUS (pour enrichissement)
# ============================================================
OPERATEURS = ["djezzy", "ooredoo", "mobilis", "atm", "algerie telecom"]

SUJETS_TELECOM = {
    # Réseau
    "قطع_الانترنت": "reseau", "شبكة": "reseau", "نت": "reseau",
    "4g": "reseau", "3g": "reseau", "wifi": "reseau",
    # Facturation
    "فاتورة": "facturation", "رصيد": "facturation", "recharge": "facturation",
    # Service client
    "خدمة_عملاء": "service_client", "support": "service_client",
    # Qualité
    "جودة": "qualite", "سرعة": "qualite", "ضعف": "qualite",
}

# ============================================================
# FONCTIONS DISTRIBUÉES
# ============================================================

def extraire_et_ecrire_partition(partition):
    """
    Chaque Worker :
    1. Extrait les hashtags dans une liste
    2. Identifie l'opérateur et le sujet
    3. Supprime les hashtags du texte
    4. Écrit directement dans MongoDB
    """
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    import re
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError

    # Patterns compilés une seule fois par partition
    re_hashtag = re.compile(r'#([\w\u0600-\u06FF\u0750-\u077F]+)', re.IGNORECASE)
    re_espaces = re.compile(r'\s+')

    def extraire_hashtags(texte):
        """Extrait la liste des hashtags sans le #"""
        if not texte or not isinstance(texte, str):
            return []
        return re_hashtag.findall(texte)

    def classifier_hashtags(hashtags):
        """
        Retourne :
        - operateur détecté
        - sujet détecté
        - sentiment_hashtag (si hashtag négatif connu)
        """
        operateur  = None
        sujet      = None

        for tag in hashtags:
            tag_lower = tag.lower()

            # Détecter l'opérateur
            for op in OPERATEURS:
                if op in tag_lower:
                    operateur = op
                    break

            # Détecter le sujet
            for mot, categorie in SUJETS_TELECOM.items():
                if mot.lower() in tag_lower:
                    sujet = categorie
                    break

        return operateur, sujet

    def supprimer_hashtags(texte):
        """Supprime les hashtags du texte"""
        if not texte or not isinstance(texte, str):
            return texte
        texte = re_hashtag.sub('', texte)
        texte = re_espaces.sub(' ', texte).strip()
        return texte if texte else None

    # Connexion MongoDB depuis le Worker
    try:
        client = MongoClient(
            "mongodb://mongodb_pfe:27017/",
            serverSelectionTimeoutMS=5000
        )
        db         = client["telecom_algerie"]
        collection = db["commentaires_sans_hashtags"]
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed"}
        return

    batch        = []
    docs_traites = 0
    total_hashtags_extraits = 0

    for row in partition:

        commentaire_original = row.get("Commentaire_Client", "")

        # 1. Extraire les hashtags
        hashtags = extraire_hashtags(commentaire_original)
        total_hashtags_extraits += len(hashtags)

        # 2. Classifier les hashtags
        operateur, sujet = classifier_hashtags(hashtags)

        # 3. Supprimer les hashtags du texte
        commentaire_propre = supprimer_hashtags(commentaire_original)
        moderateur_propre  = supprimer_hashtags(row.get("commentaire_moderateur", ""))

        # 4. Construire le document enrichi
        doc = {
            "_id"                    : row.get("_id"),
            "Commentaire_Client"     : commentaire_propre,
            "commentaire_moderateur" : moderateur_propre,
            "date"                   : row.get("date"),
            "source"                 : row.get("source"),
            "moderateur"             : row.get("moderateur"),
            "metadata"               : row.get("metadata"),
            "statut"                 : row.get("statut"),

            # ✅ NOUVELLES COLONNES
            "hashtags_detectes"      : hashtags,           # ["Djezzy", "قطع_الانترنت"]
            "nb_hashtags"            : len(hashtags),      # 2
            "operateur_cite"         : operateur,          # "djezzy"
            "sujet_hashtag"          : sujet,              # "reseau"
            "a_hashtags"             : len(hashtags) > 0,  # True/False
        }

        # Nettoyer les None/NaN
        doc_propre = {k: (None if (v != v) else v) for k, v in doc.items()}
        batch.append(InsertOne(doc_propre))
        docs_traites += 1

        # Écriture par lots
        if len(batch) >= 1000:
            try:
                collection.bulk_write(batch, ordered=False)
            except BulkWriteError:
                pass
            batch = []

    # Dernier lot
    if batch:
        try:
            collection.bulk_write(batch, ordered=False)
        except BulkWriteError:
            pass

    client.close()
    yield {
        "docs_traites"           : docs_traites,
        "total_hashtags_extraits": total_hashtags_extraits,
        "statut"                 : "ok"
    }


def lire_partition_depuis_mongo(partition_info):
    """Chaque Worker lit sa portion depuis MongoDB"""
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        skip  = item["skip"]
        limit = item["limit"]

        client = MongoClient(
            "mongodb://mongodb_pfe:27017/",
            serverSelectionTimeoutMS=5000
        )
        db         = client["telecom_algerie"]
        collection = db["commentaires_sans_urls_arobase"]

        curseur = collection.find(
            {},
            {"_id": 1, "Commentaire_Client": 1,
             "commentaire_moderateur": 1, "date": 1,
             "source": 1, "moderateur": 1,
             "metadata": 1, "statut": 1}
        ).skip(skip).limit(limit)

        for doc in curseur:
            doc["_id"] = str(doc["_id"])
            yield doc

        client.close()


# ============================================================
# DÉBUT DU PIPELINE
# ============================================================
temps_debut_global = time.time()

print("="*70)
print("🔍 ÉTAPE 2 — EXTRACTION + SUPPRESSION DES HASHTAGS")
print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
print("="*70)

# ============================================================
# 1. CONNEXION MONGODB DRIVER
# ============================================================
print("\n📂 Connexion MongoDB (Driver)...")
try:
    client_driver = MongoClient(MONGO_URI_DRIVER)
    db_driver     = client_driver[DB_NAME]
    coll_source   = db_driver[COLLECTION_SOURCE]
    total_docs    = coll_source.count_documents({})
    print(f"✅ MongoDB connecté — {total_docs} documents dans la source")
    print(f"   Source : {DB_NAME}.{COLLECTION_SOURCE}")
except Exception as e:
    print(f"❌ Erreur MongoDB : {e}")
    exit(1)

# ============================================================
# 2. CONNEXION SPARK
# ============================================================
print("\n⚡ Connexion au cluster Spark...")
temps_debut_spark = time.time()

spark = SparkSession.builder \
    .appName("Extraction_Hashtags_MultiNode") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
temps_fin_spark = time.time()
print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")

# ============================================================
# 3. LECTURE DISTRIBUÉE
# ============================================================
print("\n📥 LECTURE DISTRIBUÉE...")
temps_debut_chargement = time.time()

docs_par_worker = math.ceil(total_docs / NB_WORKERS)
plages = [
    {"skip" : i * docs_par_worker,
     "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
    for i in range(NB_WORKERS)
]

print(f"   Distribution :")
for idx, p in enumerate(plages):
    print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']} docs")

rdd_data = spark.sparkContext \
    .parallelize(plages, NB_WORKERS) \
    .mapPartitions(lire_partition_depuis_mongo)

df_spark = spark.read.json(rdd_data.map(
    lambda d: __import__('json').dumps(
        {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
         for k, v in d.items()}
    )
))

total_lignes = df_spark.count()
temps_fin_chargement = time.time()
print(f"✅ {total_lignes} documents chargés en {temps_fin_chargement - temps_debut_chargement:.2f}s")

# ============================================================
# 4. ANALYSE AVANT — Compter les hashtags
# ============================================================
print("\n🔎 ANALYSE AVANT...")
temps_debut_analyse = time.time()

df_analyse = df_spark.withColumn(
    "a_hashtag",
    when(col("Commentaire_Client").rlike(PATTERN_HASHTAG), 1).otherwise(0)
)

stats_avant = df_analyse.agg(
    spark_count("*").alias("total"),
    spark_sum("a_hashtag").alias("avec_hashtags")
).collect()[0]

total          = stats_avant["total"]
avec_hashtags_av = int(stats_avant["avec_hashtags"] or 0)

temps_fin_analyse = time.time()
print(f"\n📊 AVANT (en {temps_fin_analyse - temps_debut_analyse:.2f}s):")
print(f"   ┌────────────────────────────────────┐")
print(f"   │ Total documents     : {total:<15} │")
print(f"   │ Avec hashtags       : {avec_hashtags_av:<15} │")
print(f"   │ % avec hashtags     : {(avec_hashtags_av/total*100):<14.2f}% │")
print(f"   └────────────────────────────────────┘")

# ============================================================
# 5. EXTRACTION + ÉCRITURE DISTRIBUÉE PAR LES WORKERS
# ============================================================
print("\n🏷️  EXTRACTION DES HASHTAGS SUR LES WORKERS...")
temps_debut_traitement = time.time()

# Vider la collection destination
coll_dest = db_driver[COLLECTION_DEST]
coll_dest.delete_many({})
print("   🧹 Collection destination vidée")

# Chaque Worker extrait + écrit directement
print("   📤 Workers en train d'extraire et écrire...")
rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(extraire_et_ecrire_partition)

stats_ecriture = rdd_stats.collect()

total_inseres           = sum(s.get("docs_traites", 0) for s in stats_ecriture if s.get("statut") == "ok")
total_hashtags_extraits = sum(s.get("total_hashtags_extraits", 0) for s in stats_ecriture if s.get("statut") == "ok")
erreurs                 = [s for s in stats_ecriture if "_erreur" in s]

temps_fin_traitement = time.time()
print(f"✅ Traitement terminé en {temps_fin_traitement - temps_debut_traitement:.2f}s")

# ============================================================
# 6. VÉRIFICATION FINALE
# ============================================================
print("\n🔎 VÉRIFICATION FINALE...")

# Compter les hashtags restants
hashtags_restants = coll_dest.count_documents({
    "Commentaire_Client": {"$regex": "#\\w+"}
})

# Compter les docs avec hashtags extraits
docs_avec_hashtags = coll_dest.count_documents({
    "a_hashtags": True
})

# Top hashtags extraits
pipeline_top = [
    {"$match": {"hashtags_detectes": {"$ne": []}}},
    {"$unwind": "$hashtags_detectes"},
    {"$group": {"_id": "$hashtags_detectes", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
]
top_hashtags = list(coll_dest.aggregate(pipeline_top))

# Stats par opérateur
pipeline_op = [
    {"$match": {"operateur_cite": {"$ne": None}}},
    {"$group": {"_id": "$operateur_cite", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
stats_operateurs = list(coll_dest.aggregate(pipeline_op))

# Stats par sujet
pipeline_sujet = [
    {"$match": {"sujet_hashtag": {"$ne": None}}},
    {"$group": {"_id": "$sujet_hashtag", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
stats_sujets = list(coll_dest.aggregate(pipeline_sujet))

total_en_dest = coll_dest.count_documents({})

print(f"\n📊 RÉSULTATS:")
print(f"   ┌────────────────────────────────────┐")
print(f"   │ Documents insérés   : {total_inseres:<15} │")
print(f"   │ Hashtags extraits   : {total_hashtags_extraits:<15} │")
print(f"   │ Docs avec hashtags  : {docs_avec_hashtags:<15} │")
print(f"   │ Hashtags restants   : {hashtags_restants:<15} │")
print(f"   └────────────────────────────────────┘")

if top_hashtags:
    print(f"\n🏆 TOP 10 HASHTAGS EXTRAITS:")
    for i, h in enumerate(top_hashtags, 1):
        print(f"   {i:2}. #{h['_id']:<30} → {h['count']} fois")

if stats_operateurs:
    print(f"\n📡 OPÉRATEURS CITÉS DANS LES HASHTAGS:")
    for op in stats_operateurs:
        print(f"   • {op['_id']:<20} → {op['count']} fois")

if stats_sujets:
    print(f"\n📋 SUJETS DÉTECTÉS DANS LES HASHTAGS:")
    for s in stats_sujets:
        print(f"   • {s['_id']:<20} → {s['count']} fois")

if hashtags_restants == 0:
    print("\n   ✅ SUCCÈS : Tous les hashtags supprimés du texte !")
else:
    print(f"\n   ⚠️  ATTENTION : {hashtags_restants} hashtags restants")

if erreurs:
    print(f"\n   ⚠️  Erreurs : {len(erreurs)}")
    for e in erreurs:
        print(f"      • {e.get('_erreur')}")

# ============================================================
# 7. RAPPORT FINAL
# ============================================================
temps_fin_global = time.time()
temps_total      = temps_fin_global - temps_debut_global

top_hashtags_str = "\n".join(
    f"   {i+1:2}. #{h['_id']:<30} → {h['count']} fois"
    for i, h in enumerate(top_hashtags)
) if top_hashtags else "   Aucun hashtag trouvé"

operateurs_str = "\n".join(
    f"   • {op['_id']:<20} → {op['count']} fois"
    for op in stats_operateurs
) if stats_operateurs else "   Aucun opérateur détecté"

sujets_str = "\n".join(
    f"   • {s['_id']:<20} → {s['count']} fois"
    for s in stats_sujets
) if stats_sujets else "   Aucun sujet détecté"

rapport = f"""
{"="*70}
RAPPORT — EXTRACTION + SUPPRESSION HASHTAGS — MODE DISTRIBUÉ
{"="*70}
Date             : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode             : Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

⏱️  TEMPS D'EXÉCUTION:
   • Connexion Spark     : {temps_fin_spark - temps_debut_spark:.2f}s
   • Chargement données  : {temps_fin_chargement - temps_debut_chargement:.2f}s
   • Analyse             : {temps_fin_analyse - temps_debut_analyse:.2f}s
   • Traitement          : {temps_fin_traitement - temps_debut_traitement:.2f}s
   • ─────────────────────────────────
   • TEMPS TOTAL         : {temps_total:.2f}s
   • Vitesse             : {total / temps_total:.2f} doc/s

📊 RÉSULTATS:
   • Total documents        : {total}
   • Documents avec hashtags: {avec_hashtags_av}
   • Hashtags extraits      : {total_hashtags_extraits}
   • Hashtags restants      : {hashtags_restants}
   • Statut : {"✅ SUCCÈS" if hashtags_restants == 0 else "⚠️ INCOMPLET"}

🏆 TOP HASHTAGS:
{top_hashtags_str}

📡 OPÉRATEURS CITÉS:
{operateurs_str}

📋 SUJETS DÉTECTÉS:
{sujets_str}

📁 STOCKAGE:
   • Source      : {DB_NAME}.{COLLECTION_SOURCE}
   • Destination : {DB_NAME}.{COLLECTION_DEST}
   • Insérés     : {total_inseres}

📄 EXEMPLE DOCUMENT MONGODB:
   {{
     "Commentaire_Client"  : "texte sans hashtag",
     "hashtags_detectes"   : ["Djezzy", "قطع_الانترنت"],
     "nb_hashtags"         : 2,
     "operateur_cite"      : "djezzy",
     "sujet_hashtag"       : "reseau",
     "a_hashtags"          : true
   }}
"""

os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
    f.write(rapport)
print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

# ============================================================
# RÉSUMÉ CONSOLE
# ============================================================
print("\n" + "="*70)
print("📊 RÉSUMÉ FINAL")
print("="*70)
print(f"📥 Documents traités       : {total}")
print(f"📤 Documents insérés       : {total_inseres}")
print(f"🏷️  Hashtags extraits       : {total_hashtags_extraits}")
print(f"📁 Docs avec hashtags      : {docs_avec_hashtags}")
print(f"⏱️  Temps total             : {temps_total:.2f}s")
print(f"🚀 Vitesse                 : {total/temps_total:.0f} docs/s")
print(f"📁 Collection destination  : {DB_NAME}.{COLLECTION_DEST}")
print("="*70)
print("🎉 EXTRACTION DES HASHTAGS TERMINÉE EN MODE MULTI-NODE !")

# ============================================================
# FERMETURE
# ============================================================
spark.stop()
client_driver.close()
print("🔌 Connexions fermées proprement")