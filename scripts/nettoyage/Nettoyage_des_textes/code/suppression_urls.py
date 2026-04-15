

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# # scripts/nettoyage/suppression_urls.py
# # VERSION VRAIMENT DISTRIBUÉE — mapPartitions
# # Spark 4.1.1 / Scala 2.13 / Java 21
# # Nettoyage : URLs + @ + # + Ponctuations + Lignes vides
# # Chaque Worker lit ET écrit MongoDB directement
# #
# # ═══════════════════════════════════════════════════════════════
# # CORRECTIONS v1.7
# #
# #   FIX-C4  Deux-points entre chiffres préservé (18:00, 23:00)
# #   FIX-C5  Placeholders sans underscore
# #   FIX-C6  Wi-Fi → wifi avant suppression des tirets
# #   FIX-C7  Dates préservées :
# #           DD/MM/YYYY (63 cas) — slash
# #           DD/MM/YY   (33 cas) — slash
# #           DD-MM-YYYY (1 cas)  — tiret
# #   FIX-C7c Dates avec backslash DD\MM\YYYY → DD/MM/YYYY
# #   FIX-C7d Dates avec espaces après mot-clé :
# #           "من 18 10 2025" → "من 18/10/2025"
# #           "depuis le 20 09 2025" → "depuis le 20/09/2025"
# #   FIX-C8  Ponctuation naturelle gardée (1 seule) : ؟ ! . ، ؛ / ? '
# #           Lignes que ponctuation → supprimées
# # ═══════════════════════════════════════════════════════════════

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, regexp_replace, trim, when
# from pyspark.sql.functions import sum as spark_sum, count as spark_count
# from pymongo import MongoClient, InsertOne
# from pymongo.errors import BulkWriteError
# from datetime import datetime
# from spark_reporter import SparkReporter   # ← AJOUT REPORTER
# import os, time, math

# # ============================================================
# # CONFIGURATION CENTRALISÉE
# # ============================================================
# MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# DB_NAME           = "telecom_algerie"
# COLLECTION_SOURCE = "commentaires_bruts"
# COLLECTION_DEST   = "commentaires_sans_urls_arobase"
# NB_WORKERS        = 3
# SPARK_MASTER      = "spark://spark-master:7077"
# RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_nettoyage_complet.txt"
# STATS_FILE        = "/tmp/spark_stats.json"   # ← AJOUT : fichier partagé avec le dashboard

# # ============================================================
# # PATTERNS REGEX (pour analyse Spark)
# # ============================================================
# PATTERN_URLS           = r'https?://\S*|www\.\S+'
# PATTERN_AT             = r'@'
# PATTERN_HASHTAG        = r'#'
# PATTERN_ESPACES        = r'\s+'
# PATTERN_DETECTION      = r'https?://|www\.'
# PATTERN_DETECT_PONCT   = r'[.,!?;:"\'«»؟،؛]'
# PATTERN_DETECT_HASHTAG = r'#'

# # ============================================================
# # FONCTIONS DISTRIBUÉES (s'exécutent sur les Workers)
# # ============================================================

# def nettoyer_et_ecrire_partition(partition):
#     import socket
#     hostname = socket.gethostname()
#     print(f"🖥️  Worker {hostname} — début traitement partition")

#     import sys, json, os
#     sys.path.insert(0, '/opt/pymongo_libs')
#     import re
#     from pymongo import MongoClient, InsertOne
#     from pymongo.errors import BulkWriteError

#     # ── Tous les patterns définis ICI — accessibles par nettoyer_texte() ──

#     re_urls          = re.compile(r'https?://\S*|www\.\S+', re.IGNORECASE)
#     re_at            = re.compile(r'@')
#     re_hashtag       = re.compile(r'#')
#     re_espaces       = re.compile(r'\s+')

#     # FIX-C8 : ponctuation naturelle
#     re_rep_punct     = re.compile(r"([؟!.,،؛/?'])\1+")
#     re_ponct_sup     = re.compile(
#         r'["«»()\[\]{}\u2026\u2013\u2014~`^*+=<>|\\_;:]')
#     re_tiret_non_num = re.compile(r'(?<![A-Za-z0-9])-(?![A-Za-z0-9])')
#     re_only_punct    = re.compile(r"^[؟!.,،؛/?'\s]+$")

#     # FIX-C7d : dates avec espaces après mot-clé
#     re_date_espaces  = re.compile(
#         r'(من|منذ|depuis\s+le|depuis|تاريخ)\s+'
#         r'(0?[1-9]|[12]\d|3[01])\s+'
#         r'(0?[1-9]|1[0-2])\s+'
#         r'(20\d{2})'
#     )

#     # FIX-C7c : dates avec backslash DD\MM\YYYY
#     re_date_back  = re.compile(r'(?<!\d)(\d{1,2})\\(\d{1,2})\\(\d{2,4})(?!\d)')

#     # FIX-C7 : dates avec slash DD/MM/YYYY et DD/MM/YY
#     re_date_slash = re.compile(r'(?<!\d)(\d{1,2})/(\d{1,2})/(\d{2,4})(?!\d)')

#     # FIX-C7 : dates avec tiret DD-MM-YYYY
#     re_date_dash  = re.compile(r'(?<!\d)(\d{1,2})-(\d{1,2})-(\d{4})(?!\d)')

#     # FIX-C4 : séparateurs numériques
#     re_num_dash  = re.compile(r'(?<!\d)(\d+)-(\d+)(?!\d)')
#     re_num_slash = re.compile(r'(?<!\d)(\d+)/(\d+)(?!\d)')
#     re_num_colon = re.compile(r'(?<!\d)(\d+):(\d+)(?!\d)')

#     # FIX-C5 : placeholders SANS underscore ni tiret
#     DASH_PH  = 'XNUMDASHX'
#     SLASH_PH = 'XNUMSLASHX'
#     COLON_PH = 'XNUMCOLONX'
#     DATE_S1  = 'XDATESLASH1X'
#     DATE_S2  = 'XDATESLASH2X'
#     DATE_D1  = 'XDATEDASH1X'
#     DATE_D2  = 'XDATEDASH2X'
#     DATE_B1  = 'XDATEBACK1X'
#     DATE_B2  = 'XDATEBACK2X'

#     # FIX-C6
#     re_wifi = re.compile(r'\bwi-fi\b', re.IGNORECASE)

#     def nettoyer_texte(texte):
#         if not texte or not isinstance(texte, str):
#             return None

#         texte = re_date_espaces.sub(
#             lambda m: f"{m.group(1)} {m.group(2)}/{m.group(3)}/{m.group(4)}",
#             texte)
#         texte = re_wifi.sub('wifi', texte)
#         texte = re_date_back.sub(
#             lambda m: f"{m.group(1)}{DATE_B1}{m.group(2)}{DATE_B2}{m.group(3)}",
#             texte)
#         texte = re_date_slash.sub(
#             lambda m: f"{m.group(1)}{DATE_S1}{m.group(2)}{DATE_S2}{m.group(3)}",
#             texte)
#         texte = re_date_dash.sub(
#             lambda m: f"{m.group(1)}{DATE_D1}{m.group(2)}{DATE_D2}{m.group(3)}",
#             texte)
#         texte = re_num_dash.sub(
#             lambda m: f"{m.group(1)}{DASH_PH}{m.group(2)}", texte)
#         texte = re_num_slash.sub(
#             lambda m: f"{m.group(1)}{SLASH_PH}{m.group(2)}", texte)
#         texte = re_num_colon.sub(
#             lambda m: f"{m.group(1)}{COLON_PH}{m.group(2)}", texte)
#         texte = re_rep_punct.sub(r'\1', texte)
#         texte = re_urls.sub('', texte)
#         texte = re_at.sub('', texte)
#         texte = re_hashtag.sub('', texte)
#         texte = re_ponct_sup.sub(' ', texte)
#         texte = re_tiret_non_num.sub(' ', texte)
#         texte = re_espaces.sub(' ', texte).strip()
#         texte = texte.replace(DATE_B1, '/').replace(DATE_B2, '/')
#         texte = texte.replace(DATE_S1, '/').replace(DATE_S2, '/')
#         texte = texte.replace(DATE_D1, '-').replace(DATE_D2, '-')
#         texte = texte.replace(DASH_PH,  '-')
#         texte = texte.replace(SLASH_PH, '/')
#         texte = texte.replace(COLON_PH, ':')

#         if not texte or re_only_punct.match(texte):
#             return None

#         return texte

#     try:
#         client     = MongoClient("mongodb://mongodb_pfe:27017/",
#                                  serverSelectionTimeoutMS=5000)
#         db         = client["telecom_algerie"]
#         collection = db[COLLECTION_DEST]
#     except Exception as e:
#         yield {"_erreur": str(e), "statut": "connexion_failed"}
#         return

#     batch        = []
#     docs_traites = 0
#     docs_vides   = 0

#     for row in partition:
#         commentaire_propre = nettoyer_texte(row.get("Commentaire_Client"))

#         if not commentaire_propre or commentaire_propre.strip() == "":
#             docs_vides += 1
#             continue

#         doc = {
#             "_id"                    : row.get("_id"),
#             "Commentaire_Client"     : commentaire_propre,
#             "commentaire_moderateur" : nettoyer_texte(
#                                          row.get("commentaire_moderateur")),
#             "date"                   : row.get("date"),
#             "source"                 : row.get("source"),
#             "moderateur"             : row.get("moderateur"),
#             "metadata"               : row.get("metadata"),
#             "statut"                 : row.get("statut"),
#         }
#         doc_propre = {k: (None if v != v else v) for k, v in doc.items()}
#         batch.append(InsertOne(doc_propre))
#         docs_traites += 1

#         if len(batch) >= 1000:
#             try:
#                 collection.bulk_write(batch, ordered=False)
#             except BulkWriteError:
#                 pass
#             batch = []

#     if batch:
#         try:
#             collection.bulk_write(batch, ordered=False)
#         except BulkWriteError:
#             pass

#     client.close()

#     # ── AJOUT REPORTER : écrire les stats du worker dans le fichier JSON ──
#     stats_file = "/tmp/spark_stats.json"
#     try:
#         with open(stats_file, "r", encoding="utf-8") as f:
#             data = json.load(f)
#         workers = data.get("workers", [])
#         found = False
#         for w in workers:
#             if w["hostname"] == hostname:
#                 w["docs_traites"] += docs_traites
#                 w["docs_vides"]   += docs_vides
#                 found = True
#                 break
#         if not found:
#             workers.append({
#                 "hostname"    : hostname,
#                 "docs_traites": docs_traites,
#                 "docs_vides"  : docs_vides
#             })
#         data["workers"] = workers
#         with open(stats_file, "w", encoding="utf-8") as f:
#             json.dump(data, f, ensure_ascii=False)
#     except Exception as ex:
#         print(f"[reporter-worker] {ex}")
#     # ─────────────────────────────────────────────────────────────────────

#     print(f"🖥️  Worker {hostname} — ✅ {docs_traites} docs traités, {docs_vides} vides ignorés")
#     yield {"docs_traites": docs_traites, "docs_vides": docs_vides, "statut": "ok"}


# def lire_partition_depuis_mongo(partition_info):
#     import sys
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient

#     for item in partition_info:
#         client     = MongoClient("mongodb://mongodb_pfe:27017/",
#                                  serverSelectionTimeoutMS=5000)
#         db         = client["telecom_algerie"]
#         collection = db[COLLECTION_SOURCE]

#         curseur = collection.find(
#             {},
#             {"_id": 1, "Commentaire_Client": 1,
#              "commentaire_moderateur": 1, "date": 1, "source": 1,
#              "moderateur": 1, "metadata": 1, "statut": 1}
#         ).skip(item["skip"]).limit(item["limit"])

#         for doc in curseur:
#             doc["_id"] = str(doc["_id"])
#             yield doc

#         client.close()


# # ============================================================
# # DÉBUT DU PIPELINE
# # ============================================================
# temps_debut_global = time.time()

# print("=" * 70)
# print("🔍 NETTOYAGE COMPLET v1.7 — URLs + @ + # + Ponctuations")
# print("   FIX-C4  : 18:00 / 23:00 préservés ✅")
# print("   FIX-C6  : Wi-Fi → wifi ✅")
# print("   FIX-C7  : DD/MM/YYYY + DD/MM/YY + DD-MM-YYYY préservés ✅")
# print("   FIX-C7c : DD\\MM\\YYYY → DD/MM/YYYY ✅")
# print("   FIX-C7d : 'من 18 10 2025' → 'من 18/10/2025' ✅")
# print("   FIX-C8  : ؟؟؟؟→؟  !!!→!  ...→.  '''→'  garder 1 ✅")
# print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# print("=" * 70)

# # 1. CONNEXION MONGODB DRIVER
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

# # ── AJOUT REPORTER : initialisation ──────────────────────────
# reporter = SparkReporter(nb_workers=NB_WORKERS, total_docs=total_docs)
# # ─────────────────────────────────────────────────────────────

# # 2. CONNEXION SPARK
# print("\n⚡ Connexion au cluster Spark...")
# temps_debut_spark = time.time()

# spark = SparkSession.builder \
#     .appName("Nettoyage_Complet_MultiNode_v7") \
#     .master(SPARK_MASTER) \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.sql.shuffle.partitions", "4") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")
# temps_fin_spark = time.time()
# print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")

# # ── AJOUT REPORTER : Spark connecté ──────────────────────────
# reporter.spark_connected(
#     elapsed_spark=round(temps_fin_spark - temps_debut_spark, 2)
# )
# # ─────────────────────────────────────────────────────────────

# # 3. LECTURE DISTRIBUÉE
# print("\n📥 LECTURE DISTRIBUÉE — Chaque Worker lit sa portion...")
# temps_debut_chargement = time.time()

# docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# plages = [
#     {"skip" : i * docs_par_worker,
#      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
#     for i in range(NB_WORKERS)
# ]

# for idx, p in enumerate(plages):
#     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']} docs")

# rdd_data = spark.sparkContext \
#     .parallelize(plages, NB_WORKERS) \
#     .mapPartitions(lire_partition_depuis_mongo)

# df_spark = spark.read.json(rdd_data.map(
#     lambda d: __import__('json').dumps(
#         {k: str(v) if not isinstance(v, (str, int, float, bool,
#                                          type(None))) else v
#          for k, v in d.items()}
#     )
# ))

# total_lignes = df_spark.count()
# temps_fin_chargement = time.time()
# print(f"✅ {total_lignes} documents chargés en "
#       f"{temps_fin_chargement - temps_debut_chargement:.2f}s")

# # ── AJOUT REPORTER : chargement terminé ──────────────────────
# reporter.loading_done(
#     total_lignes=total_lignes,
#     elapsed_load=round(temps_fin_chargement - temps_debut_chargement, 2)
# )
# # ─────────────────────────────────────────────────────────────

# # 4. ANALYSE AVANT
# print("\n🔎 ANALYSE AVANT NETTOYAGE...")
# temps_debut_analyse = time.time()

# df_analyse = df_spark \
#     .withColumn("a_url",
#         when(col("Commentaire_Client").rlike(PATTERN_DETECTION), 1).otherwise(0)) \
#     .withColumn("a_at",
#         when(col("Commentaire_Client").rlike(PATTERN_AT), 1).otherwise(0)) \
#     .withColumn("a_hashtag",
#         when(col("Commentaire_Client").rlike(PATTERN_DETECT_HASHTAG), 1).otherwise(0)) \
#     .withColumn("a_ponct",
#         when(col("Commentaire_Client").rlike(PATTERN_DETECT_PONCT), 1).otherwise(0)) \
#     .withColumn("a_heure",
#         when(col("Commentaire_Client").rlike(r"\d{1,2}:\d{2}"), 1).otherwise(0)) \
#     .withColumn("a_wifi",
#         when(col("Commentaire_Client").rlike(r"(?i)wi-fi"), 1).otherwise(0)) \
#     .withColumn("a_date",
#         when(col("Commentaire_Client")
#              .rlike(r"\d{1,2}[/\\\-]\d{1,2}[/\\\-]\d{2,4}"), 1).otherwise(0))

# stats_avant = df_analyse.agg(
#     spark_count("*").alias("total"),
#     spark_sum("a_url").alias("avec_urls"),
#     spark_sum("a_at").alias("avec_at"),
#     spark_sum("a_hashtag").alias("avec_hashtag"),
#     spark_sum("a_ponct").alias("avec_ponct"),
#     spark_sum("a_heure").alias("avec_heures"),
#     spark_sum("a_wifi").alias("avec_wifi"),
#     spark_sum("a_date").alias("avec_dates"),
# ).collect()[0]

# total           = stats_avant["total"]
# avec_urls_av    = int(stats_avant["avec_urls"]    or 0)
# avec_at_av      = int(stats_avant["avec_at"]      or 0)
# avec_hashtag_av = int(stats_avant["avec_hashtag"] or 0)
# avec_ponct_av   = int(stats_avant["avec_ponct"]   or 0)
# avec_heures_av  = int(stats_avant["avec_heures"]  or 0)
# avec_wifi_av    = int(stats_avant["avec_wifi"]    or 0)
# avec_dates_av   = int(stats_avant["avec_dates"]   or 0)

# temps_fin_analyse = time.time()

# print(f"\n📊 AVANT NETTOYAGE (en {temps_fin_analyse - temps_debut_analyse:.2f}s):")
# print(f"   ┌────────────────────────────────────────────┐")
# print(f"   │ Total documents         : {total:<15} │")
# print(f"   │ Avec URLs               : {avec_urls_av:<15} │")
# print(f"   │ Avec @                  : {avec_at_av:<15} │")
# print(f"   │ Avec #                  : {avec_hashtag_av:<15} │")
# print(f"   │ Avec ponctuations       : {avec_ponct_av:<15} │")
# print(f"   │ Avec heures (HH:MM)     : {avec_heures_av:<15} │")
# print(f"   │ Avec Wi-Fi              : {avec_wifi_av:<15} │")
# print(f"   │ Avec dates (DD/MM+)     : {avec_dates_av:<15} │")
# print(f"   │ % ponctuations          : {(avec_ponct_av/total*100):<14.2f}% │")
# print(f"   └────────────────────────────────────────────┘")

# # ── AJOUT REPORTER : analyse terminée ────────────────────────
# reporter.analyse_done(
#     stats_avant={
#         "total"        : total,
#         "avec_urls"    : avec_urls_av,
#         "avec_at"      : avec_at_av,
#         "avec_hashtag" : avec_hashtag_av,
#         "avec_ponct"   : avec_ponct_av,
#         "avec_heures"  : avec_heures_av,
#         "avec_wifi"    : avec_wifi_av,
#         "avec_dates"   : avec_dates_av,
#     },
#     elapsed_analyse=round(temps_fin_analyse - temps_debut_analyse, 2)
# )
# # ─────────────────────────────────────────────────────────────

# # 5. ÉCRITURE DISTRIBUÉE
# print("\n💾 NETTOYAGE + ÉCRITURE DISTRIBUÉE...")
# temps_debut_sauvegarde = time.time()

# coll_dest = db_driver[COLLECTION_DEST]
# coll_dest.delete_many({})
# print("   🧹 Collection destination vidée")

# df_final = df_spark.select(
#     "_id", "Commentaire_Client", "commentaire_moderateur",
#     "date", "source", "moderateur", "metadata", "statut"
# )

# print("   📤 Workers en train de nettoyer et écrire...")
# rdd_stats = df_final.rdd \
#     .map(lambda row: row.asDict()) \
#     .mapPartitions(nettoyer_et_ecrire_partition)

# stats_ecriture = rdd_stats.collect()
# total_inseres  = sum(s.get("docs_traites", 0)
#                      for s in stats_ecriture if s.get("statut") == "ok")
# total_vides    = sum(s.get("docs_vides", 0)
#                      for s in stats_ecriture if s.get("statut") == "ok")
# erreurs        = [s for s in stats_ecriture if "_erreur" in s]

# temps_fin_sauvegarde = time.time()
# print(f"✅ {total_inseres} documents écrits en "
#       f"{temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s")
# print(f"🗑️  {total_vides} lignes vides ignorées (non insérées)")

# if erreurs:
#     for e in erreurs:
#         print(f"   ⚠️  {e.get('_erreur')}")

# # ── AJOUT REPORTER : écriture terminée ───────────────────────
# reporter.writing_done(
#     total_inseres=total_inseres,
#     total_vides=total_vides,
#     elapsed_write=round(temps_fin_sauvegarde - temps_debut_sauvegarde, 2)
# )
# # ─────────────────────────────────────────────────────────────

# # 6. VÉRIFICATION FINALE
# print("\n🔎 VÉRIFICATION FINALE...")

# urls_restantes   = coll_dest.count_documents(
#     {"Commentaire_Client": {"$regex": PATTERN_DETECTION, "$options": "i"}})
# at_restants      = coll_dest.count_documents(
#     {"Commentaire_Client": {"$regex": "@"}})
# hashtag_restants = coll_dest.count_documents(
#     {"Commentaire_Client": {"$regex": "#"}})
# vides_restants   = coll_dest.count_documents({"$or": [
#     {"Commentaire_Client": ""},
#     {"Commentaire_Client": None},
#     {"Commentaire_Client": {"$regex": "^\\s*$"}}
# ]})
# dates_cassees    = coll_dest.count_documents(
#     {"Commentaire_Client": {"$regex": "\\d{1,2} \\d{2} \\d{4}"}})
# wifi_casse       = coll_dest.count_documents(
#     {"Commentaire_Client": {"$regex": "wi \u0641\u064a"}})
# total_en_dest    = coll_dest.count_documents({})

# succes = (urls_restantes == 0 and at_restants == 0
#           and hashtag_restants == 0 and vides_restants == 0
#           and dates_cassees == 0 and wifi_casse == 0)

# print(f"   • Documents en destination  : {total_en_dest}")
# print(f"   • URLs restantes            : {urls_restantes}")
# print(f"   • @ restants                : {at_restants}")
# print(f"   • # restants                : {hashtag_restants}")
# print(f"   • Lignes vides restantes    : {vides_restants}")
# print(f"   • Dates cassées  (FIX-C7)   : {dates_cassees}"
#       f"  {'✅' if dates_cassees == 0 else '❌'}")
# print(f"   • Wi-Fi cassé    (FIX-C6)   : {wifi_casse}"
#       f"  {'✅' if wifi_casse == 0 else '❌'}")
# print(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")

# # 7. RAPPORT FINAL
# temps_fin_global = time.time()
# temps_total      = temps_fin_global - temps_debut_global

# lignes_rapport = []
# lignes_rapport.append("=" * 70)
# lignes_rapport.append("🔍 NETTOYAGE COMPLET v1.7 — URLs + @ + # + Ponctuations")
# lignes_rapport.append(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# lignes_rapport.append(f"   Mode : Spark 4.1.1 | mapPartitions | {NB_WORKERS} Workers → MongoDB direct")
# lignes_rapport.append("=" * 70)
# lignes_rapport.append(f"\n📊 AVANT NETTOYAGE :")
# lignes_rapport.append(f"   ┌────────────────────────────────────────────┐")
# lignes_rapport.append(f"   │ Total documents         : {total:<15} │")
# lignes_rapport.append(f"   │ Avec URLs               : {avec_urls_av:<15} │")
# lignes_rapport.append(f"   │ Avec @                  : {avec_at_av:<15} │")
# lignes_rapport.append(f"   │ Avec #                  : {avec_hashtag_av:<15} │")
# lignes_rapport.append(f"   │ Avec ponctuations       : {avec_ponct_av:<15} │")
# lignes_rapport.append(f"   │ Avec heures (HH:MM)     : {avec_heures_av:<15} │")
# lignes_rapport.append(f"   │ Avec Wi-Fi              : {avec_wifi_av:<15} │")
# lignes_rapport.append(f"   │ Avec dates (DD/MM+)     : {avec_dates_av:<15} │")
# lignes_rapport.append(f"   │ % ponctuations          : {(avec_ponct_av/total*100):<14.2f}% │")
# lignes_rapport.append(f"   └────────────────────────────────────────────┘")
# lignes_rapport.append(f"\n💾 NETTOYAGE + ÉCRITURE DISTRIBUÉE...")
# lignes_rapport.append(f"✅ {total_inseres} documents écrits en {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s")
# lignes_rapport.append(f"🗑️  {total_vides} lignes vides ignorées (non insérées)")
# lignes_rapport.append(f"\n🔎 VÉRIFICATION FINALE...")
# lignes_rapport.append(f"   • Documents en destination  : {total_en_dest}")
# lignes_rapport.append(f"   • URLs restantes            : {urls_restantes}")
# lignes_rapport.append(f"   • @ restants                : {at_restants}")
# lignes_rapport.append(f"   • # restants                : {hashtag_restants}")
# lignes_rapport.append(f"   • Lignes vides restantes    : {vides_restants}")
# lignes_rapport.append(f"   • Dates cassées  (FIX-C7)   : {dates_cassees}  {'✅' if dates_cassees == 0 else '❌'}")
# lignes_rapport.append(f"   • Wi-Fi cassé    (FIX-C6)   : {wifi_casse}  {'✅' if wifi_casse == 0 else '❌'}")
# lignes_rapport.append(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")
# lignes_rapport.append("\n" + "=" * 70)
# lignes_rapport.append("📊 RÉSUMÉ FINAL")
# lignes_rapport.append("=" * 70)
# lignes_rapport.append(f"📥 Documents source        : {total}")
# lignes_rapport.append(f"📤 Documents insérés       : {total_inseres}")
# lignes_rapport.append(f"🗑️  Lignes vides ignorées   : {total_vides}")
# lignes_rapport.append(f"✅ Heures préservées (C4)   : {avec_heures_av}")
# lignes_rapport.append(f"✅ Dates préservées  (C7)   : {avec_dates_av}")
# lignes_rapport.append(f"✅ Wi-Fi protégé    (C6)    : {avec_wifi_av}")
# lignes_rapport.append(f"⏱️  Temps total             : {temps_total:.2f}s")
# lignes_rapport.append(f"🚀 Vitesse                 : {total/temps_total:.0f} docs/s")
# lignes_rapport.append(f"📁 Collection source       : {DB_NAME}.{COLLECTION_SOURCE}")
# lignes_rapport.append(f"📁 Collection dest.        : {DB_NAME}.{COLLECTION_DEST}")
# lignes_rapport.append("=" * 70)
# lignes_rapport.append(f"   Statut : {'✅ SUCCÈS' if succes else '⚠️ INCOMPLET'}")
# lignes_rapport.append("=" * 70)
# lignes_rapport.append("")
# lignes_rapport.append("PONCTUATION GARDÉE (1 seule) : ؟  !  .  ،  ؛  /  ?  '")
# lignes_rapport.append("PONCTUATION SUPPRIMÉE        : \" « » ( ) [ ] { }")
# lignes_rapport.append("                               ~ ` ^ * + = < > | \\ _")
# lignes_rapport.append("                               tirets non numériques")
# lignes_rapport.append("")
# lignes_rapport.append("CORRECTIONS APPLIQUÉES :")
# lignes_rapport.append(f"   FIX-C4  : Heures préservées (18:00, 23:00)          — {avec_heures_av} cas")
# lignes_rapport.append(f"   FIX-C6  : Wi-Fi → wifi                              — {avec_wifi_av} cas")
# lignes_rapport.append(f"   FIX-C7  : Dates préservées (DD/MM/YY, DD-MM-YYYY)   — {avec_dates_av} cas")
# lignes_rapport.append( "   FIX-C7c : DD\\MM\\YYYY → DD/MM/YYYY")
# lignes_rapport.append( "   FIX-C7d : 'من 18 10 2025' → 'من 18/10/2025'")
# lignes_rapport.append( "   FIX-C8  : Ponctuation répétée réduite à 1")
# lignes_rapport.append("")
# lignes_rapport.append("⏱️  TEMPS DÉTAILLÉ :")
# lignes_rapport.append(f"   • Connexion Spark  : {temps_fin_spark - temps_debut_spark:.2f}s")
# lignes_rapport.append(f"   • Chargement       : {temps_fin_chargement - temps_debut_chargement:.2f}s")
# lignes_rapport.append(f"   • Analyse          : {temps_fin_analyse - temps_debut_analyse:.2f}s")
# lignes_rapport.append(f"   • Écriture MongoDB : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s")
# lignes_rapport.append(f"   • TOTAL            : {temps_total:.2f}s ({total/temps_total:.0f} doc/s)")
# lignes_rapport.append("=" * 70)

# rapport_texte = "\n".join(lignes_rapport)
# os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
#     f.write(rapport_texte)
# print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

# print("\n" + "=" * 70)
# print("📊 RÉSUMÉ FINAL")
# print("=" * 70)
# print(f"📥 Documents source        : {total}")
# print(f"📤 Documents insérés       : {total_inseres}")
# print(f"🗑️  Lignes vides ignorées   : {total_vides}")
# print(f"✅ Heures préservées (C4)   : {avec_heures_av}")
# print(f"✅ Dates préservées  (C7)   : {avec_dates_av}")
# print(f"✅ Wi-Fi protégé    (C6)    : {avec_wifi_av}")
# print(f"⏱️  Temps total             : {temps_total:.2f}s")
# print(f"🚀 Vitesse                 : {total/temps_total:.0f} docs/s")
# print(f"📁 Collection source       : {DB_NAME}.{COLLECTION_SOURCE}")
# print(f"📁 Collection dest.        : {DB_NAME}.{COLLECTION_DEST}")
# print("=" * 70)
# print(f"   Statut : {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")
# print("=" * 70)
# print("🎉 PIPELINE DISTRIBUÉ v1.7 TERMINÉ !")

# # ============================================================
# # BENCHMARK : preuve du gain avec NB_WORKERS
# # ============================================================
# print("\n📊 BENCHMARK PARALLÉLISME")
# print("=" * 50)

# temps_debut_1 = time.time()
# rdd_test_1 = spark.sparkContext \
#     .parallelize(plages[:1], 1) \
#     .mapPartitions(lire_partition_depuis_mongo)
# count_1 = rdd_test_1.count()
# temps_1 = time.time() - temps_debut_1

# temps_debut_3 = time.time()
# rdd_test_3 = spark.sparkContext \
#     .parallelize(plages, NB_WORKERS) \
#     .mapPartitions(lire_partition_depuis_mongo)
# count_3 = rdd_test_3.count()
# temps_3 = time.time() - temps_debut_3

# speedup = temps_1 / temps_3 if temps_3 > 0 else 0

# print(f"   • 1 worker  : {temps_1:.2f}s  ({count_1} docs)")
# print(f"   • 3 workers : {temps_3:.2f}s  ({count_3} docs)")
# print(f"   • Speedup   : {speedup:.2f}x  ✅")
# print("=" * 50)

# # ── AJOUT REPORTER : benchmark + final ───────────────────────
# reporter.benchmark_done(
#     temps_1=temps_1, count_1=count_1,
#     temps_3=temps_3, count_3=count_3
# )

# reporter.final(
#     stats_apres={
#         "urls_restantes"   : urls_restantes,
#         "at_restants"      : at_restants,
#         "hashtag_restants" : hashtag_restants,
#         "vides_restants"   : vides_restants,
#         "avec_dates"       : avec_dates_av,
#         "avec_heures"      : avec_heures_av,
#     },
#     total_docs=total,
#     total_inseres=total_inseres,
#     total_vides=total_vides,
#     vitesse=round(total / temps_total),
#     succes=succes,
#     timeline={
#         "connexion_spark"  : round(temps_fin_spark     - temps_debut_spark, 2),
#         "chargement"       : round(temps_fin_chargement - temps_debut_chargement, 2),
#         "analyse"          : round(temps_fin_analyse   - temps_debut_analyse, 2),
#         "ecriture_mongodb" : round(temps_fin_sauvegarde - temps_debut_sauvegarde, 2),
#     }
# )
# # ─────────────────────────────────────────────────────────────

# spark.stop()
# client_driver.close()
# print("🔌 Connexions fermées proprement")



#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/nettoyage/suppression_urls.py
# ÉTAPE 1: Suppression des URLs, @, #, ponctuations
# VERSION AVEC GESTION DU FLAG "traite" - CORRIGÉE POUR STRING IDS

from pyspark.sql import SparkSession
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime
import time, math, json, os, re

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_bruts"
COLLECTION_DEST   = "commentaires_sans_urls_arobase"
NB_WORKERS        = 3
SPARK_MASTER      = "spark://spark-master:7077"

# Dossier pour les logs
LOG_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# ============================================================
# FONCTIONS DE GESTION DES FLAGS
# ============================================================

def get_nouveaux_commentaires_count():
    """Compte les commentaires avec traite=false dans la source"""
    client = MongoClient(MONGO_URI_DRIVER, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION_SOURCE]
    count = collection.count_documents({"traite": False})
    client.close()
    return count


def marquer_comme_traite(ids):
    """Marque les commentaires comme traités (traite=True) dans la collection source"""
    if not ids:
        print("   ⚠️ Aucun ID à marquer")
        return
    
    print(f"   📝 Tentative de marquage de {len(ids)} IDs...")
    print(f"   📝 Premier ID: {ids[0] if ids else 'aucun'}")
    
    client = MongoClient(MONGO_URI_DRIVER, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION_SOURCE]
    
    # ⚠️ IMPORTANT: Les IDs sont des STRINGS, pas des ObjectId !
    # On garde les IDs comme strings pour la recherche
    valid_ids = []
    invalid_ids = []
    
    for id_str in ids:
        try:
            id_str = str(id_str).strip()
            if len(id_str) > 0:
                valid_ids.append(id_str)
            else:
                invalid_ids.append(id_str)
        except Exception as e:
            invalid_ids.append(str(id_str))
    
    print(f"   📝 IDs valides (strings): {len(valid_ids)}, IDs invalides: {len(invalid_ids)}")
    
    if valid_ids:
        # Mise à jour avec les strings (pas besoin de ObjectId)
        resultat = collection.update_many(
            {"_id": {"$in": valid_ids}},  # ← Recherche par string
            {"$set": {"traite": True, "date_traitement_etape1": datetime.now()}}
        )
        print(f"   ✅ {resultat.modified_count} commentaires marqués traite=True dans {COLLECTION_SOURCE}")
        
        # Vérification supplémentaire
        if resultat.modified_count == 0:
            print("   ⚠️ Aucun document trouvé avec ces IDs. Vérification...")
            # Tester un ID spécifique
            test_id = valid_ids[0]
            test_doc = collection.find_one({"_id": test_id})
            if test_doc:
                print(f"   ✅ Document trouvé avec l'ID {test_id}")
                print(f"      traite actuel: {test_doc.get('traite')}")
                print(f"      Type de _id: {type(test_doc.get('_id'))}")
            else:
                print(f"   ❌ Document NON trouvé avec l'ID {test_id}")
    else:
        print("   ⚠️ Aucun ID valide trouvé")
        if invalid_ids[:3]:
            print(f"   ⚠️ Exemples d'IDs invalides: {invalid_ids[:3]}")
    
    client.close()


# ============================================================
# FONCTIONS DE NETTOYAGE
# ============================================================

def get_patterns():
    """Retourne tous les patterns regex pour le nettoyage"""
    
    # Placeholders (sans underscore pour éviter conflits)
    DASH_PH  = 'XNUMDASHX'
    SLASH_PH = 'XNUMSLASHX'
    COLON_PH = 'XNUMCOLONX'
    DATE_S1  = 'XDATESLASH1X'
    DATE_S2  = 'XDATESLASH2X'
    DATE_D1  = 'XDATEDASH1X'
    DATE_D2  = 'XDATEDASH2X'
    DATE_B1  = 'XDATEBACK1X'
    DATE_B2  = 'XDATEBACK2X'
    
    # Compilation des regex
    re_urls          = re.compile(r'https?://\S*|www\.\S+', re.IGNORECASE)
    re_at            = re.compile(r'@')
    re_hashtag       = re.compile(r'#')
    re_espaces       = re.compile(r'\s+')
    re_rep_punct     = re.compile(r"([؟!.,،؛/?'])\1+")
    re_ponct_sup     = re.compile(r'["«»()\[\]{}\u2026\u2013\u2014~`^*+=<>|\\_;:]')
    re_tiret_non_num = re.compile(r'(?<![A-Za-z0-9])-(?![A-Za-z0-9])')
    re_only_punct    = re.compile(r"^[؟!.,،؛/?'\s]+$")
    re_date_espaces  = re.compile(
        r'(من|منذ|depuis\s+le|depuis|تاريخ)\s+'
        r'(0?[1-9]|[12]\d|3[01])\s+'
        r'(0?[1-9]|1[0-2])\s+'
        r'(20\d{2})'
    )
    re_date_back     = re.compile(r'(?<!\d)(\d{1,2})\\(\d{1,2})\\(\d{2,4})(?!\d)')
    re_date_slash    = re.compile(r'(?<!\d)(\d{1,2})/(\d{1,2})/(\d{2,4})(?!\d)')
    re_date_dash     = re.compile(r'(?<!\d)(\d{1,2})-(\d{1,2})-(\d{4})(?!\d)')
    re_num_dash      = re.compile(r'(?<!\d)(\d+)-(\d+)(?!\d)')
    re_num_slash     = re.compile(r'(?<!\d)(\d+)/(\d+)(?!\d)')
    re_num_colon     = re.compile(r'(?<!\d)(\d+):(\d+)(?!\d)')
    re_wifi          = re.compile(r'\bwi-fi\b', re.IGNORECASE)
    
    return {
        're_urls': re_urls, 're_at': re_at, 're_hashtag': re_hashtag,
        're_espaces': re_espaces, 're_rep_punct': re_rep_punct,
        're_ponct_sup': re_ponct_sup, 're_tiret_non_num': re_tiret_non_num,
        're_only_punct': re_only_punct, 're_date_espaces': re_date_espaces,
        're_date_back': re_date_back, 're_date_slash': re_date_slash,
        're_date_dash': re_date_dash, 're_num_dash': re_num_dash,
        're_num_slash': re_num_slash, 're_num_colon': re_num_colon,
        're_wifi': re_wifi,
        'DASH_PH': DASH_PH, 'SLASH_PH': SLASH_PH, 'COLON_PH': COLON_PH,
        'DATE_S1': DATE_S1, 'DATE_S2': DATE_S2,
        'DATE_D1': DATE_D1, 'DATE_D2': DATE_D2,
        'DATE_B1': DATE_B1, 'DATE_B2': DATE_B2
    }


def nettoyer_texte(texte, patterns):
    """Nettoie le texte avec les patterns fournis"""
    if not texte or not isinstance(texte, str):
        return None
    
    p = patterns
    
    # FIX-C7d : dates avec espaces après mot-clé
    texte = p['re_date_espaces'].sub(
        lambda m: f"{m.group(1)} {m.group(2)}/{m.group(3)}/{m.group(4)}",
        texte)
    
    # FIX-C6 : Wi-Fi → wifi
    texte = p['re_wifi'].sub('wifi', texte)
    
    # FIX-C7c : dates avec backslash
    texte = p['re_date_back'].sub(
        lambda m: f"{m.group(1)}{p['DATE_B1']}{m.group(2)}{p['DATE_B2']}{m.group(3)}",
        texte)
    
    # FIX-C7 : dates avec slash
    texte = p['re_date_slash'].sub(
        lambda m: f"{m.group(1)}{p['DATE_S1']}{m.group(2)}{p['DATE_S2']}{m.group(3)}",
        texte)
    
    # FIX-C7 : dates avec tiret
    texte = p['re_date_dash'].sub(
        lambda m: f"{m.group(1)}{p['DATE_D1']}{m.group(2)}{p['DATE_D2']}{m.group(3)}",
        texte)
    
    # Protéger les séparateurs numériques
    texte = p['re_num_dash'].sub(
        lambda m: f"{m.group(1)}{p['DASH_PH']}{m.group(2)}", texte)
    texte = p['re_num_slash'].sub(
        lambda m: f"{m.group(1)}{p['SLASH_PH']}{m.group(2)}", texte)
    texte = p['re_num_colon'].sub(
        lambda m: f"{m.group(1)}{p['COLON_PH']}{m.group(2)}", texte)
    
    # Nettoyage
    texte = p['re_rep_punct'].sub(r'\1', texte)
    texte = p['re_urls'].sub('', texte)
    texte = p['re_at'].sub('', texte)
    texte = p['re_hashtag'].sub('', texte)
    texte = p['re_ponct_sup'].sub(' ', texte)
    texte = p['re_tiret_non_num'].sub(' ', texte)
    texte = p['re_espaces'].sub(' ', texte).strip()
    
    # Restaurer les séparateurs
    texte = texte.replace(p['DATE_B1'], '/').replace(p['DATE_B2'], '/')
    texte = texte.replace(p['DATE_S1'], '/').replace(p['DATE_S2'], '/')
    texte = texte.replace(p['DATE_D1'], '-').replace(p['DATE_D2'], '-')
    texte = texte.replace(p['DASH_PH'], '-')
    texte = texte.replace(p['SLASH_PH'], '/')
    texte = texte.replace(p['COLON_PH'], ':')
    
    # Vérifier si le texte n'est que ponctuation
    if not texte or p['re_only_punct'].match(texte):
        return None
    
    return texte


# ============================================================
# FONCTIONS SPARK
# ============================================================

def lire_commentaires_non_traites_depuis_mongo(partition_info):
    """
    Lecture distribuée depuis MongoDB
    Lit UNIQUEMENT les commentaires avec traite=False
    """
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client = MongoClient("mongodb://mongodb_pfe:27017/",
                             serverSelectionTimeoutMS=5000)
        db = client["telecom_algerie"]
        collection = db["commentaires_bruts"]
        
        # Requête : seulement les commentaires non traités
        query = {"traite": False}
        
        curseur = collection.find(
            query,
            {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
             "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
        ).skip(item["skip"]).limit(item["limit"])
        
        for doc in curseur:
            # Récupérer l'ID sous forme de string
            original_id = str(doc["_id"])
            doc["original_id"] = original_id
            yield doc
        client.close()


def nettoyer_et_ecrire_partition(partition):
    """
    Nettoie les textes et écrit dans la collection destination
    """
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError
    
    patterns = get_patterns()
    
    docs_partition = list(partition)
    
    if len(docs_partition) == 0:
        return []
    
    docs_nettoies = []
    ids_traites = []  # ← TOUS les IDs traités (même les vides)
    docs_vides = 0
    
    for doc in docs_partition:
        texte_original = doc.get("Commentaire_Client", "")
        texte_propre = nettoyer_texte(texte_original, patterns)
        
        original_id = doc.get("original_id")
        if not original_id:
            original_id = str(doc.get("_id", ""))
        
        # ⚠️ CHANGEMENT IMPORTANT : On marque TOUJOURS l'ID comme traité
        ids_traites.append(original_id)
        
        if not texte_propre or texte_propre.strip() == "":
            docs_vides += 1
            continue  # On n'écrit pas dans la destination
        
        # Nettoyer aussi le commentaire modérateur
        commentaire_modo = doc.get("commentaire_moderateur", "")
        commentaire_modo_propre = nettoyer_texte(commentaire_modo, patterns)
        
        # Créer le document nettoyé
        doc_propre = {
            "_id": original_id,
            "Commentaire_Client": texte_propre,
            "commentaire_moderateur": commentaire_modo_propre if commentaire_modo_propre else None,
            "date": doc.get("date"),
            "source": doc.get("source"),
            "moderateur": doc.get("moderateur"),
            "metadata": doc.get("metadata"),
            "statut": "nettoye",
            "traite": False
        }
        
        docs_nettoies.append(doc_propre)
    
    # Écriture dans MongoDB (collection destination)
    if docs_nettoies:
        try:
            client = MongoClient("mongodb://mongodb_pfe:27017/",
                                 serverSelectionTimeoutMS=5000)
            db = client["telecom_algerie"]
            collection = db["commentaires_sans_urls_arobase"]
        except Exception as e:
            yield {"_erreur": str(e), "ids_traites": [], "docs_vides": docs_vides}
            return
        
        batch = []
        for doc in docs_nettoies:
            batch.append(InsertOne(doc))
            if len(batch) >= 1000:
                try:
                    collection.bulk_write(batch, ordered=False)
                except BulkWriteError:
                    pass
                batch = []
        
        if batch:
            try:
                collection.bulk_write(batch, ordered=False)
            except BulkWriteError:
                pass
        
        client.close()
    
    yield {
        "docs_traites": len(docs_partition),
        "docs_nettoies": len(docs_nettoies),
        "docs_vides": docs_vides,
        "ids_traites": ids_traites  # ← TOUS les IDs (y compris vides)
    }

# ============================================================
# MAIN
# ============================================================

def main():
    temps_debut = time.time()
    
    print("=" * 70)
    print("🔍 ÉTAPE 1: NETTOYAGE DES COMMENTAIRES (URLs, @, #, ponctuations)")
    print("   🏷️  Filtre: traite=false dans commentaires_bruts")
    print("   📥 Source: commentaires_bruts")
    print("   📤 Destination: commentaires_sans_urls_arobase")
    print("=" * 70)
    
    # Vérifier les nouveaux commentaires (traite=false)
    nouveaux_count = get_nouveaux_commentaires_count()
    
    if nouveaux_count == 0:
        print("\n✅ Aucun nouveau commentaire à traiter (traite=false)")
        print("   Le pipeline est à jour.")
        return
    
    print(f"\n📥 {nouveaux_count} nouveaux commentaires à traiter (traite=false)")
    
    # Connexion MongoDB
    client_driver = MongoClient(MONGO_URI_DRIVER, serverSelectionTimeoutMS=5000)
    db_driver = client_driver[DB_NAME]
    
    total_docs = nouveaux_count
    print(f"\n📂 {total_docs} documents à traiter")
    
    # Connexion Spark
    print("\n⚡ Connexion au cluster Spark...")
    temps_debut_spark = time.time()
    
    spark = SparkSession.builder \
        .appName("Nettoyage_With_Traite_Flag") \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ Spark connecté en {time.time() - temps_debut_spark:.2f}s")
    
    # Lecture distribuée (uniquement traite=false)
    print("\n📥 Lecture des commentaires avec traite=false...")
    docs_par_worker = math.ceil(total_docs / NB_WORKERS)
    plages = [
        {"skip": i * docs_par_worker,
         "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
        for i in range(NB_WORKERS)
    ]
    
    for idx, p in enumerate(plages):
        print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']} docs")
    
    rdd_data = spark.sparkContext \
        .parallelize(plages, NB_WORKERS) \
        .mapPartitions(lire_commentaires_non_traites_depuis_mongo)
    
    # Convertir en DataFrame
    def prepare_for_json(d):
        result = {}
        for k, v in d.items():
            if k == "original_id":
                continue
            if isinstance(v, (str, int, float, bool, type(None))):
                result[k] = v
            else:
                result[k] = str(v)
        return json.dumps(result)
    
    df_spark = spark.read.json(rdd_data.map(prepare_for_json))
    
    total_lignes = df_spark.count()
    print(f"✅ {total_lignes} nouveaux documents chargés")
    
    # Nettoyage et écriture
    print("\n💾 Nettoyage et écriture distribuée...")
    
    # Afficher le nombre de documents existants dans la destination
    coll_dest = db_driver[COLLECTION_DEST]
    existing_count = coll_dest.count_documents({})
    print(f"   📁 Collection destination: {existing_count} documents existants")
    
    rdd_stats = df_spark.rdd \
        .map(lambda row: row.asDict()) \
        .mapPartitions(nettoyer_et_ecrire_partition)
    
    stats = rdd_stats.collect()
    
    total_traites = sum(s.get("docs_traites", 0) for s in stats)
    total_nettoies = sum(s.get("docs_nettoies", 0) for s in stats)
    total_vides = sum(s.get("docs_vides", 0) for s in stats)
    
    # Récupérer tous les IDs à marquer comme traités
    tous_ids_traites = []
    for s in stats:
        ids = s.get("ids_traites", [])
        if ids:
            tous_ids_traites.extend(ids)
    
    print(f"\n✅ Traitement Spark terminé")
    print(f"\n📊 RÉSULTATS :")
    print(f"   📥 Commentaires lus      : {total_traites}")
    print(f"   ✅ Commentaires nettoyés : {total_nettoies}")
    print(f"   🗑️  Commentaires vides    : {total_vides}")
    print(f"   📝 IDs à marquer         : {len(tous_ids_traites)}")
    
    # Afficher un exemple d'ID pour déboguer
    if tous_ids_traites:
        print(f"   📝 Exemple d'ID: {tous_ids_traites[0]}")
    
    # Marquer les commentaires comme traités dans la collection source
    if tous_ids_traites:
        print("\n🏷️  Marquage des commentaires traités...")
        marquer_comme_traite(tous_ids_traites)
    else:
        print("\n⚠️ Aucun ID à marquer (vérifier la récupération des IDs)")
    
    # Vérification finale
    coll_dest = db_driver[COLLECTION_DEST]
    total_dest = coll_dest.count_documents({})
    print(f"\n📊 Collection destination ({COLLECTION_DEST}): {total_dest} commentaires")
    
    # Vérifier combien sont encore à traiter
    restants = get_nouveaux_commentaires_count()
    print(f"📊 Collection source ({COLLECTION_SOURCE}): {restants} commentaires restants (traite=false)")
    
    spark.stop()
    client_driver.close()
    
    temps_total = time.time() - temps_debut
    print(f"\n⏱️  Temps total : {temps_total:.2f}s")
    print("=" * 70)
    print("🎉 ÉTAPE 1 TERMINÉE AVEC SUCCÈS !")
    print("=" * 70)


if __name__ == "__main__":
    main()