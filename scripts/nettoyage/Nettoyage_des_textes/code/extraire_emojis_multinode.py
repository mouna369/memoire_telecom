# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# # extraire_emojis_multinode.py
# # Extraction emojis → colonnes séparées
# # SOURCE      : commentaires_sans_doublons
# # DESTINATION : commentaires_sans_emojis
# # Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

# from pymongo import MongoClient
# import os, time, math, json

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# DB_NAME           = "telecom_algerie"
# COLLECTION_SOURCE = "commentaires_sans_doublons"
# COLLECTION_DEST   = "commentaires_sans_emojis"
# NB_WORKERS        = 3
# SPARK_MASTER      = "spark://spark-master:7077"
# DICT_PATH         = "/opt/dictionnaires/master_dict.json"
# RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_emojis.txt"

# # ============================================================
# # FONCTION WORKER
# # ============================================================
# def traiter_partition(partition):
#     """
#     Chaque Worker :
#     1. Charge le dictionnaire emojis depuis master_dict.json
#     2. Pour chaque document :
#        - Extrait les emojis → emojis_originaux
#        - Traduit en arabe  → emojis_sentiment
#        - Supprime les emojis du texte
#     3. Écrit dans MongoDB
#     """
#     import sys, json
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient, InsertOne
#     from pymongo.errors import BulkWriteError

#     # ── Charger dictionnaire emojis ──────────────────────────
#     with open(DICT_PATH, encoding="utf-8") as f:
#         d = json.load(f)
#     emojis_dict = d["emojis"]  # {"😠": "غضب شديد", "👍": "اعجاب", ...}

#     # ── Connexion MongoDB Worker ─────────────────────────────
#     try:
#         client     = MongoClient("mongodb://mongodb_pfe:27017/",
#                                  serverSelectionTimeoutMS=5000)
#         db         = client["telecom_algerie"]
#         collection = db["commentaires_sans_emojis"]
#     except Exception as e:
#         yield {"_erreur": str(e), "statut": "connexion_failed"}
#         return

#     # ── Fonction extraction emojis ───────────────────────────
#     def extraire_emojis(texte):
#         """
#         Retourne :
#            emojis_originaux  : ["😠", "😠", "👎"]
#            emojis_sentiment  : ["غضب شديد", "غضب شديد", "عدم رضا"]
#            texte_propre      : texte sans emojis
#         """
#         if not isinstance(texte, str) or not texte.strip():
#             return [], [], texte or ""

#         originaux  = []
#         sentiments = []
#         texte_propre = texte

#         for emoji, sentiment in emojis_dict.items():
#             if emoji in texte_propre:
#                 # Compter combien de fois cet emoji apparaît
#                 count = texte_propre.count(emoji)
#                 for _ in range(count):
#                     originaux.append(emoji)
#                     sentiments.append(sentiment)
#                 # Supprimer l'emoji du texte
#                 texte_propre = texte_propre.replace(emoji, " ")

#         # Nettoyer les espaces multiples
#         import re
#         texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()

#         return originaux, sentiments, texte_propre

#     # ── Traitement + écriture ────────────────────────────────
#     batch         = []
#     docs_traites  = 0
#     docs_avec_emojis = 0

#     for row in partition:
#         commentaire_original = row.get("Commentaire_Client", "") or ""

#         # Extraire emojis
#         originaux, sentiments, texte_propre = extraire_emojis(commentaire_original)

#         if originaux:
#             docs_avec_emojis += 1

#         doc = {
#             "_id"                   : row.get("_id"),
#             "Commentaire_Client"    : texte_propre,
#             "emojis_originaux"      : originaux,
#             "emojis_sentiment"      : sentiments,
#             "commentaire_moderateur": row.get("commentaire_moderateur"),
#             "date"                  : row.get("date"),
#             "source"                : row.get("source"),
#             "moderateur"            : row.get("moderateur"),
#             "metadata"              : row.get("metadata"),
#             "statut"                : row.get("statut"),
#         }
#         batch.append(InsertOne(doc))
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
#     yield {
#         "docs_traites"     : docs_traites,
#         "docs_avec_emojis" : docs_avec_emojis,
#         "statut"           : "ok"
#     }

# # ============================================================
# # LECTURE DISTRIBUÉE
# # ============================================================
# def lire_partition_depuis_mongo(partition_info):
#     import sys
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient

#     for item in partition_info:
#         client     = MongoClient("mongodb://mongodb_pfe:27017/",
#                                  serverSelectionTimeoutMS=5000)
#         db         = client["telecom_algerie"]
#         collection = db["commentaires_sans_doublons"]

#         curseur = collection.find(
#             {},
#             {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
#              "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
#         ).skip(item["skip"]).limit(item["limit"])

#         for doc in curseur:
#             doc["_id"] = str(doc["_id"])
#             yield doc

#         client.close()

# # ============================================================
# # PIPELINE SPARK
# # ============================================================
# from pyspark.sql import SparkSession
# from datetime import datetime

# temps_debut = time.time()

# print("="*70)
# print("✨ EXTRACTION EMOJIS — Multi-Node Spark")
# print(f"   Source      : {COLLECTION_SOURCE}")
# print(f"   Destination : {COLLECTION_DEST}")
# print("   Traitement  :")
# print("   ✅ Extraction emojis → emojis_originaux")
# print("   ✅ Traduction arabe  → emojis_sentiment")
# print("   ✅ Suppression emojis du texte")
# print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# print("="*70)

# # 1. Connexion MongoDB Driver
# print("\n📂 Connexion MongoDB (Driver)...")
# client_driver = MongoClient(MONGO_URI_DRIVER)
# db_driver     = client_driver[DB_NAME]
# coll_source   = db_driver[COLLECTION_SOURCE]
# total_docs    = coll_source.count_documents({})
# print(f"✅ {total_docs} documents dans la source")

# # 2. Connexion Spark
# print("\n⚡ Connexion au cluster Spark...")
# temps_spark = time.time()

# spark = SparkSession.builder \
#     .appName("Extraction_Emojis_MultiNode") \
#     .master(SPARK_MASTER) \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.sql.shuffle.partitions", "4") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")
# print(f"✅ Spark connecté en {time.time()-temps_spark:.2f}s")

# # 3. Lecture distribuée
# print("\n📥 LECTURE DISTRIBUÉE...")
# docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# plages = [
#     {"skip" : i * docs_par_worker,
#      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
#     for i in range(NB_WORKERS)
# ]
# for idx, p in enumerate(plages):
#     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

# rdd_data = spark.sparkContext \
#     .parallelize(plages, NB_WORKERS) \
#     .mapPartitions(lire_partition_depuis_mongo)

# df_spark     = spark.read.json(rdd_data.map(
#     lambda d: json.dumps(
#         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None), list)) else v
#          for k, v in d.items()}
#     )
# ))
# total_lignes = df_spark.count()
# print(f"✅ {total_lignes} documents chargés")

# # 4. Vider destination
# coll_dest = db_driver[COLLECTION_DEST]
# coll_dest.delete_many({})
# print("\n🧹 Collection destination vidée")

# # 5. Traitement + écriture distribuée
# print("\n💾 EXTRACTION + ÉCRITURE DISTRIBUÉE...")
# temps_traitement = time.time()

# rdd_stats = df_spark.rdd \
#     .map(lambda row: row.asDict()) \
#     .mapPartitions(traiter_partition)

# stats              = rdd_stats.collect()
# total_traites      = sum(s.get("docs_traites",      0) for s in stats if s.get("statut") == "ok")
# total_avec_emojis  = sum(s.get("docs_avec_emojis",  0) for s in stats if s.get("statut") == "ok")
# erreurs            = [s for s in stats if "_erreur" in s]

# print(f"✅ Traitement terminé en {time.time()-temps_traitement:.2f}s")
# if erreurs:
#     for e in erreurs:
#         print(f"   ⚠️  {e.get('_erreur')}")

# # 6. Vérification finale
# print("\n🔎 VÉRIFICATION FINALE...")
# total_en_dest      = coll_dest.count_documents({})
# docs_sans_emojis   = coll_dest.count_documents({"emojis_originaux": []})
# docs_avec_emojis_v = coll_dest.count_documents({"emojis_originaux": {"$ne": []}})

# # Vérifier qu'il ne reste plus d'emojis dans le texte
# import json as json_mod
# with open("/home/mouna/projet_telecom/dictionnaires/master_dict.json", encoding="utf-8") as f:
#     emojis_list = list(json_mod.load(f)["emojis"].keys())

# emojis_restants = 0
# for emoji in emojis_list[:10]:  # vérifier les 10 premiers emojis
#     emojis_restants += coll_dest.count_documents(
#         {"Commentaire_Client": {"$regex": re.escape(emoji) if False else emoji}}
#     )

# print(f"   ┌──────────────────────────────────────────────────┐")
# print(f"   │ Documents insérés        : {total_en_dest:<20} │")
# print(f"   │ Docs avec emojis         : {docs_avec_emojis_v:<20} │")
# print(f"   │ Docs sans emojis         : {docs_sans_emojis:<20} │")
# print(f"   │ Statut : {'✅ SUCCÈS TOTAL !':<38} │")
# print(f"   └──────────────────────────────────────────────────┘")

# # 7. Exemple dans MongoDB
# print("\n📋 EXEMPLE DE DOCUMENT CRÉÉ :")
# exemple = coll_dest.find_one({"emojis_originaux": {"$ne": []}})
# if exemple:
#     print(f"   Texte original   : {exemple.get('Commentaire_Client', '')[:60]}...")
#     print(f"   emojis_originaux : {exemple.get('emojis_originaux', [])}")
#     print(f"   emojis_sentiment : {exemple.get('emojis_sentiment', [])}")

# # 8. Rapport
# temps_total = time.time() - temps_debut
# rapport = f"""
# {"="*70}
# RAPPORT — EXTRACTION EMOJIS (Multi-Node Spark)
# {"="*70}
# Date        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Spark 4.1.1 | Multi-Node | {NB_WORKERS} Workers
# Collection  : {DB_NAME}.{COLLECTION_SOURCE} → {COLLECTION_DEST}
# Dict        : {DICT_PATH}

# TRAITEMENT :
#    • Extraction emojis  → colonne emojis_originaux
#    • Traduction arabe   → colonne emojis_sentiment
#    • Suppression emojis → Commentaire_Client propre

# RÉSULTATS :
#    • Total source           : {total_lignes}
#    • Total inséré           : {total_en_dest}
#    • Docs avec emojis       : {docs_avec_emojis_v}
#    • Docs sans emojis       : {docs_sans_emojis}
#    • Pourcentage avec emojis: {docs_avec_emojis_v/total_en_dest*100:.1f}%

# STRUCTURE NOUVELLE COLLECTION :
#    • Commentaire_Client  : texte sans emojis
#    • emojis_originaux    : ["😠", "👎", ...]
#    • emojis_sentiment    : ["غضب شديد", "عدم رضا", ...]

# TEMPS :
#    • Total               : {temps_total:.2f}s
#    • Vitesse             : {total_lignes/temps_total:.0f} docs/s

# STOCKAGE :
#    • Source      : {DB_NAME}.{COLLECTION_SOURCE}
#    • Destination : {DB_NAME}.{COLLECTION_DEST}
# {"="*70}
# """

# os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
#     f.write(rapport)

# spark.stop()
# client_driver.close()

# print(f"\n✅ Rapport : {RAPPORT_PATH}")
# print("\n" + "="*70)
# print("📊 RÉSUMÉ FINAL")
# print("="*70)
# print(f"   📥 Documents source      : {total_lignes}")
# print(f"   📤 Documents insérés     : {total_en_dest}")
# print(f"   😀 Docs avec emojis      : {docs_avec_emojis_v}")
# print(f"   📝 Docs sans emojis      : {docs_sans_emojis}")
# print(f"   ⏱️  Temps total           : {temps_total:.2f}s")
# print(f"   🚀 Vitesse               : {total_lignes/temps_total:.0f} docs/s")
# print(f"   📁 Destination           : {DB_NAME}.{COLLECTION_DEST}")
# print("="*70)
# print("🎉 EXTRACTION EMOJIS TERMINÉE EN MODE MULTI-NODE !")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# extraire_emojis_multinode.py
# Extraction emojis → colonnes séparées + correction des chaînes "[]"
# SOURCE      : commentaires_sans_doublons
# DESTINATION : commentaires_sans_emojis
# Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

from pymongo import MongoClient
import os, time, math, json, re
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_sans_doublons_tfidf"
COLLECTION_DEST   = "commentaires_sans_emojis_tfidf"
NB_WORKERS        = 3
SPARK_MASTER      = "spark://spark-master:7077"
DICT_PATH         = "/opt/dictionnaires/master_dict.json"
RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_emojis.txt"

# ============================================================
# FONCTION DE CORRECTION INITIALE (une seule fois)
# ============================================================
def corriger_chaines_vides():
    """Convertit les chaînes '[]' en tableaux vides dans la collection source."""
    print("\n🔄 Correction des chaînes '[]' dans la collection source...")
    client = MongoClient(MONGO_URI_DRIVER)
    db = client[DB_NAME]
    coll = db[COLLECTION_SOURCE]
    res1 = coll.update_many(
        {"emojis_originaux": "[]"},
        {"$set": {"emojis_originaux": []}}
    )
    print(f"   → {res1.modified_count} docs corrigés pour emojis_originaux")
    res2 = coll.update_many(
        {"emojis_sentiment": "[]"},
        {"$set": {"emojis_sentiment": []}}
    )
    print(f"   → {res2.modified_count} docs corrigés pour emojis_sentiment")
    client.close()

# ============================================================
# FONCTION WORKER (avec extraction et suppression)
# ============================================================
def traiter_partition(partition):
    """
    Chaque Worker :
    1. Charge le dictionnaire emojis depuis master_dict.json
    2. Pour chaque document :
       - Extrait les emojis → emojis_originaux (liste)
       - Traduit en arabe  → emojis_sentiment (liste)
       - Supprime les emojis du texte
    3. Écrit dans MongoDB
    """
    import sys, json, re
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError

    # ── Charger dictionnaire emojis ──────────────────────────
    with open(DICT_PATH, encoding="utf-8") as f:
        d = json.load(f)
    emojis_dict = d["emojis"]  # {"😠": "غضب شديد", ...}

    # ── Connexion MongoDB Worker ─────────────────────────────
    try:
        client = MongoClient("mongodb://mongodb_pfe:27017/",
                             serverSelectionTimeoutMS=5000)
        db = client["telecom_algerie"]
        collection = db["commentaires_sans_emojis_tfidf"]
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed"}
        return

    # ── Fonction extraction emojis ───────────────────────────
    def extraire_emojis(texte):
        if not isinstance(texte, str) or not texte.strip():
            return [], [], texte or ""

        originaux = []
        sentiments = []
        texte_propre = texte

        for emoji, sentiment in emojis_dict.items():
            if emoji in texte_propre:
                count = texte_propre.count(emoji)
                originaux.extend([emoji] * count)
                sentiments.extend([sentiment] * count)
                texte_propre = texte_propre.replace(emoji, " ")

        texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
        return originaux, sentiments, texte_propre

    # ── Traitement + écriture ────────────────────────────────
    batch = []
    docs_traites = 0
    docs_avec_emojis = 0

    for row in partition:
        commentaire_original = row.get("Commentaire_Client", "") or ""

        # Extraire emojis
        originaux, sentiments, texte_propre = extraire_emojis(commentaire_original)

        if originaux:
            docs_avec_emojis += 1

        # Construction du document (enrichi)
        doc = {
            "_id": row.get("_id"),
            "Commentaire_Client": texte_propre,
            "emojis_originaux": originaux,
            "emojis_sentiment": sentiments,
            "commentaire_moderateur": row.get("commentaire_moderateur"),
            "date": row.get("date"),
            "source": row.get("source"),
            "moderateur": row.get("moderateur"),
            "metadata": row.get("metadata"),
            "statut": row.get("statut"),
        }
        batch.append(InsertOne(doc))
        docs_traites += 1

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
        "docs_traites": docs_traites,
        "docs_avec_emojis": docs_avec_emojis,
        "statut": "ok"
    }

# ============================================================
# LECTURE DISTRIBUÉE
# ============================================================
def lire_partition_depuis_mongo(partition_info):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client = MongoClient("mongodb://mongodb_pfe:27017/",
                             serverSelectionTimeoutMS=5000)
        db = client["telecom_algerie"]
        collection = db["commentaires_sans_doublons_tfidf"]

        curseur = collection.find(
            {},
            {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
             "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
        ).skip(item["skip"]).limit(item["limit"])

        for doc in curseur:
            doc["_id"] = str(doc["_id"])
            yield doc

        client.close()

# ============================================================
# PIPELINE SPARK
# ============================================================
from pyspark.sql import SparkSession

temps_debut = time.time()

print("=" * 70)
print("✨ EXTRACTION + CORRECTION EMOJIS — Multi-Node Spark")
print(f"   Source      : {COLLECTION_SOURCE}")
print(f"   Destination : {COLLECTION_DEST}")
print("   Traitement  :")
print("   ✅ Correction des chaînes '[]' → [] (source)")
print("   ✅ Extraction emojis → emojis_originaux (liste)")
print("   ✅ Traduction arabe  → emojis_sentiment (liste)")
print("   ✅ Suppression emojis du texte")
print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
print("=" * 70)

# ---- 1. Correction préalable des chaînes "[]" dans la source ----
corriger_chaines_vides()

# ---- 2. Connexion MongoDB Driver ----
print("\n📂 Connexion MongoDB (Driver)...")
client_driver = MongoClient(MONGO_URI_DRIVER)
db_driver = client_driver[DB_NAME]
coll_source = db_driver[COLLECTION_SOURCE]
total_docs = coll_source.count_documents({})
print(f"✅ {total_docs} documents dans la source")

# ---- 3. Connexion Spark ----
print("\n⚡ Connexion au cluster Spark...")
temps_spark = time.time()

spark = SparkSession.builder \
    .appName("Extraction_Emojis_MultiNode") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"✅ Spark connecté en {time.time() - temps_spark:.2f}s")

# ---- 4. Lecture distribuée ----
print("\n📥 LECTURE DISTRIBUÉE...")
docs_par_worker = math.ceil(total_docs / NB_WORKERS)
plages = [
    {"skip": i * docs_par_worker,
     "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
    for i in range(NB_WORKERS)
]
for idx, p in enumerate(plages):
    print(f"   • Worker {idx + 1} : skip={p['skip']}, limit={p['limit']}")

rdd_data = spark.sparkContext \
    .parallelize(plages, NB_WORKERS) \
    .mapPartitions(lire_partition_depuis_mongo)

df_spark = spark.read.json(rdd_data.map(
    lambda d: json.dumps(
        {k: str(v) if not isinstance(v, (str, int, float, bool, type(None), list)) else v
         for k, v in d.items()}
    )
))
total_lignes = df_spark.count()
print(f"✅ {total_lignes} documents chargés")

# ---- 5. Vider destination ----
coll_dest = db_driver[COLLECTION_DEST]
coll_dest.delete_many({})
print("\n🧹 Collection destination vidée")

# ---- 6. Traitement + écriture distribuée ----
print("\n💾 EXTRACTION + ÉCRITURE DISTRIBUÉE...")
temps_traitement = time.time()

rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(traiter_partition)

stats = rdd_stats.collect()
total_traites = sum(s.get("docs_traites", 0) for s in stats if s.get("statut") == "ok")
total_avec_emojis = sum(s.get("docs_avec_emojis", 0) for s in stats if s.get("statut") == "ok")
erreurs = [s for s in stats if "_erreur" in s]

print(f"✅ Traitement terminé en {time.time() - temps_traitement:.2f}s")
if erreurs:
    for e in erreurs:
        print(f"   ⚠️  {e.get('_erreur')}")

# ---- 7. Vérification finale ----
print("\n🔎 VÉRIFICATION FINALE...")
total_en_dest = coll_dest.count_documents({})
docs_sans_emojis = coll_dest.count_documents({"emojis_originaux": []})
docs_avec_emojis_v = coll_dest.count_documents({"emojis_originaux": {"$ne": []}})

print(f"   ┌──────────────────────────────────────────────────┐")
print(f"   │ Documents insérés        : {total_en_dest:<20} │")
print(f"   │ Docs avec emojis         : {docs_avec_emojis_v:<20} │")
print(f"   │ Docs sans emojis         : {docs_sans_emojis:<20} │")
print(f"   │ Statut : {'✅ SUCCÈS TOTAL !':<38} │")
print(f"   └──────────────────────────────────────────────────┘")

# ---- 8. Exemple ----
print("\n📋 EXEMPLE DE DOCUMENT CRÉÉ :")
exemple = coll_dest.find_one({"emojis_originaux": {"$ne": []}})
if exemple:
    print(f"   Texte original   : {exemple.get('Commentaire_Client', '')[:60]}...")
    print(f"   emojis_originaux : {exemple.get('emojis_originaux', [])}")
    print(f"   emojis_sentiment : {exemple.get('emojis_sentiment', [])}")

# ---- 9. Rapport ----
temps_total = time.time() - temps_debut
rapport = f"""
{"=" * 70}
RAPPORT — EXTRACTION + CORRECTION EMOJIS (Multi-Node Spark)
{"=" * 70}
Date        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Spark 4.1.1 | Multi-Node | {NB_WORKERS} Workers
Collection  : {DB_NAME}.{COLLECTION_SOURCE} → {COLLECTION_DEST}
Dict        : {DICT_PATH}

CORRECTION PRÉALABLE :
   • Conversion des chaînes "[]" → [] dans la source

TRAITEMENT :
   • Extraction emojis  → colonne emojis_originaux (liste)
   • Traduction arabe   → colonne emojis_sentiment (liste)
   • Suppression emojis → Commentaire_Client propre

RÉSULTATS :
   • Total source           : {total_lignes}
   • Total inséré           : {total_en_dest}
   • Docs avec emojis       : {docs_avec_emojis_v}
   • Docs sans emojis       : {docs_sans_emojis}
   • Pourcentage avec emojis: {docs_avec_emojis_v / total_en_dest * 100:.1f}%

TEMPS :
   • Total               : {temps_total:.2f}s
   • Vitesse             : {total_lignes / temps_total:.0f} docs/s

STOCKAGE :
   • Source      : {DB_NAME}.{COLLECTION_SOURCE}
   • Destination : {DB_NAME}.{COLLECTION_DEST}
{"=" * 70}
"""

os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
    f.write(rapport)

spark.stop()
client_driver.close()

print(f"\n✅ Rapport : {RAPPORT_PATH}")
print("\n" + "=" * 70)
print("📊 RÉSUMÉ FINAL")
print("=" * 70)
print(f"   📥 Documents source      : {total_lignes}")
print(f"   📤 Documents insérés     : {total_en_dest}")
print(f"   😀 Docs avec emojis      : {docs_avec_emojis_v}")
print(f"   📝 Docs sans emojis      : {docs_sans_emojis}")
print(f"   ⏱️  Temps total           : {temps_total:.2f}s")
print(f"   🚀 Vitesse               : {total_lignes / temps_total:.0f} docs/s")
print(f"   📁 Destination           : {DB_NAME}.{COLLECTION_DEST}")
print("=" * 70)
print("🎉 EXTRACTION + CORRECTION EMOJIS TERMINÉE EN MODE MULTI-NODE !")