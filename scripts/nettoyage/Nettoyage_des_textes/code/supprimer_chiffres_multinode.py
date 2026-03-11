#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/nettoyage/03_supprimer_chiffres_multinode.py
# ÉTAPE 3 — Nettoyage des chiffres (cas certains uniquement)
# ✅ GARDER  : prix, durée, quantité, années, ambigus → LLM gère après
# ❌ SUPPRIMER : téléphones, réclamations, répétitions (cas certains)
# Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import sum as spark_sum, count as spark_count
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime
import os, time, math

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
COLLECTION_DEST   = "commentaires_sans_chiffres_certains"
NB_WORKERS        = 2
SPARK_MASTER      = "spark://spark-master:7077"
RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports//rapport_chiffres.txt"

# ============================================================
# RÈGLES DE NETTOYAGE
# ============================================================
#
# ❌ SUPPRIMER — cas certains (aucune valeur NLP)
#    • Téléphones mobiles  : 0664514558 / 0551383392
#    • Téléphones fixes    : 027955127  / 042057067
#    • Numéros réclamation : 20250927181446511421 (+10 chiffres)
#    • Répétitions pures   : 0000000000 / 111111
#
# ✅ GARDER — tout le reste
#    • Prix ambigus    : 1500 / 7000 / 14000
#    • Chiffres seuls  : 1000 / 3000
#    • Années          : 2009 / 2022 / 2025
#    → Le LLM NLP comprendra le contexte après !
#
# ============================================================

PATTERNS_SUPPRIMER = [
    # Numéros réclamation (+10 chiffres consécutifs)
    # ex: 20250927181446511421
    r'\b\d{10,}\b',

    # Téléphones mobiles algériens
    # ex: 0664514558 / 0551383392 / 0770475813
    r'\b0[5-7]\d{8}\b',

    # Téléphones fixes algériens
    # ex: 027955127 / 042057067
    r'\b0[2-4]\d{7,8}\b',

    # Répétitions pures (+5 chiffres identiques)
    # ex: 0000000000 / 111111
    # ⚠️ NE PAS supprimer : 9999 دينار (4 chiffres seulement)
    r'(?<!\d)(\d)\1{5,}(?!\d)',
]

# ============================================================
# FONCTIONS DISTRIBUÉES
# ============================================================

def nettoyer_chiffres_partition(partition):
    """
    Chaque Worker :
    1. Supprime uniquement les cas certains
    2. Garde tout le reste pour le LLM NLP
    3. Écrit directement dans MongoDB
    """
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    import re
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError

    # Compiler les patterns une seule fois par partition
    re_supprimer = [re.compile(p, re.UNICODE) for p in PATTERNS_SUPPRIMER]
    re_espaces   = re.compile(r'\s+')

    def nettoyer_texte(texte):
        if not texte or not isinstance(texte, str):
            return texte

        # Supprimer uniquement les cas certains
        for pattern in re_supprimer:
            texte = pattern.sub(' ', texte)

        # Nettoyer les espaces
        texte = re_espaces.sub(' ', texte).strip()
        return texte if texte else None

    # Connexion MongoDB depuis le Worker
    try:
        client     = MongoClient("mongodb://mongodb_pfe:27017/", serverSelectionTimeoutMS=5000)
        db         = client["telecom_algerie"]
        collection = db[COLLECTION_DEST]
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed"}
        return

    batch         = []
    docs_traites  = 0
    docs_modifies = 0

    for row in partition:
        commentaire_original = row.get("Commentaire_Client", "")
        commentaire_propre   = nettoyer_texte(commentaire_original)

        if commentaire_propre != commentaire_original:
            docs_modifies += 1

        doc = {
            "_id"                    : row.get("_id"),
            "Commentaire_Client"     : commentaire_propre,
            "commentaire_moderateur" : nettoyer_texte(row.get("commentaire_moderateur")),
            "date"                   : row.get("date"),
            "source"                 : row.get("source"),
            "moderateur"             : row.get("moderateur"),
            "metadata"               : row.get("metadata"),
            "statut"                 : row.get("statut"),
        }
        doc_propre = {k: (None if v != v else v) for k, v in doc.items()}
        batch.append(InsertOne(doc_propre))
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
    yield {"docs_traites": docs_traites, "docs_modifies": docs_modifies, "statut": "ok"}


def lire_partition_depuis_mongo(partition_info):
    """Chaque Worker lit sa portion depuis MongoDB directement"""
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client     = MongoClient("mongodb://mongodb_pfe:27017/", serverSelectionTimeoutMS=5000)
        db         = client["telecom_algerie"]
        collection = db["commentaires_sans_urls_arobase"]

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
# TEST LOCAL — vérifier les patterns avant Spark
# ============================================================
def tester_patterns():
    import re

    re_supprimer = [re.compile(p, re.UNICODE) for p in PATTERNS_SUPPRIMER]
    re_espaces   = re.compile(r'\s+')

    def nettoyer(texte):
        for p in re_supprimer:
            texte = p.sub(' ', texte)
        return re_espaces.sub(' ', texte).strip()

    cas_tests = [
        # (texte,                                        doit_changer, description)
        ("ريزوووووووو 0000000000",                        True,  "répétition pure"),
        ("تخفيض 9999 دينار جزائري",                      False, "prix — 4 chiffres seulement"),
        ("10000 دج و مقوي",                              False, "prix — pas répétition"),
        ("انا نخلص في 2200 دج",                          False, "prix"),
        ("منذ سنة 2009",                                  False, "année"),
        ("20250927181446511421 du 27 09",                True,  "numéro réclamation"),
        ("0664514558 رقم الهاتف",                        True,  "tél. mobile"),
        ("027955127 ولا عيطولي",                         True,  "tél. fixe"),
        ("60 ميغا بسرعة",                                False, "quantité"),
        ("1500 ندخل رقم الهاتف",                         False, "ambigu → laisser pour LLM"),
        ("7000 الف فيه",                                 False, "ambigu → laisser pour LLM"),
        ("1000 الشهر بمعنى 3000",                        False, "ambigu → laisser pour LLM"),
        ("0000 مكالمة",                                  False, "4 répétitions seulement → garder"),
        ("الاتصالات بالهاتف الثابت 000000 مكالمة",       True,  "6 répétitions → supprimer"),
    ]

    print("\n🧪 TEST DES PATTERNS AVANT SPARK :")
    print(f"{'TEXTE':<50} {'RÉSULTAT':<12} {'STATUT'}")
    print("-"*80)

    tous_ok = True
    for texte, doit_changer, description in cas_tests:
        resultat   = nettoyer(texte)
        a_change   = resultat != texte
        ok         = a_change == doit_changer
        statut     = "✅ OK" if ok else "❌ ERREUR"
        if not ok:
            tous_ok = False
        print(f"{texte:<50} {statut:<12} {description}")
        if not ok:
            print(f"   → Attendu  : {'modifié' if doit_changer else 'inchangé'}")
            print(f"   → Obtenu   : {resultat}")

    print()
    if tous_ok:
        print("✅ TOUS LES TESTS PASSÉS — on peut lancer Spark !")
    else:
        print("❌ CERTAINS TESTS ONT ÉCHOUÉ — vérifier les patterns !")
    print()
    return tous_ok

# Lancer les tests
tests_ok = tester_patterns()
if not tests_ok:
    print("⚠️  Corrige les patterns avant de continuer.")
    exit(1)

# ============================================================
# DÉBUT DU PIPELINE
# ============================================================
temps_debut_global = time.time()

print("="*70)
print("🔢 ÉTAPE 3 — NETTOYAGE DES CHIFFRES (CAS CERTAINS)")
print("   ❌ SUPPRIMER : téléphones, réclamations, répétitions")
print("   ✅ GARDER    : tout le reste → LLM NLP gère après")
print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
print("="*70)

# 1. CONNEXION MONGODB DRIVER
print("\n📂 Connexion MongoDB (Driver)...")
try:
    client_driver = MongoClient(MONGO_URI_DRIVER)
    db_driver     = client_driver[DB_NAME]
    coll_source   = db_driver[COLLECTION_SOURCE]
    total_docs    = coll_source.count_documents({})
    print(f"✅ MongoDB connecté — {total_docs} documents dans la source")
except Exception as e:
    print(f"❌ Erreur MongoDB : {e}")
    exit(1)

# 2. CONNEXION SPARK
print("\n⚡ Connexion au cluster Spark...")
temps_debut_spark = time.time()

spark = SparkSession.builder \
    .appName("Nettoyage_Chiffres_MultiNode") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
temps_fin_spark = time.time()
print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")

# 3. LECTURE DISTRIBUÉE
print("\n📥 LECTURE DISTRIBUÉE...")
temps_debut_chargement = time.time()

docs_par_worker = math.ceil(total_docs / NB_WORKERS)
plages = [
    {"skip" : i * docs_par_worker,
     "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
    for i in range(NB_WORKERS)
]

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

# 4. ANALYSE AVANT
print("\n🔎 ANALYSE AVANT...")
temps_debut_analyse = time.time()

DETECT_TEL     = r'0[5-7]\d{8}|0[2-4]\d{7,8}'
DETECT_RECLA   = r'\d{10,}'
DETECT_REPET   = r'(\d)\1{5,}'

stats_avant = df_spark.agg(
    spark_count("*").alias("total"),
    spark_sum(when(col("Commentaire_Client").rlike(DETECT_TEL),   1).otherwise(0)).alias("avec_tel"),
    spark_sum(when(col("Commentaire_Client").rlike(DETECT_RECLA), 1).otherwise(0)).alias("avec_recla"),
    spark_sum(when(col("Commentaire_Client").rlike(DETECT_REPET), 1).otherwise(0)).alias("avec_repet"),
).collect()[0]

total       = stats_avant["total"]
avec_tel    = int(stats_avant["avec_tel"]   or 0)
avec_recla  = int(stats_avant["avec_recla"] or 0)
avec_repet  = int(stats_avant["avec_repet"] or 0)

temps_fin_analyse = time.time()
print(f"\n📊 AVANT (en {temps_fin_analyse - temps_debut_analyse:.2f}s):")
print(f"   ┌─────────────────────────────────────────┐")
print(f"   │ Total documents          : {total:<14} │")
print(f"   │ Avec téléphones          : {avec_tel:<14} │")
print(f"   │ Avec numéros réclamation : {avec_recla:<14} │")
print(f"   │ Avec répétitions         : {avec_repet:<14} │")
print(f"   └─────────────────────────────────────────┘")

# 5. ÉCRITURE DISTRIBUÉE
print("\n💾 NETTOYAGE + ÉCRITURE DISTRIBUÉE...")
temps_debut_traitement = time.time()

coll_dest = db_driver[COLLECTION_DEST]
coll_dest.delete_many({})
print("   🧹 Collection destination vidée")

print("   📤 Workers en train de nettoyer et écrire...")
rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(nettoyer_chiffres_partition)

stats_ecriture = rdd_stats.collect()
total_inseres  = sum(s.get("docs_traites", 0)  for s in stats_ecriture if s.get("statut") == "ok")
total_modifies = sum(s.get("docs_modifies", 0) for s in stats_ecriture if s.get("statut") == "ok")
erreurs        = [s for s in stats_ecriture if "_erreur" in s]

temps_fin_traitement = time.time()
print(f"✅ Traitement terminé en {temps_fin_traitement - temps_debut_traitement:.2f}s")

if erreurs:
    for e in erreurs:
        print(f"   ⚠️  {e.get('_erreur')}")

# 6. VÉRIFICATION FINALE
print("\n🔎 VÉRIFICATION FINALE...")

tel_restants   = coll_dest.count_documents({"Commentaire_Client": {"$regex": r"0[5-7]\d{8}"}})
recla_restants = coll_dest.count_documents({"Commentaire_Client": {"$regex": r"\d{10,}"}})
repet_restants = coll_dest.count_documents({"Commentaire_Client": {"$regex": r"(\d)\1{5,}"}})
total_en_dest  = coll_dest.count_documents({})

succes = tel_restants == 0 and recla_restants == 0 and repet_restants == 0

print(f"   • Documents en destination       : {total_en_dest}")
print(f"   • Documents modifiés             : {total_modifies}")
print(f"   • Téléphones restants            : {tel_restants}")
print(f"   • Numéros réclamation restants   : {recla_restants}")
print(f"   • Répétitions restantes          : {repet_restants}")
print(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")

# 7. RAPPORT
temps_fin_global = time.time()
temps_total      = temps_fin_global - temps_debut_global

rapport = f"""
{"="*70}
RAPPORT — NETTOYAGE DES CHIFFRES (CAS CERTAINS)
{"="*70}
Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode   : Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

❌ SUPPRIMÉS (cas certains) :
   • Téléphones mobiles   : 0664514558 / 0551383392
   • Téléphones fixes     : 027955127  / 042057067
   • Numéros réclamation  : 20250927181446511421 (+10 chiffres)
   • Répétitions pures    : 0000000000 (+5 chiffres identiques)

✅ GARDÉS (pour le LLM NLP) :
   • Prix ambigus         : 1500 / 7000 / 14000
   • Chiffres seuls       : 1000 / 3000
   • Années               : 2009 / 2022 / 2025
   • Prix clairs          : 2200 دج / 9999 دينار
   • Quantités            : 60 ميغا / 1 جيغا

⏱️  TEMPS:
   • Connexion Spark  : {temps_fin_spark - temps_debut_spark:.2f}s
   • Chargement       : {temps_fin_chargement - temps_debut_chargement:.2f}s
   • Analyse          : {temps_fin_analyse - temps_debut_analyse:.2f}s
   • Traitement       : {temps_fin_traitement - temps_debut_traitement:.2f}s
   • TOTAL            : {temps_total:.2f}s ({total_lignes/temps_total:.0f} doc/s)

📊 RÉSULTATS:
   • Total source          : {total_lignes} documents
   • Documents modifiés    : {total_modifies}
   • Total inséré          : {total_inseres}
   • Téléphones restants   : {tel_restants}
   • Réclamations restantes: {recla_restants}
   • Répétitions restantes : {repet_restants}
   • Statut                : {"✅ SUCCÈS" if succes else "⚠️ INCOMPLET"}

📁 STOCKAGE:
   • Source      : {DB_NAME}.{COLLECTION_SOURCE}
   • Destination : {DB_NAME}.{COLLECTION_DEST}
"""

os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
    f.write(rapport)
print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

print("\n" + "="*70)
print("📊 RÉSUMÉ FINAL")
print("="*70)
print(f"📥 Documents source     : {total_lignes}")
print(f"📤 Documents insérés    : {total_inseres}")
print(f"✏️  Documents modifiés   : {total_modifies}")
print(f"🗑️  Téléphones supprimés : {avec_tel}")
print(f"🗑️  Réclamations supp.   : {avec_recla}")
print(f"🗑️  Répétitions supp.    : {avec_repet}")
print(f"⏱️  Temps total          : {temps_total:.2f}s")
print(f"🚀 Vitesse              : {total_lignes/temps_total:.0f} docs/s")
print(f"📁 Collection dest.     : {DB_NAME}.{COLLECTION_DEST}")
print("="*70)
print("🎉 NETTOYAGE CHIFFRES TERMINÉ EN MODE MULTI-NODE !")

spark.stop()
client_driver.close()
print("🔌 Connexions fermées proprement")