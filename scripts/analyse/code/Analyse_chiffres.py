#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/nettoyage/03_supprimer_chiffres_multinode.py
# ÉTAPE 3 — Nettoyage intelligent des chiffres
# Basé sur l'analyse réelle des données
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
COLLECTION_DEST   = "commentaires_chiffres_nettoyes"
NB_WORKERS        = 2
SPARK_MASTER      = "spark://spark-master:7077"
RAPPORT_PATH      = "Rapports/rapport_chiffres.txt"

# ============================================================
# DICTIONNAIRE DES CHIFFRES
# (basé sur l'analyse réelle des données)
# ============================================================

# ✅ GARDER — protéger ces chiffres avant suppression
PATTERNS_GARDER = [
    # Prix : 2200 دج / 9999 دينار / 2400 DA
    r'\d+[.,]?\d*\s*(دج|دينار|DA|da)',
    # Quantité : 60 ميغا / 1 جيغا / 1000 Mo
    r'\d+[.,]?\d*\s*(ميغا|ميقا|ميڨا|ميجا|جيغا|جيقا|جيڨا|جي|Mo|Go|GB|MB|KB|Ko|Gb|Mb|Kb|mega|mg|mb|gb)',
    # Durée : 15 يوم / 10 ايام / 9 اشهر / 24 ساعة
    r'\d+\s*(يوم|أيام|ايام|اشهر|أشهر|شهر|ساعة|ساعات|ثانية|ثواني|دقيقة|دقايق|سنوات)',
    # Vitesse : 600 Mbps / 50 Kbps
    r'\d+\s*(Mbps|Kbps|mbps|kbps|bps|MBPS)',
    # Pourcentage : 40% / 100%
    r'\d+\s*%',
    # Technologie : 4G / 3G / 5G / wifi6
    r'\b(5G|4G|3G|2G|4g|3g|5g|[Ww]ifi\s*\d*|[Ii]doom\s*\d+|[Ff]ibre?\s*\d*)\b',
    # Années : 2009, 2018, 2019...2030
    r'\b(19|20)\d{2}\b',
    # Heure : 18h / 24h
    r'\d+\s*h\b',
    # Mois français : 06 mois / 28 jours
    r'\d+\s*(mois|jours?|jour)',
    # Ping : ping 100
    r'(ping|PING)\s*\d+',
    # Adresse/numéro de logement : حي 236 / مسكن 165
    r'(حي|مسكن|رقم\s+الشقة)\s*\d+',
]

# ❌ SUPPRIMER — ces chiffres sont du bruit
PATTERNS_SUPPRIMER = [
    # Numéros de réclamation longs (+10 chiffres)
    r'\b\d{10,}\b',
    # Répétitions VRAIES (+5 chiffres identiques)
    # ex: 0000000, 111111 — mais PAS 9999 دينار
    r'(?<!\d)(\d)\1{5,}(?!\d)',
    # Téléphones mobiles algériens : 0551..., 0664..., 0770...
    r'\b0[5-7]\d{8}\b',
    # Téléphones fixes algériens : 021..., 023..., 027..., 028...
    r'\b0[2-4]\d{7,8}\b',
    # Numéros service : 02121
    r'\b02\d{3,4}\b',
]

# ============================================================
# FONCTIONS DISTRIBUÉES
# ============================================================

def nettoyer_chiffres_partition(partition):
    """
    Chaque Worker :
    1. Protège les chiffres utiles (prix, durée, années...)
    2. Supprime les chiffres inutiles (répétitions, téléphones, réclamations)
    3. Restaure les chiffres utiles
    4. Écrit directement dans MongoDB
    """
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    import re
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError

    # Compiler les patterns une seule fois par partition
    re_garder    = [re.compile(p, re.IGNORECASE | re.UNICODE) for p in PATTERNS_GARDER]
    re_supprimer = [re.compile(p, re.UNICODE) for p in PATTERNS_SUPPRIMER]
    re_espaces   = re.compile(r'\s+')

    def proteger_chiffres_utiles(texte):
        """Remplace les chiffres utiles par des placeholders temporaires"""
        placeholders = {}
        compteur     = [0]

        def remplacer(match):
            cle = f"PROT{compteur[0]}PROT"
            placeholders[cle] = match.group(0)
            compteur[0] += 1
            return cle

        for pattern in re_garder:
            texte = pattern.sub(remplacer, texte)

        return texte, placeholders

    def restaurer_chiffres(texte, placeholders):
        """Restaure les chiffres utiles depuis les placeholders"""
        for cle, valeur in placeholders.items():
            texte = texte.replace(cle, valeur)
        return texte

    def nettoyer_texte(texte):
        if not texte or not isinstance(texte, str):
            return texte

        # 1. Protéger les chiffres utiles
        texte, placeholders = proteger_chiffres_utiles(texte)

        # 2. Supprimer les chiffres inutiles
        for pattern in re_supprimer:
            texte = pattern.sub(' ', texte)

        # 3. Restaurer les chiffres utiles
        texte = restaurer_chiffres(texte, placeholders)

        # 4. Nettoyer les espaces
        texte = re_espaces.sub(' ', texte).strip()
        return texte if texte else None

    # Connexion MongoDB depuis le Worker
    try:
        client     = MongoClient("mongodb://mongodb_pfe:27017/", serverSelectionTimeoutMS=5000)
        db         = client["telecom_algerie"]
        collection = db["commentaires_chiffres_nettoyes"]
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

    re_garder    = [re.compile(p, re.IGNORECASE | re.UNICODE) for p in PATTERNS_GARDER]
    re_supprimer = [re.compile(p, re.UNICODE) for p in PATTERNS_SUPPRIMER]
    re_espaces   = re.compile(r'\s+')

    cas_tests = [
        # (texte, attendu_garder, attendu_supprimer)
        ("ريزوووووووو 0000000000",            "bruit",   True),
        ("تخفيض 9999 دينار جزائري",           "prix",    False),
        ("10000 دج و مقوي",                   "prix",    False),
        ("انا نخلص في 2200 دج",              "prix",    False),
        ("منذ سنة 2009",                      "année",   False),
        ("depuis août 2025",                  "année",   False),
        ("20250927181446511421 du 27 09",     "récla",   True),
        ("0664514558 رقم الهاتف",             "tél",     True),
        ("027955127 ولا عيطولي",              "fixe",    True),
        ("60 ميغا بسرعة",                     "quantité",False),
        ("السرعة 40% من السرعة",              "pourcent",False),
        ("600 Mbps سرعة",                     "vitesse", False),
        ("15 يوم بلا نت",                     "durée",   False),
    ]

    print("\n🧪 TEST DES PATTERNS :")
    print(f"{'TEXTE':<45} {'TYPE':<10} {'MODIFIÉ ?'}")
    print("-"*70)

    for texte, type_cas, devrait_changer in cas_tests:
        # Simuler le nettoyage
        placeholders = {}
        compteur     = [0]

        def remplacer(match):
            cle = f"PROT{compteur[0]}PROT"
            placeholders[cle] = match.group(0)
            compteur[0] += 1
            return cle

        texte_work = texte
        for p in re_garder:
            texte_work = p.sub(remplacer, texte_work)
        for p in re_supprimer:
            texte_work = p.sub(' ', texte_work)
        for cle, val in placeholders.items():
            texte_work = texte_work.replace(cle, val)
        texte_work = re_espaces.sub(' ', texte_work).strip()

        modifie = texte_work != texte
        statut  = "✅" if modifie == devrait_changer else "❌ ERREUR"
        print(f"{texte:<45} {type_cas:<10} {statut} → {texte_work}")

    print()

tester_patterns()

# ============================================================
# DÉBUT DU PIPELINE
# ============================================================
temps_debut_global = time.time()

print("="*70)
print("🔢 ÉTAPE 3 — NETTOYAGE INTELLIGENT DES CHIFFRES")
print("   ✅ GARDER  : prix, durée, années, quantité, vitesse, %")
print("   ❌ SUPPRIMER: répétitions, téléphones, numéros réclamation")
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

# 4. ÉCRITURE DISTRIBUÉE
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

# 5. VÉRIFICATION FINALE
print("\n🔎 VÉRIFICATION FINALE...")

repet_restantes = coll_dest.count_documents({"Commentaire_Client": {"$regex": r"(\d)\1{5,}"}})
tel_restants    = coll_dest.count_documents({"Commentaire_Client": {"$regex": r"0[5-7]\d{8}"}})
recla_restants  = coll_dest.count_documents({"Commentaire_Client": {"$regex": r"\d{10,}"}})
total_en_dest   = coll_dest.count_documents({})

succes = repet_restantes == 0 and tel_restants == 0 and recla_restants == 0

print(f"   • Documents en destination    : {total_en_dest}")
print(f"   • Documents modifiés          : {total_modifies}")
print(f"   • Répétitions restantes       : {repet_restantes}")
print(f"   • Téléphones restants         : {tel_restants}")
print(f"   • Numéros réclamation restants: {recla_restants}")
print(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")

# 6. RAPPORT
temps_fin_global = time.time()
temps_total      = temps_fin_global - temps_debut_global

rapport = f"""
{"="*70}
RAPPORT — NETTOYAGE INTELLIGENT DES CHIFFRES
{"="*70}
Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode   : Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

✅ CHIFFRES GARDÉS :
   • Prix        : 2200 دج / 9999 دينار / 2400 DA
   • Quantité    : 60 ميغا / 1 جيغا / 1000 Mo
   • Durée       : 15 يوم / 10 ايام / 24 ساعة
   • Vitesse     : 600 Mbps / 50 Kbps
   • Pourcentage : 40% / 100%
   • Technologie : 4G / 3G / 5G / wifi6
   • Années      : 2009, 2018, 2019...2030

❌ CHIFFRES SUPPRIMÉS :
   • Répétitions      : 0000000, 111111 (+5 identiques)
   • Tél. mobiles     : 0551..., 0664..., 0770...
   • Tél. fixes       : 0212..., 027..., 028...
   • Numéros récla.   : 20250927181446511421 (+10 chiffres)

⏱️  TEMPS:
   • Connexion Spark  : {temps_fin_spark - temps_debut_spark:.2f}s
   • Chargement       : {temps_fin_chargement - temps_debut_chargement:.2f}s
   • Traitement       : {temps_fin_traitement - temps_debut_traitement:.2f}s
   • TOTAL            : {temps_total:.2f}s ({total_lignes/temps_total:.0f} doc/s)

📊 RÉSULTATS:
   • Total source          : {total_lignes} documents
   • Documents modifiés    : {total_modifies}
   • Total inséré          : {total_inseres}
   • Répétitions restantes : {repet_restantes}
   • Téléphones restants   : {tel_restants}
   • Réclamations restantes: {recla_restants}
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
print(f"⏱️  Temps total          : {temps_total:.2f}s")
print(f"🚀 Vitesse              : {total_lignes/temps_total:.0f} docs/s")
print(f"📁 Collection dest.     : {DB_NAME}.{COLLECTION_DEST}")
print("="*70)
print("🎉 NETTOYAGE CHIFFRES TERMINÉ EN MODE MULTI-NODE !")

spark.stop()
client_driver.close()
print("🔌 Connexions fermées proprement")