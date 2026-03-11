#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/nettoyage/supprimer_doublons_multinode.py
# ÉTAPE — Suppression intelligente des doublons
#
# RÈGLES :
#   ❌ R1 : même texte + même jour + même source + même mod → garder 1
#   ✅ R2 : même texte + même jour + source diff            → 1 par source
#   ✅ R3 : même texte + jours différents                   → 1 par jour
#   ✅ R4 : même texte + même jour + mod diff               → 1 par mod
#   ✅ R5 : texte tronqué vs complet → garder le complet
#           (comparaison sur les 60 premiers caractères)
#
# Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

from pyspark.sql import SparkSession
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime
from collections import defaultdict
import os, time, math, json, re

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
COLLECTION_DEST   = "commentaires_sans_doublons"
NB_WORKERS        = 2
SPARK_MASTER      = "spark://spark-master:7077"
RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_suppression_doublons.txt"

# Nombre de caractères pour comparaison texte tronqué
NB_CHARS_COMPARAISON = 60

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================
def extraire_jour(date_str):
    if not date_str:
        return "inconnu"
    date_str = str(date_str).strip()
    for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except:
            continue
    return date_str[:10]

def normaliser_texte(texte, nb_chars=NB_CHARS_COMPARAISON):
    """
    Normalise le texte pour comparaison :
    - Supprime 'En voir plus' et variantes
    - Supprime espaces en trop
    - Prend les 60 premiers caractères
    """
    if not texte:
        return ""
    texte = str(texte).strip()
    texte = re.sub(r'[Ee]n voir plus\.?', '', texte)
    texte = re.sub(r'See more\.?',        '', texte)
    texte = re.sub(r'أكثر\.?',           '', texte)
    texte = re.sub(r'\s+', ' ', texte).strip()
    return texte[:nb_chars]

def appliquer_regles(copies):
    """
    Applique les règles de déduplication.
    Clé de regroupement : (texte_60chars, jour, source, modérateur)
    → 1 copie par combinaison unique
    → Garde la copie la plus LONGUE (texte complet vs tronqué)
    """
    if len(copies) <= 1:
        return copies

    for c in copies:
        c["_jour"]       = extraire_jour(str(c.get("date", "")))
        c["_texte_norm"] = normaliser_texte(c.get("Commentaire_Client", ""))

    # Grouper par (texte_60chars, jour, source, modérateur)
    groupes = defaultdict(list)
    for c in copies:
        cle = (
            c["_texte_norm"],
            c["_jour"],
            str(c.get("source",     "") or ""),
            str(c.get("moderateur", "") or "inconnu")
        )
        groupes[cle].append(c)

    copies_finales = []
    for cle, groupe in groupes.items():
        # Garder le texte le plus LONG (complet plutôt que tronqué)
        groupe_trie = sorted(
            groupe,
            key=lambda x: len(str(x.get("Commentaire_Client", ""))),
            reverse=True
        )
        copies_finales.append(groupe_trie[0])

    return copies_finales

# ============================================================
# TEST LOCAL DES RÈGLES
# ============================================================
def tester_regles():
    texte_tronque = "كي يطلقولي الفيبر برك وساهل 20 يوم ملي ركبولي كلشي من هذاك النهار وانا En voir plus"
    texte_complet = "كي يطلقولي الفيبر برك وساهل 20 يوم ملي ركبولي كلشي من هذاك النهار وانا طالع هابط حسبت روحي"

    cas_tests = [
        {
            "desc"   : "R1 — Même jour + même source + même mod → garder 1",
            "copies" : [
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:58", "moderateur": "Hiba"},
            ],
            "attendu": 1
        },
        {
            "desc"   : "R2 — Même jour + sources diff → 1 par source",
            "copies" : [
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "28/11/2025 21:38", "moderateur": "Hiba"},
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Instagram", "date": "28/11/2025 21:27", "moderateur": "Hiba"},
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Instagram", "date": "28/11/2025 21:29", "moderateur": "Hiba"},
            ],
            "attendu": 2
        },
        {
            "desc"   : "R3 — Jours différents → garder les 2",
            "copies" : [
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "28/11/2025 14:34", "moderateur": "Hiba"},
            ],
            "attendu": 2
        },
        {
            "desc"   : "R4 — Même jour + mod diff → 1 par mod",
            "copies" : [
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
                {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:58", "moderateur": "Ali"},
            ],
            "attendu": 2
        },
        {
            "desc"   : "Cas تم — 4 copies mixtes → garder 2 (1 par jour)",
            "copies" : [
                {"Commentaire_Client": "تم", "source": "Facebook", "date": "28/11/2025 14:34", "moderateur": "Hiba"},
                {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:58", "moderateur": "Hiba"},
                {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:55", "moderateur": "Hiba"},
                {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:55", "moderateur": "Hiba"},
            ],
            "attendu": 2
        },
        {
            "desc"   : "R5 — Texte tronqué vs complet → garder complet",
            "copies" : [
                {"Commentaire_Client": texte_tronque, "source": "Instagram", "date": "30/11/2025 21:27", "moderateur": "MohamedAmine"},
                {"Commentaire_Client": texte_complet, "source": "Instagram", "date": "30/11/2025 21:30", "moderateur": "MohamedAmine"},
            ],
            "attendu": 1
        },
    ]

    print("\n🧪 TEST DES RÈGLES AVANT SPARK :")
    print("-"*70)

    tous_ok = True
    for cas in cas_tests:
        copies   = [dict(c) for c in cas["copies"]]
        resultat = appliquer_regles(copies)
        ok       = len(resultat) == cas["attendu"]
        if not ok:
            tous_ok = False
        print(f"   {'✅ OK' if ok else '❌ ERREUR'} — {cas['desc']}")
        print(f"          Attendu={cas['attendu']} | Obtenu={len(resultat)}")
        if "tronqué" in cas["desc"] and resultat:
            print(f"          Texte gardé : {resultat[0].get('Commentaire_Client','')[:60]}...")

    print()
    if tous_ok:
        print("✅ TOUS LES TESTS PASSÉS — on peut lancer Spark !")
    else:
        print("❌ CERTAINS TESTS ONT ÉCHOUÉ !")
    print()
    return tous_ok

tests_ok = tester_regles()
if not tests_ok:
    exit(1)

# ============================================================
# FONCTIONS DISTRIBUÉES SPARK
# ============================================================
def lire_partition_depuis_mongo(partition_info):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client     = MongoClient("mongodb://mongodb_pfe:27017/",
                                 serverSelectionTimeoutMS=5000)
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


def deduplication_partition(partition):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    import re
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError
    from collections import defaultdict
    from datetime import datetime

    NB_CHARS = 60

    def extraire_jour_w(date_str):
        if not date_str:
            return "inconnu"
        date_str = str(date_str).strip()
        for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y",
                    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except:
                continue
        return date_str[:10]

    def normaliser_w(texte):
        if not texte:
            return ""
        texte = str(texte).strip()
        texte = re.sub(r'[Ee]n voir plus\.?', '', texte)
        texte = re.sub(r'See more\.?',        '', texte)
        texte = re.sub(r'أكثر\.?',           '', texte)
        texte = re.sub(r'\s+', ' ', texte).strip()
        return texte[:NB_CHARS]

    def appliquer_regles_w(copies):
        if len(copies) <= 1:
            return copies

        for c in copies:
            c["_jour"]  = extraire_jour_w(str(c.get("date", "")))
            c["_tnorm"] = normaliser_w(c.get("Commentaire_Client", ""))

        groupes = defaultdict(list)
        for c in copies:
            cle = (c["_tnorm"], c["_jour"],
                   str(c.get("source",     "") or ""),
                   str(c.get("moderateur", "") or "inconnu"))
            groupes[cle].append(c)

        copies_finales = []
        for cle, groupe in groupes.items():
            groupe_trie = sorted(
                groupe,
                key=lambda x: len(str(x.get("Commentaire_Client", ""))),
                reverse=True
            )
            copies_finales.append(groupe_trie[0])

        return copies_finales

    # Charger la partition
    docs_partition = list(partition)

    # Grouper par texte normalisé (60 chars)
    groupes_texte = defaultdict(list)
    for doc in docs_partition:
        texte = doc.get("Commentaire_Client", "") or ""
        cle   = normaliser_w(texte.strip())
        groupes_texte[cle].append(doc)

    # Appliquer les règles
    docs_a_garder   = []
    docs_supprimes  = 0

    for cle_texte, copies in groupes_texte.items():
        gardes = appliquer_regles_w(copies)
        for g in gardes:
            g.pop("_jour",  None)
            g.pop("_tnorm", None)
        docs_a_garder.extend(gardes)
        docs_supprimes += len(copies) - len(gardes)

    # Connexion MongoDB Worker
    try:
        client     = MongoClient("mongodb://mongodb_pfe:27017/",
                                 serverSelectionTimeoutMS=5000)
        db         = client["telecom_algerie"]
        collection = db["commentaires_sans_doublons"]
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed"}
        return

    # Écriture par batch
    batch        = []
    docs_inseres = 0

    for doc in docs_a_garder:
        batch.append(InsertOne(doc))
        docs_inseres += 1
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
        "docs_traites" : len(docs_partition),
        "docs_inseres" : docs_inseres,
        "docs_supprimes": docs_supprimes,
        "statut"       : "ok"
    }

# ============================================================
# PIPELINE SPARK
# ============================================================
temps_debut = time.time()

print("="*70)
print("🔁 SUPPRESSION INTELLIGENTE DES DOUBLONS — MULTI-NODE")
print("   ❌ R1 : même texte + même jour + même source + même mod → 1")
print("   ✅ R2 : même texte + même jour + source diff            → 1/source")
print("   ✅ R3 : même texte + jours différents                   → 1/jour")
print("   ✅ R4 : même texte + même jour + mod diff               → 1/mod")
print("   ✅ R5 : texte tronqué vs complet                        → garder complet")
print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
print("="*70)

# 1. Connexion MongoDB Driver
print("\n📂 Connexion MongoDB (Driver)...")
client_driver = MongoClient(MONGO_URI_DRIVER)
db_driver     = client_driver[DB_NAME]
coll_source   = db_driver[COLLECTION_SOURCE]
total_docs    = coll_source.count_documents({})
print(f"✅ {total_docs} documents dans la source")

# 2. Connexion Spark
print("\n⚡ Connexion au cluster Spark...")
temps_spark = time.time()

spark = SparkSession.builder \
    .appName("Suppression_Doublons_MultiNode") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"✅ Spark connecté en {time.time()-temps_spark:.2f}s")

# 3. Lecture distribuée
print("\n📥 LECTURE DISTRIBUÉE...")
docs_par_worker = math.ceil(total_docs / NB_WORKERS)
plages = [
    {"skip" : i * docs_par_worker,
     "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
    for i in range(NB_WORKERS)
]
for idx, p in enumerate(plages):
    print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

rdd_data = spark.sparkContext \
    .parallelize(plages, NB_WORKERS) \
    .mapPartitions(lire_partition_depuis_mongo)

df_spark     = spark.read.json(rdd_data.map(
    lambda d: json.dumps(
        {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
         for k, v in d.items()}
    )
))
total_lignes = df_spark.count()
print(f"✅ {total_lignes} documents chargés")

# 4. Vider destination
coll_dest = db_driver[COLLECTION_DEST]
coll_dest.delete_many({})
print("\n🧹 Collection destination vidée")

# 5. Déduplication + écriture
print("\n💾 DÉDUPLICATION + ÉCRITURE DISTRIBUÉE...")
temps_traitement = time.time()

rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(deduplication_partition)

stats           = rdd_stats.collect()
total_inseres   = sum(s.get("docs_inseres",   0) for s in stats if s.get("statut") == "ok")
total_supprimes = sum(s.get("docs_supprimes", 0) for s in stats if s.get("statut") == "ok")
erreurs         = [s for s in stats if "_erreur" in s]

print(f"✅ Traitement terminé en {time.time()-temps_traitement:.2f}s")
if erreurs:
    for e in erreurs:
        print(f"   ⚠️  {e.get('_erreur')}")

# 6. Vérification
print("\n🔎 VÉRIFICATION FINALE...")
total_en_dest = coll_dest.count_documents({})
succes        = total_en_dest == total_inseres

print(f"   • Documents source      : {total_lignes}")
print(f"   • Documents supprimés   : {total_supprimes}")
print(f"   • Documents gardés      : {total_en_dest}")
print(f"   • Taux de réduction     : {total_supprimes/total_lignes*100:.2f}%")
print(f"   {'✅ SUCCÈS !' if succes else '⚠️  Vérifier manuellement'}")

# 7. Rapport
temps_total = time.time() - temps_debut
rapport = f"""
{"="*70}
RAPPORT — SUPPRESSION INTELLIGENTE DES DOUBLONS
{"="*70}
Date       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode       : Spark 4.1.1 | Multi-Node | Workers → MongoDB direct
Collection : {DB_NAME}.{COLLECTION_SOURCE} → {COLLECTION_DEST}

RÈGLES APPLIQUÉES :
   ❌ R1 : même texte(60c) + même jour + même source + même mod → garder 1
   ✅ R2 : même texte(60c) + même jour + source diff            → 1 par source
   ✅ R3 : même texte(60c) + jours différents                   → 1 par jour
   ✅ R4 : même texte(60c) + même jour + mod diff               → 1 par modérateur
   ✅ R5 : texte tronqué vs complet → garder le plus long (complet)

RÉSULTATS :
   • Documents source          : {total_lignes}
   • Documents supprimés       : {total_supprimes}
   • Documents gardés          : {total_en_dest}
   • Taux de réduction         : {total_supprimes/total_lignes*100:.2f}%

TEMPS :
   • Total                     : {temps_total:.2f}s
   • Vitesse                   : {total_lignes/temps_total:.0f} docs/s

STOCKAGE :
   • Source      : {DB_NAME}.{COLLECTION_SOURCE}
   • Destination : {DB_NAME}.{COLLECTION_DEST}
   • Statut      : {"✅ SUCCÈS" if succes else "⚠️ VÉRIFIER"}
{"="*70}
"""

os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
    f.write(rapport)

spark.stop()
client_driver.close()

print(f"\n✅ Rapport : {RAPPORT_PATH}")
print("\n" + "="*70)
print("📊 RÉSUMÉ FINAL")
print("="*70)
print(f"   📥 Documents source     : {total_lignes}")
print(f"   ❌ Documents supprimés  : {total_supprimes}")
print(f"   ✅ Documents gardés     : {total_en_dest}")
print(f"   📊 Taux réduction       : {total_supprimes/total_lignes*100:.2f}%")
print(f"   ⏱️  Temps total          : {temps_total:.2f}s")
print(f"   📁 Destination          : {DB_NAME}.{COLLECTION_DEST}")
print("="*70)
print("🎉 SUPPRESSION DOUBLONS TERMINÉE EN MODE MULTI-NODE !")