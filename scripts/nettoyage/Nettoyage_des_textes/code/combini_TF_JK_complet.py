#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ÉTAPE 2: SUPPRESSION DES DOUBLONS (COMBO B - TF-IDF + Jaccard Mots en mode ET)
CORRIGÉ - Avec écriture correcte dans MongoDB
"""

from pyspark.sql import SparkSession
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime
import time, math, json, os, sys

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
COLLECTION_DEST   = "commentaires_sans_doublons_combinision"
NB_WORKERS        = 3
SPARK_MASTER      = "spark://spark-master:7077"

# Configuration du COMBO B
SEUIL_TFIDF   = 0.85
SEUIL_JACCARD = 0.85

LOG_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/logs"
os.makedirs(LOG_DIR, exist_ok=True)


# ============================================================
# FONCTIONS DE GESTION DES FLAGS (CORRIGÉES)
# ============================================================

def get_nouveaux_commentaires_count():
    client = MongoClient(MONGO_URI_DRIVER, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION_SOURCE]
    count = collection.count_documents({"traite": False})
    client.close()
    return count


def marquer_comme_traite(ids):
    """Marque les commentaires comme traités - Version STRINGS"""
    if not ids:
        print("   ⚠️ Aucun ID à marquer")
        return
    
    client = MongoClient(MONGO_URI_DRIVER, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION_SOURCE]
    
    valid_ids = []
    for id_str in ids:
        try:
            id_str = str(id_str).strip()
            if len(id_str) > 0:
                valid_ids.append(id_str)
        except:
            pass
    
    if valid_ids:
        resultat = collection.update_many(
            {"_id": {"$in": valid_ids}},
            {"$set": {"traite": True, "date_traitement_doublons": datetime.now()}}
        )
        print(f"   ✅ {resultat.modified_count} commentaires marqués traite=True")
    else:
        print("   ⚠️ Aucun ID valide trouvé")
    
    client.close()


# ============================================================
# FONCTIONS DE SIMILARITÉ (COMBO B)
# ============================================================

def jaccard_mots(texte1, texte2):
    if not texte1 or not texte2:
        return 0
    t1 = str(texte1).lower().strip()
    t2 = str(texte2).lower().strip()
    if t1 == t2:
        return 1.0
    set1 = set(t1.split())
    set2 = set(t2.split())
    if not set1 or not set2:
        return 0
    return len(set1 & set2) / len(set1 | set2)


def cosine_tfidf(texte1, texte2):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    
    vectorizer = TfidfVectorizer(analyzer='word', ngram_range=(1, 2), 
                                  min_df=1, sublinear_tf=True)
    try:
        tfidf = vectorizer.fit_transform([texte1, texte2])
        return cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0]
    except:
        return 0.0


def sont_doublons_combo_b(texte1, texte2):
    score_jaccard = jaccard_mots(texte1, texte2)
    if score_jaccard >= SEUIL_JACCARD:
        score_tfidf = cosine_tfidf(texte1, texte2)
        return score_tfidf >= SEUIL_TFIDF
    return False


# ============================================================
# FONCTIONS SPARK (CORRIGÉES)
# ============================================================

def lire_commentaires_non_traites_depuis_mongo(partition_info):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client = MongoClient("mongodb://mongodb_pfe:27017/",
                             serverSelectionTimeoutMS=5000)
        db = client["telecom_algerie"]
        collection = db["commentaires_sans_urls_arobase"]
        
        query = {"traite": False}
        
        curseur = collection.find(
            query,
            {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
             "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
        ).skip(item["skip"]).limit(item["limit"])
        
        for doc in curseur:
            original_id = str(doc["_id"])
            # Créer un nouveau dict avec l'ID comme string
            yield {
                "original_id": original_id,
                "Commentaire_Client": doc.get("Commentaire_Client", ""),
                "commentaire_moderateur": doc.get("commentaire_moderateur", ""),
                "date": doc.get("date"),
                "source": doc.get("source"),
                "moderateur": doc.get("moderateur"),
                "metadata": doc.get("metadata"),
                "statut": doc.get("statut")
            }
        client.close()


def deduplication_partition_spark(partition):
    """
    Version Spark : suppression des doublons avec COMBO B (TF-IDF + Jaccard Mots)
    """
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    
    SEUIL_J = 0.85
    SEUIL_T = 0.85
    
    def jaccard_local(t1, t2):
        if not t1 or not t2:
            return 0
        t1 = str(t1).lower().strip()
        t2 = str(t2).lower().strip()
        if t1 == t2:
            return 1.0
        set1 = set(t1.split())
        set2 = set(t2.split())
        if not set1 or not set2:
            return 0
        return len(set1 & set2) / len(set1 | set2)
    
    def tfidf_local(t1, t2):
        vectorizer = TfidfVectorizer(analyzer='word', ngram_range=(1, 2), 
                                      min_df=1, sublinear_tf=True)
        try:
            tfidf = vectorizer.fit_transform([t1, t2])
            return cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0]
        except:
            return 0.0
    
    def sont_doublons_local(t1, t2):
        score_j = jaccard_local(t1, t2)
        if score_j >= SEUIL_J:
            score_t = tfidf_local(t1, t2)
            return score_t >= SEUIL_T
        return False
    
    docs_partition = list(partition)
    
    if len(docs_partition) == 0:
        return []
    
    docs_gardes = []
    ids_traites = []  # ← TOUS les IDs traités (même les doublons)
    
    for doc in docs_partition:
        texte_ref = doc.get("Commentaire_Client", "")
        
        est_doublon = False
        for doc_garde in docs_gardes:
            texte_garde = doc_garde.get("Commentaire_Client", "")
            if sont_doublons_local(texte_ref, texte_garde):
                est_doublon = True
                break
        
        # ⚠️ CHANGEMENT IMPORTANT : On marque TOUJOURS l'ID comme traité
        original_id = doc.get("original_id")
        if not original_id:
            original_id = doc.get("_id", "")
        ids_traites.append(str(original_id))
        
        if not est_doublon:
            docs_gardes.append(doc)
    
    # Écriture dans MongoDB (collection destination)
    if docs_gardes:
        try:
            client = MongoClient("mongodb://mongodb_pfe:27017/",
                                 serverSelectionTimeoutMS=5000)
            db = client["telecom_algerie"]
            collection = db["commentaires_sans_doublons_combinision"]
        except Exception as e:
            yield {"_erreur": str(e), "ids_traites": ids_traites, "docs_gardes": 0}
            return
        
        batch = []
        for doc in docs_gardes:
            doc_clean = {
                "_id": doc.get("original_id"),
                "Commentaire_Client": doc.get("Commentaire_Client"),
                "commentaire_moderateur": doc.get("commentaire_moderateur"),
                "date": doc.get("date"),
                "source": doc.get("source"),
                "moderateur": doc.get("moderateur"),
                "metadata": doc.get("metadata"),
                "statut": "sans_doublons",
                "traite": False
            }
            batch.append(InsertOne(doc_clean))
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
        "docs_gardes": len(docs_gardes),
        "docs_supprimes": len(docs_partition) - len(docs_gardes),
        "ids_traites": ids_traites  # ← TOUS les IDs (y compris les doublons)
    }


# ============================================================
# MAIN
# ============================================================

def main():
    temps_debut = time.time()
    
    print("=" * 70)
    print("🔁 ÉTAPE 2: SUPPRESSION DES DOUBLONS (COMBO B)")
    print(f"   📊 Méthode: TF-IDF + Jaccard Mots (mode ET)")
    print(f"   📊 Seuils: TF-IDF={SEUIL_TFIDF}, Jaccard={SEUIL_JACCARD}")
    print("=" * 70)
    
    nouveaux_count = get_nouveaux_commentaires_count()
    
    if nouveaux_count == 0:
        print("\n✅ Aucun nouveau commentaire à traiter")
        return
    
    print(f"\n📥 {nouveaux_count} nouveaux commentaires à traiter")
    
    client_driver = MongoClient(MONGO_URI_DRIVER, serverSelectionTimeoutMS=5000)
    db_driver = client_driver[DB_NAME]
    
    # Vider la collection destination pour ce test
    coll_dest = db_driver[COLLECTION_DEST]
    # coll_dest.delete_many({})
    # print("🧹 Collection destination vidée")
    print(f"   s📁 Collection destination: {coll_dest.count_documents({})} documents existants")
    total_docs = nouveaux_count
    print(f"\n📂 {total_docs} documents à traiter")
    
    print("\n⚡ Connexion au cluster Spark...")
    spark = SparkSession.builder \
        .appName("Deduplication_ComboB_ET") \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n📥 Lecture des commentaires...")
    docs_par_worker = math.ceil(total_docs / NB_WORKERS)
    plages = [
        {"skip": i * docs_par_worker,
         "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
        for i in range(NB_WORKERS)
    ]
    
    rdd_data = spark.sparkContext \
        .parallelize(plages, NB_WORKERS) \
        .mapPartitions(lire_commentaires_non_traites_depuis_mongo)
    
    df_spark = spark.read.json(rdd_data.map(lambda d: json.dumps(d)))
    total_lignes = df_spark.count()
    print(f"✅ {total_lignes} documents chargés")
    
    print(f"\n💾 Suppression des doublons...")
    temps_traitement = time.time()
    
    rdd_stats = df_spark.rdd \
        .map(lambda row: row.asDict()) \
        .mapPartitions(deduplication_partition_spark)
    
    stats = rdd_stats.collect()
    total_traites = sum(s.get("docs_traites", 0) for s in stats)
    total_gardes = sum(s.get("docs_gardes", 0) for s in stats)
    total_supprimes = sum(s.get("docs_supprimes", 0) for s in stats)
    
    tous_ids_traites = []
    for s in stats:
        tous_ids_traites.extend(s.get("ids_traites", []))
    
    print(f"✅ Traitement terminé en {time.time()-temps_traitement:.2f}s")
    print(f"\n📊 RÉSULTATS :")
    print(f"   📥 Commentaires lus      : {total_traites}")
    print(f"   ✅ Commentaires uniques  : {total_gardes}")
    print(f"   ❌ Doublons supprimés    : {total_supprimes}")
    if total_traites > 0:
        print(f"   📉 Taux de réduction    : {total_supprimes/total_traites*100:.2f}%")
    
    if tous_ids_traites:
        print(f"\n🏷️  Marquage de {len(tous_ids_traites)} commentaires...")
        marquer_comme_traite(tous_ids_traites)
    
    # Vérification
    total_dest = db_driver[COLLECTION_DEST].count_documents({})
    print(f"\n📊 Collection destination: {total_dest} commentaires")
    
    restants = get_nouveaux_commentaires_count()
    print(f"📊 Collection source: {restants} commentaires restants")
    
    spark.stop()
    client_driver.close()
    
    print(f"\n⏱️  Temps total : {time.time()-temps_debut:.2f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()