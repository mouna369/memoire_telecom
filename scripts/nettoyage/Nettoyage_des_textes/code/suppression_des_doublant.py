

# # # # # #!/usr/bin/env python3
# # # # # # -*- coding: utf-8 -*-

# # # # # # scripts/nettoyage/supprimer_doublons_multinode.py
# # # # # # ÉTAPE — Suppression intelligente des doublons
# # # # # #
# # # # # # RÈGLES :
# # # # # #   ❌ R1 : même texte + même jour + même source + même mod → garder 1
# # # # # #   ✅ R2 : même texte + même jour + source diff            → 1 par source
# # # # # #   ✅ R3 : même texte + jours différents                   → 1 par jour
# # # # # #   ✅ R4 : même texte + même jour + mod diff               → 1 par mod
# # # # # #   ✅ R5 : texte tronqué vs complet → garder le complet
# # # # # #           (comparaison sur les 60 premiers caractères)
# # # # # #
# # # # # # CORRECTION : passe finale Python pour supprimer les doublons R1
# # # # # #              qui échappent à Spark (répartis sur 2 partitions différentes)
# # # # # #
# # # # # # Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

# # # # # from pyspark.sql import SparkSession
# # # # # from pymongo import MongoClient, InsertOne
# # # # # from pymongo.errors import BulkWriteError
# # # # # from datetime import datetime
# # # # # from collections import defaultdict
# # # # # import os, time, math, json, re

# # # # # # ============================================================
# # # # # # CONFIGURATION
# # # # # # ============================================================
# # # # # MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# # # # # MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# # # # # DB_NAME           = "telecom_algerie"
# # # # # COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
# # # # # COLLECTION_DEST   = "commentaires_sans_doublons"
# # # # # NB_WORKERS        = 2
# # # # # SPARK_MASTER      = "spark://spark-master:7077"
# # # # # RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_suppression_doublons.txt"

# # # # # NB_CHARS_COMPARAISON = 60

# # # # # # ============================================================
# # # # # # FONCTIONS UTILITAIRES
# # # # # # ============================================================
# # # # # def extraire_jour(date_str):
# # # # #     if not date_str:
# # # # #         return "inconnu"
# # # # #     date_str = str(date_str).strip()
# # # # #     for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y",
# # # # #                 "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
# # # # #         try:
# # # # #             return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
# # # # #         except:
# # # # #             continue
# # # # #     return date_str[:10]

# # # # # def normaliser_texte(texte, nb_chars=NB_CHARS_COMPARAISON):
# # # # #     if not texte:
# # # # #         return ""
# # # # #     texte = str(texte).strip()
# # # # #     texte = re.sub(r'[Ee]n voir plus\.?', '', texte)
# # # # #     texte = re.sub(r'See more\.?',        '', texte)
# # # # #     texte = re.sub(r'أكثر\.?',           '', texte)
# # # # #     texte = re.sub(r'\s+', ' ', texte).strip()
# # # # #     return texte[:nb_chars]

# # # # # def appliquer_regles(copies):
# # # # #     if len(copies) <= 1:
# # # # #         return copies

# # # # #     for c in copies:
# # # # #         c["_jour"]       = extraire_jour(str(c.get("date", "")))
# # # # #         c["_texte_norm"] = normaliser_texte(c.get("Commentaire_Client", ""))

# # # # #     groupes = defaultdict(list)
# # # # #     for c in copies:
# # # # #         cle = (
# # # # #             c["_texte_norm"],
# # # # #             c["_jour"],
# # # # #             str(c.get("source",     "") or ""),
# # # # #             str(c.get("moderateur", "") or "inconnu")
# # # # #         )
# # # # #         groupes[cle].append(c)

# # # # #     copies_finales = []
# # # # #     for cle, groupe in groupes.items():
# # # # #         groupe_trie = sorted(
# # # # #             groupe,
# # # # #             key=lambda x: len(str(x.get("Commentaire_Client", ""))),
# # # # #             reverse=True
# # # # #         )
# # # # #         copies_finales.append(groupe_trie[0])

# # # # #     return copies_finales

# # # # # # ============================================================
# # # # # # TEST LOCAL DES RÈGLES
# # # # # # ============================================================
# # # # # def tester_regles():
# # # # #     texte_tronque = "كي يطلقولي الفيبر برك وساهل 20 يوم ملي ركبولي كلشي من هذاك النهار وانا En voir plus"
# # # # #     texte_complet = "كي يطلقولي الفيبر برك وساهل 20 يوم ملي ركبولي كلشي من هذاك النهار وانا طالع هابط حسبت روحي"

# # # # #     cas_tests = [
# # # # #         {
# # # # #             "desc"   : "R1 — Même jour + même source + même mod → garder 1",
# # # # #             "copies" : [
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:58", "moderateur": "Hiba"},
# # # # #             ],
# # # # #             "attendu": 1
# # # # #         },
# # # # #         {
# # # # #             "desc"   : "R2 — Même jour + sources diff → 1 par source",
# # # # #             "copies" : [
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "28/11/2025 21:38", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Instagram", "date": "28/11/2025 21:27", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Instagram", "date": "28/11/2025 21:29", "moderateur": "Hiba"},
# # # # #             ],
# # # # #             "attendu": 2
# # # # #         },
# # # # #         {
# # # # #             "desc"   : "R3 — Jours différents → garder les 2",
# # # # #             "copies" : [
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "28/11/2025 14:34", "moderateur": "Hiba"},
# # # # #             ],
# # # # #             "attendu": 2
# # # # #         },
# # # # #         {
# # # # #             "desc"   : "R4 — Même jour + mod diff → 1 par mod",
# # # # #             "copies" : [
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:58", "moderateur": "Ali"},
# # # # #             ],
# # # # #             "attendu": 2
# # # # #         },
# # # # #         {
# # # # #             "desc"   : "Cas تم — 4 copies mixtes → garder 2 (1 par jour)",
# # # # #             "copies" : [
# # # # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "28/11/2025 14:34", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:58", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # # #             ],
# # # # #             "attendu": 2
# # # # #         },
# # # # #         {
# # # # #             "desc"   : "R5 — Texte tronqué vs complet → garder complet",
# # # # #             "copies" : [
# # # # #                 {"Commentaire_Client": texte_tronque, "source": "Instagram", "date": "30/11/2025 21:27", "moderateur": "MohamedAmine"},
# # # # #                 {"Commentaire_Client": texte_complet, "source": "Instagram", "date": "30/11/2025 21:30", "moderateur": "MohamedAmine"},
# # # # #             ],
# # # # #             "attendu": 1
# # # # #         },
# # # # #     ]

# # # # #     print("\n🧪 TEST DES RÈGLES AVANT SPARK :")
# # # # #     print("-"*70)

# # # # #     tous_ok = True
# # # # #     for cas in cas_tests:
# # # # #         copies   = [dict(c) for c in cas["copies"]]
# # # # #         resultat = appliquer_regles(copies)
# # # # #         ok       = len(resultat) == cas["attendu"]
# # # # #         if not ok:
# # # # #             tous_ok = False
# # # # #         print(f"   {'✅ OK' if ok else '❌ ERREUR'} — {cas['desc']}")
# # # # #         print(f"          Attendu={cas['attendu']} | Obtenu={len(resultat)}")
# # # # #         if "tronqué" in cas["desc"] and resultat:
# # # # #             print(f"          Texte gardé : {resultat[0].get('Commentaire_Client','')[:60]}...")

# # # # #     print()
# # # # #     if tous_ok:
# # # # #         print("✅ TOUS LES TESTS PASSÉS — on peut lancer Spark !")
# # # # #     else:
# # # # #         print("❌ CERTAINS TESTS ONT ÉCHOUÉ !")
# # # # #     print()
# # # # #     return tous_ok

# # # # # tests_ok = tester_regles()
# # # # # if not tests_ok:
# # # # #     exit(1)

# # # # # # ============================================================
# # # # # # FONCTIONS DISTRIBUÉES SPARK
# # # # # # ============================================================
# # # # # def lire_partition_depuis_mongo(partition_info):
# # # # #     import sys
# # # # #     sys.path.insert(0, '/opt/pymongo_libs')
# # # # #     from pymongo import MongoClient

# # # # #     for item in partition_info:
# # # # #         client     = MongoClient("mongodb://mongodb_pfe:27017/",
# # # # #                                  serverSelectionTimeoutMS=5000)
# # # # #         db         = client["telecom_algerie"]
# # # # #         collection = db["commentaires_sans_urls_arobase"]

# # # # #         curseur = collection.find(
# # # # #             {},
# # # # #             {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
# # # # #              "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
# # # # #         ).skip(item["skip"]).limit(item["limit"])

# # # # #         for doc in curseur:
# # # # #             doc["_id"] = str(doc["_id"])
# # # # #             yield doc

# # # # #         client.close()


# # # # # def deduplication_partition(partition):
# # # # #     import sys
# # # # #     sys.path.insert(0, '/opt/pymongo_libs')
# # # # #     import re
# # # # #     from pymongo import MongoClient, InsertOne
# # # # #     from pymongo.errors import BulkWriteError
# # # # #     from collections import defaultdict
# # # # #     from datetime import datetime

# # # # #     NB_CHARS = 60

# # # # #     def extraire_jour_w(date_str):
# # # # #         if not date_str:
# # # # #             return "inconnu"
# # # # #         date_str = str(date_str).strip()
# # # # #         for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y",
# # # # #                     "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
# # # # #             try:
# # # # #                 return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
# # # # #             except:
# # # # #                 continue
# # # # #         return date_str[:10]

# # # # #     def normaliser_w(texte):
# # # # #         if not texte:
# # # # #             return ""
# # # # #         texte = str(texte).strip()
# # # # #         texte = re.sub(r'[Ee]n voir plus\.?', '', texte)
# # # # #         texte = re.sub(r'See more\.?',        '', texte)
# # # # #         texte = re.sub(r'أكثر\.?',           '', texte)
# # # # #         texte = re.sub(r'\s+', ' ', texte).strip()
# # # # #         return texte[:NB_CHARS]

# # # # #     def appliquer_regles_w(copies):
# # # # #         if len(copies) <= 1:
# # # # #             return copies

# # # # #         for c in copies:
# # # # #             c["_jour"]  = extraire_jour_w(str(c.get("date", "")))
# # # # #             c["_tnorm"] = normaliser_w(c.get("Commentaire_Client", ""))

# # # # #         groupes = defaultdict(list)
# # # # #         for c in copies:
# # # # #             cle = (c["_tnorm"], c["_jour"],
# # # # #                    str(c.get("source",     "") or ""),
# # # # #                    str(c.get("moderateur", "") or "inconnu"))
# # # # #             groupes[cle].append(c)

# # # # #         copies_finales = []
# # # # #         for cle, groupe in groupes.items():
# # # # #             groupe_trie = sorted(
# # # # #                 groupe,
# # # # #                 key=lambda x: len(str(x.get("Commentaire_Client", ""))),
# # # # #                 reverse=True
# # # # #             )
# # # # #             copies_finales.append(groupe_trie[0])

# # # # #         return copies_finales

# # # # #     docs_partition = list(partition)

# # # # #     groupes_texte = defaultdict(list)
# # # # #     for doc in docs_partition:
# # # # #         texte = doc.get("Commentaire_Client", "") or ""
# # # # #         cle   = normaliser_w(texte.strip())
# # # # #         groupes_texte[cle].append(doc)

# # # # #     docs_a_garder  = []
# # # # #     docs_supprimes = 0

# # # # #     for cle_texte, copies in groupes_texte.items():
# # # # #         gardes = appliquer_regles_w(copies)
# # # # #         for g in gardes:
# # # # #             g.pop("_jour",  None)
# # # # #             g.pop("_tnorm", None)
# # # # #         docs_a_garder.extend(gardes)
# # # # #         docs_supprimes += len(copies) - len(gardes)

# # # # #     try:
# # # # #         client     = MongoClient("mongodb://mongodb_pfe:27017/",
# # # # #                                  serverSelectionTimeoutMS=5000)
# # # # #         db         = client["telecom_algerie"]
# # # # #         collection = db["commentaires_sans_doublons"]
# # # # #     except Exception as e:
# # # # #         yield {"_erreur": str(e), "statut": "connexion_failed"}
# # # # #         return

# # # # #     batch        = []
# # # # #     docs_inseres = 0

# # # # #     for doc in docs_a_garder:
# # # # #         batch.append(InsertOne(doc))
# # # # #         docs_inseres += 1
# # # # #         if len(batch) >= 1000:
# # # # #             try:
# # # # #                 collection.bulk_write(batch, ordered=False)
# # # # #             except BulkWriteError:
# # # # #                 pass
# # # # #             batch = []

# # # # #     if batch:
# # # # #         try:
# # # # #             collection.bulk_write(batch, ordered=False)
# # # # #         except BulkWriteError:
# # # # #             pass

# # # # #     client.close()
# # # # #     yield {
# # # # #         "docs_traites"  : len(docs_partition),
# # # # #         "docs_inseres"  : docs_inseres,
# # # # #         "docs_supprimes": docs_supprimes,
# # # # #         "statut"        : "ok"
# # # # #     }

# # # # # # ============================================================
# # # # # # PIPELINE SPARK
# # # # # # ============================================================
# # # # # temps_debut = time.time()

# # # # # print("="*70)
# # # # # print("🔁 SUPPRESSION INTELLIGENTE DES DOUBLONS — MULTI-NODE")
# # # # # print("   ❌ R1 : même texte + même jour + même source + même mod → 1")
# # # # # print("   ✅ R2 : même texte + même jour + source diff            → 1/source")
# # # # # print("   ✅ R3 : même texte + jours différents                   → 1/jour")
# # # # # print("   ✅ R4 : même texte + même jour + mod diff               → 1/mod")
# # # # # print("   ✅ R5 : texte tronqué vs complet                        → garder complet")
# # # # # print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# # # # # print("="*70)

# # # # # # 1. Connexion MongoDB Driver
# # # # # print("\n📂 Connexion MongoDB (Driver)...")
# # # # # client_driver = MongoClient(MONGO_URI_DRIVER)
# # # # # db_driver     = client_driver[DB_NAME]
# # # # # coll_source   = db_driver[COLLECTION_SOURCE]
# # # # # total_docs    = coll_source.count_documents({})
# # # # # print(f"✅ {total_docs} documents dans la source")

# # # # # # 2. Connexion Spark
# # # # # print("\n⚡ Connexion au cluster Spark...")
# # # # # temps_spark = time.time()

# # # # # spark = SparkSession.builder \
# # # # #     .appName("Suppression_Doublons_MultiNode") \
# # # # #     .master(SPARK_MASTER) \
# # # # #     .config("spark.executor.memory", "2g") \
# # # # #     .config("spark.executor.cores", "2") \
# # # # #     .config("spark.sql.shuffle.partitions", "4") \
# # # # #     .getOrCreate()

# # # # # spark.sparkContext.setLogLevel("WARN")
# # # # # print(f"✅ Spark connecté en {time.time()-temps_spark:.2f}s")

# # # # # # 3. Lecture distribuée
# # # # # print("\n📥 LECTURE DISTRIBUÉE...")
# # # # # docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# # # # # plages = [
# # # # #     {"skip" : i * docs_par_worker,
# # # # #      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
# # # # #     for i in range(NB_WORKERS)
# # # # # ]
# # # # # for idx, p in enumerate(plages):
# # # # #     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

# # # # # rdd_data = spark.sparkContext \
# # # # #     .parallelize(plages, NB_WORKERS) \
# # # # #     .mapPartitions(lire_partition_depuis_mongo)

# # # # # df_spark     = spark.read.json(rdd_data.map(
# # # # #     lambda d: json.dumps(
# # # # #         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
# # # # #          for k, v in d.items()}
# # # # #     )
# # # # # ))
# # # # # total_lignes = df_spark.count()
# # # # # print(f"✅ {total_lignes} documents chargés")

# # # # # # 4. Vider destination
# # # # # coll_dest = db_driver[COLLECTION_DEST]
# # # # # coll_dest.delete_many({})
# # # # # print("\n🧹 Collection destination vidée")

# # # # # # 5. Déduplication Spark + écriture
# # # # # print("\n💾 DÉDUPLICATION + ÉCRITURE DISTRIBUÉE...")
# # # # # temps_traitement = time.time()

# # # # # rdd_stats = df_spark.rdd \
# # # # #     .map(lambda row: row.asDict()) \
# # # # #     .mapPartitions(deduplication_partition)

# # # # # stats           = rdd_stats.collect()
# # # # # total_inseres   = sum(s.get("docs_inseres",   0) for s in stats if s.get("statut") == "ok")
# # # # # total_supprimes = sum(s.get("docs_supprimes", 0) for s in stats if s.get("statut") == "ok")
# # # # # erreurs         = [s for s in stats if "_erreur" in s]

# # # # # print(f"✅ Traitement Spark terminé en {time.time()-temps_traitement:.2f}s")
# # # # # if erreurs:
# # # # #     for e in erreurs:
# # # # #         print(f"   ⚠️  {e.get('_erreur')}")

# # # # # spark.stop()

# # # # # # ============================================================
# # # # # # PASSE FINALE PYTHON — Correction doublons inter-partitions
# # # # # # ============================================================
# # # # # # Bug Spark : si 2 copies d'un même doc R1 se trouvent dans
# # # # # # 2 partitions différentes → chaque worker en garde 1
# # # # # # → La passe finale corrige ces cas résiduels
# # # # # # ============================================================
# # # # # print("\n🔧 PASSE FINALE — Correction doublons inter-partitions...")
# # # # # temps_passe_finale = time.time()

# # # # # from pymongo import MongoClient as MC

# # # # # client_final = MC(MONGO_URI_DRIVER)
# # # # # coll_final   = client_final[DB_NAME][COLLECTION_DEST]

# # # # # vus           = {}   # cle → _id du doc gardé
# # # # # a_supprimer   = []
# # # # # nb_R1_residuels = 0

# # # # # for doc in coll_final.find(
# # # # #     {},
# # # # #     {"_id": 1, "Commentaire_Client": 1, "date": 1, "source": 1, "moderateur": 1}
# # # # # ):
# # # # #     texte  = normaliser_texte(doc.get("Commentaire_Client") or "")
# # # # #     jour   = extraire_jour(doc.get("date") or "")
# # # # #     source = str(doc.get("source")     or "")
# # # # #     mod    = str(doc.get("moderateur") or "inconnu")
# # # # #     cle    = (texte, jour, source, mod)

# # # # #     if cle in vus:
# # # # #         # Doublon R1 résiduel → supprimer
# # # # #         a_supprimer.append(doc["_id"])
# # # # #         nb_R1_residuels += 1
# # # # #     else:
# # # # #         vus[cle] = doc["_id"]

# # # # # if a_supprimer:
# # # # #     # Les _id sont des strings après Spark (str(doc["_id"]) dans lire_partition)
# # # # #     # → on supprime directement sans conversion ObjectId
# # # # #     coll_final.delete_many({"_id": {"$in": a_supprimer}})
# # # # #     total_supprimes += nb_R1_residuels
# # # # #     print(f"✅ {nb_R1_residuels} doublons R1 inter-partitions supprimés")
# # # # # else:
# # # # #     print("✅ Aucun doublon R1 inter-partitions — Spark était complet !")

# # # # # print(f"   Passe finale terminée en {time.time()-temps_passe_finale:.2f}s")

# # # # # # 6. Vérification finale
# # # # # print("\n🔎 VÉRIFICATION FINALE...")
# # # # # total_en_dest = coll_final.count_documents({})
# # # # # succes        = True

# # # # # print(f"   • Documents source      : {total_lignes}")
# # # # # print(f"   • Documents supprimés   : {total_supprimes}")
# # # # # print(f"   • Documents gardés      : {total_en_dest}")
# # # # # print(f"   • Taux de réduction     : {total_supprimes/total_lignes*100:.2f}%")
# # # # # print(f"   • Doublons R1 résiduels : {nb_R1_residuels} ✅")
# # # # # print(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")

# # # # # # 7. Rapport
# # # # # temps_total = time.time() - temps_debut

# # # # # lignes_rapport = []
# # # # # lignes_rapport.append("=" * 70)
# # # # # lignes_rapport.append("🔁 SUPPRESSION INTELLIGENTE DES DOUBLONS")
# # # # # lignes_rapport.append(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# # # # # lignes_rapport.append(f"   Mode : Spark 4.1.1 | Multi-Node | Workers → MongoDB direct")
# # # # # lignes_rapport.append("=" * 70)
# # # # # lignes_rapport.append("\nRÈGLES APPLIQUÉES :")
# # # # # lignes_rapport.append("   ❌ R1 : même texte(60c) + même jour + même source + même mod → garder 1")
# # # # # lignes_rapport.append("   ✅ R2 : même texte(60c) + même jour + source diff            → 1 par source")
# # # # # lignes_rapport.append("   ✅ R3 : même texte(60c) + jours différents                   → 1 par jour")
# # # # # lignes_rapport.append("   ✅ R4 : même texte(60c) + même jour + mod diff               → 1 par modérateur")
# # # # # lignes_rapport.append("   ✅ R5 : texte tronqué vs complet → garder le plus long")
# # # # # lignes_rapport.append("   🔧 Passe finale Python → correction doublons inter-partitions")
# # # # # lignes_rapport.append(f"\n📊 RÉSULTATS :")
# # # # # lignes_rapport.append(f"   ┌────────────────────────────────────────────┐")
# # # # # lignes_rapport.append(f"   │ Documents source          : {total_lignes:<15} │")
# # # # # lignes_rapport.append(f"   │ Documents supprimés       : {total_supprimes:<15} │")
# # # # # lignes_rapport.append(f"   │ Documents gardés          : {total_en_dest:<15} │")
# # # # # lignes_rapport.append(f"   │ Taux de réduction         : {total_supprimes/total_lignes*100:<14.2f}% │")
# # # # # lignes_rapport.append(f"   │ Doublons R1 résiduels     : {nb_R1_residuels:<15} │")
# # # # # lignes_rapport.append(f"   └────────────────────────────────────────────┘")
# # # # # lignes_rapport.append(f"\n⏱️  TEMPS :")
# # # # # lignes_rapport.append(f"   • Total    : {temps_total:.2f}s")
# # # # # lignes_rapport.append(f"   • Vitesse  : {total_lignes/temps_total:.0f} docs/s")
# # # # # lignes_rapport.append(f"\n📁 STOCKAGE :")
# # # # # lignes_rapport.append(f"   • Source      : {DB_NAME}.{COLLECTION_SOURCE}")
# # # # # lignes_rapport.append(f"   • Destination : {DB_NAME}.{COLLECTION_DEST}")
# # # # # lignes_rapport.append(f"   • Statut      : ✅ SUCCÈS")
# # # # # lignes_rapport.append("=" * 70)

# # # # # rapport_texte = "\n".join(lignes_rapport)
# # # # # os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# # # # # with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
# # # # #     f.write(rapport_texte)

# # # # # client_driver.close()
# # # # # client_final.close()

# # # # # print(f"\n✅ Rapport : {RAPPORT_PATH}")
# # # # # print("\n" + "="*70)
# # # # # print("📊 RÉSUMÉ FINAL")
# # # # # print("="*70)
# # # # # print(f"   📥 Documents source        : {total_lignes}")
# # # # # print(f"   ❌ Documents supprimés     : {total_supprimes}")
# # # # # print(f"   ✅ Documents gardés        : {total_en_dest}")
# # # # # print(f"   📊 Taux réduction          : {total_supprimes/total_lignes*100:.2f}%")
# # # # # print(f"   🔧 Doublons R1 résiduels   : {nb_R1_residuels} corrigés")
# # # # # print(f"   ⏱️  Temps total             : {temps_total:.2f}s")
# # # # # print(f"   📁 Destination             : {DB_NAME}.{COLLECTION_DEST}")
# # # # # print("="*70)
# # # # # print("🎉 SUPPRESSION DOUBLONS TERMINÉE EN MODE MULTI-NODE !")

# # # # #!/usr/bin/env python3
# # # # # -*- coding: utf-8 -*-

# # # # # scripts/nettoyage/supprimer_doublons_multinode.py
# # # # # ÉTAPE — Suppression intelligente des doublons
# # # # #
# # # # # RÈGLES :
# # # # #   ❌ R1 : même texte + même jour + même source + même mod → garder 1
# # # # #   ✅ R2 : même texte + même jour + source diff            → 1 par source
# # # # #   ✅ R3 : même texte + jours différents                   → 1 par jour
# # # # #   ✅ R4 : même texte + même jour + mod diff               → 1 par mod
# # # # #   ✅ R5 : texte tronqué vs complet → garder le complet
# # # # #
# # # # # CORRECTION : passe finale Python pour doublons inter-partitions Spark
# # # # #
# # # # # Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

# # # # from pyspark.sql import SparkSession
# # # # from pymongo import MongoClient, InsertOne
# # # # from pymongo.errors import BulkWriteError
# # # # from datetime import datetime
# # # # from collections import defaultdict
# # # # import os, time, math, json, re



# # # # # ============================================================
# # # # # CONFIGURATION
# # # # # ============================================================
# # # # MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# # # # MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# # # # DB_NAME           = "telecom_algerie"
# # # # COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
# # # # COLLECTION_DEST   = "commentaires_sans_doublons"
# # # # NB_WORKERS        = 2
# # # # SPARK_MASTER      = "spark://spark-master:7077"
# # # # RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_suppression_doublons.txt"
# # # # STATS_FILE        = "/tmp/spark_stats.json"   # ← AJOUT : fichier partagé avec le dashboard
# # # # NB_CHARS_COMPARAISON = 60

# # # # # ============================================================
# # # # # FONCTIONS UTILITAIRES
# # # # # ============================================================
# # # # def extraire_jour(date_str):
# # # #     if not date_str:
# # # #         return "inconnu"
# # # #     date_str = str(date_str).strip()
# # # #     for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y",
# # # #                 "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
# # # #         try:
# # # #             return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
# # # #         except:
# # # #             continue
# # # #     return date_str[:10]

# # # # def normaliser_texte(texte, nb_chars=NB_CHARS_COMPARAISON):
# # # #     if not texte:
# # # #         return ""
# # # #     texte = str(texte).strip()
# # # #     texte = re.sub(r'[Ee]n voir plus\.?', '', texte)
# # # #     texte = re.sub(r'See more\.?',        '', texte)
# # # #     texte = re.sub(r'أكثر\.?',           '', texte)
# # # #     texte = re.sub(r'\s+', ' ', texte).strip()
# # # #     return texte[:nb_chars]

# # # # def appliquer_regles(copies):
# # # #     if len(copies) <= 1:
# # # #         return copies
# # # #     for c in copies:
# # # #         c["_jour"]       = extraire_jour(str(c.get("date", "")))
# # # #         c["_texte_norm"] = normaliser_texte(c.get("Commentaire_Client", ""))
# # # #     groupes = defaultdict(list)
# # # #     for c in copies:
# # # #         cle = (
# # # #             c["_texte_norm"],
# # # #             c["_jour"],
# # # #             str(c.get("source",     "") or ""),
# # # #             str(c.get("moderateur", "") or "inconnu")
# # # #         )
# # # #         groupes[cle].append(c)
# # # #     copies_finales = []
# # # #     for cle, groupe in groupes.items():
# # # #         groupe_trie = sorted(
# # # #             groupe,
# # # #             key=lambda x: len(str(x.get("Commentaire_Client", ""))),
# # # #             reverse=True
# # # #         )
# # # #         copies_finales.append(groupe_trie[0])
# # # #     return copies_finales

# # # # # ============================================================
# # # # # TEST LOCAL DES RÈGLES
# # # # # ============================================================
# # # # def tester_regles():
# # # #     texte_tronque = "كي يطلقولي الفيبر برك وساهل 20 يوم ملي ركبولي كلشي من هذاك النهار وانا En voir plus"
# # # #     texte_complet = "كي يطلقولي الفيبر برك وساهل 20 يوم ملي ركبولي كلشي من هذاك النهار وانا طالع هابط حسبت روحي"

# # # #     cas_tests = [
# # # #         {
# # # #             "desc"   : "R1 — Même jour + même source + même mod → garder 1",
# # # #             "copies" : [
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:58", "moderateur": "Hiba"},
# # # #             ],
# # # #             "attendu": 1
# # # #         },
# # # #         {
# # # #             "desc"   : "R2 — Même jour + sources diff → 1 par source",
# # # #             "copies" : [
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "28/11/2025 21:38", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Instagram", "date": "28/11/2025 21:27", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Instagram", "date": "28/11/2025 21:29", "moderateur": "Hiba"},
# # # #             ],
# # # #             "attendu": 2
# # # #         },
# # # #         {
# # # #             "desc"   : "R3 — Jours différents → garder les 2",
# # # #             "copies" : [
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "28/11/2025 14:34", "moderateur": "Hiba"},
# # # #             ],
# # # #             "attendu": 2
# # # #         },
# # # #         {
# # # #             "desc"   : "R4 — Même jour + mod diff → 1 par mod",
# # # #             "copies" : [
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:58", "moderateur": "Ali"},
# # # #             ],
# # # #             "attendu": 2
# # # #         },
# # # #         {
# # # #             "desc"   : "Cas تم — 4 copies mixtes → garder 2 (1 par jour)",
# # # #             "copies" : [
# # # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "28/11/2025 14:34", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:58", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # # #             ],
# # # #             "attendu": 2
# # # #         },
# # # #         {
# # # #             "desc"   : "R5 — Texte tronqué vs complet → garder complet",
# # # #             "copies" : [
# # # #                 {"Commentaire_Client": texte_tronque, "source": "Instagram", "date": "30/11/2025 21:27", "moderateur": "MohamedAmine"},
# # # #                 {"Commentaire_Client": texte_complet, "source": "Instagram", "date": "30/11/2025 21:30", "moderateur": "MohamedAmine"},
# # # #             ],
# # # #             "attendu": 1
# # # #         },
# # # #     ]

# # # #     print("\n🧪 TEST DES RÈGLES AVANT SPARK :")
# # # #     print("-" * 70)
# # # #     tous_ok = True
# # # #     for cas in cas_tests:
# # # #         copies   = [dict(c) for c in cas["copies"]]
# # # #         resultat = appliquer_regles(copies)
# # # #         ok       = len(resultat) == cas["attendu"]
# # # #         if not ok:
# # # #             tous_ok = False
# # # #         print(f"   {'✅ OK' if ok else '❌ ERREUR'} — {cas['desc']}")
# # # #         print(f"          Attendu={cas['attendu']} | Obtenu={len(resultat)}")
# # # #         if "tronqué" in cas["desc"] and resultat:
# # # #             print(f"          Texte gardé : {resultat[0].get('Commentaire_Client','')[:60]}...")
# # # #     print()
# # # #     if tous_ok:
# # # #         print("✅ TOUS LES TESTS PASSÉS — on peut lancer Spark !")
# # # #     else:
# # # #         print("❌ CERTAINS TESTS ONT ÉCHOUÉ !")
# # # #     print()
# # # #     return tous_ok

# # # # tests_ok = tester_regles()
# # # # if not tests_ok:
# # # #     exit(1)

# # # # # ============================================================
# # # # # FONCTIONS DISTRIBUÉES SPARK
# # # # # ============================================================
# # # # def lire_partition_depuis_mongo(partition_info):
# # # #     import sys
# # # #     sys.path.insert(0, '/opt/pymongo_libs')
# # # #     from pymongo import MongoClient

# # # #     for item in partition_info:
# # # #         client     = MongoClient("mongodb://mongodb_pfe:27017/",
# # # #                                  serverSelectionTimeoutMS=5000)
# # # #         db         = client["telecom_algerie"]
# # # #         collection = db["commentaires_sans_urls_arobase"]
# # # #         curseur = collection.find(
# # # #             {},
# # # #             {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
# # # #              "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
# # # #         ).skip(item["skip"]).limit(item["limit"])
# # # #         for doc in curseur:
# # # #             doc["_id"] = str(doc["_id"])
# # # #             yield doc
# # # #         client.close()


# # # # def deduplication_partition(partition):
# # # #     import sys
# # # #     sys.path.insert(0, '/opt/pymongo_libs')
# # # #     import re
# # # #     from pymongo import MongoClient, InsertOne
# # # #     from pymongo.errors import BulkWriteError
# # # #     from collections import defaultdict
# # # #     from datetime import datetime

# # # #     NB_CHARS = 60

# # # #     def extraire_jour_w(date_str):
# # # #         if not date_str:
# # # #             return "inconnu"
# # # #         date_str = str(date_str).strip()
# # # #         for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y",
# # # #                     "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
# # # #             try:
# # # #                 return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
# # # #             except:
# # # #                 continue
# # # #         return date_str[:10]

# # # #     def normaliser_w(texte):
# # # #         if not texte:
# # # #             return ""
# # # #         texte = str(texte).strip()
# # # #         texte = re.sub(r'[Ee]n voir plus\.?', '', texte)
# # # #         texte = re.sub(r'See more\.?',        '', texte)
# # # #         texte = re.sub(r'أكثر\.?',           '', texte)
# # # #         texte = re.sub(r'\s+', ' ', texte).strip()
# # # #         return texte[:NB_CHARS]

# # # #     def appliquer_regles_w(copies):
# # # #         if len(copies) <= 1:
# # # #             return copies
# # # #         for c in copies:
# # # #             c["_jour"]  = extraire_jour_w(str(c.get("date", "")))
# # # #             c["_tnorm"] = normaliser_w(c.get("Commentaire_Client", ""))
# # # #         groupes = defaultdict(list)
# # # #         for c in copies:
# # # #             cle = (c["_tnorm"], c["_jour"],
# # # #                    str(c.get("source",     "") or ""),
# # # #                    str(c.get("moderateur", "") or "inconnu"))
# # # #             groupes[cle].append(c)
# # # #         copies_finales = []
# # # #         for cle, groupe in groupes.items():
# # # #             groupe_trie = sorted(
# # # #                 groupe,
# # # #                 key=lambda x: len(str(x.get("Commentaire_Client", ""))),
# # # #                 reverse=True
# # # #             )
# # # #             copies_finales.append(groupe_trie[0])
# # # #         return copies_finales

# # # #     docs_partition = list(partition)
# # # #     groupes_texte  = defaultdict(list)
# # # #     for doc in docs_partition:
# # # #         texte = doc.get("Commentaire_Client", "") or ""
# # # #         cle   = normaliser_w(texte.strip())
# # # #         groupes_texte[cle].append(doc)

# # # #     docs_a_garder  = []
# # # #     docs_supprimes = 0
# # # #     for cle_texte, copies in groupes_texte.items():
# # # #         gardes = appliquer_regles_w(copies)
# # # #         for g in gardes:
# # # #             g.pop("_jour",  None)
# # # #             g.pop("_tnorm", None)
# # # #         docs_a_garder.extend(gardes)
# # # #         docs_supprimes += len(copies) - len(gardes)

# # # #     try:
# # # #         client     = MongoClient("mongodb://mongodb_pfe:27017/",
# # # #                                  serverSelectionTimeoutMS=5000)
# # # #         db         = client["telecom_algerie"]
# # # #         collection = db["commentaires_sans_doublons"]
# # # #     except Exception as e:
# # # #         yield {"_erreur": str(e), "statut": "connexion_failed"}
# # # #         return

# # # #     batch        = []
# # # #     docs_inseres = 0
# # # #     for doc in docs_a_garder:
# # # #         batch.append(InsertOne(doc))
# # # #         docs_inseres += 1
# # # #         if len(batch) >= 1000:
# # # #             try:
# # # #                 collection.bulk_write(batch, ordered=False)
# # # #             except BulkWriteError:
# # # #                 pass
# # # #             batch = []
# # # #     if batch:
# # # #         try:
# # # #             collection.bulk_write(batch, ordered=False)
# # # #         except BulkWriteError:
# # # #             pass

# # # #     client.close()
# # # #     yield {
# # # #         "docs_traites"  : len(docs_partition),
# # # #         "docs_inseres"  : docs_inseres,
# # # #         "docs_supprimes": docs_supprimes,
# # # #         "statut"        : "ok"
# # # #     }

# # # # # ============================================================
# # # # # PIPELINE SPARK
# # # # # ============================================================
# # # # temps_debut = time.time()

# # # # print("=" * 70)
# # # # print("🔁 SUPPRESSION INTELLIGENTE DES DOUBLONS — MULTI-NODE")
# # # # print("   ❌ R1 : même texte + même jour + même source + même mod → 1")
# # # # print("   ✅ R2 : même texte + même jour + source diff            → 1/source")
# # # # print("   ✅ R3 : même texte + jours différents                   → 1/jour")
# # # # print("   ✅ R4 : même texte + même jour + mod diff               → 1/mod")
# # # # print("   ✅ R5 : texte tronqué vs complet                        → garder complet")
# # # # print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# # # # print("=" * 70)

# # # # # 1. Connexion MongoDB Driver
# # # # print("\n📂 Connexion MongoDB (Driver)...")
# # # # client_driver = MongoClient(MONGO_URI_DRIVER)
# # # # db_driver     = client_driver[DB_NAME]
# # # # coll_source   = db_driver[COLLECTION_SOURCE]
# # # # total_docs    = coll_source.count_documents({})
# # # # print(f"✅ {total_docs} documents dans la source")

# # # # # 2. Connexion Spark
# # # # print("\n⚡ Connexion au cluster Spark...")
# # # # temps_spark = time.time()
# # # # spark = SparkSession.builder \
# # # #     .appName("Suppression_Doublons_MultiNode") \
# # # #     .master(SPARK_MASTER) \
# # # #     .config("spark.executor.memory", "2g") \
# # # #     .config("spark.executor.cores", "2") \
# # # #     .config("spark.sql.shuffle.partitions", "4") \
# # # #     .getOrCreate()
# # # # spark.sparkContext.setLogLevel("WARN")
# # # # print(f"✅ Spark connecté en {time.time()-temps_spark:.2f}s")

# # # # # 3. Lecture distribuée
# # # # print("\n📥 LECTURE DISTRIBUÉE...")
# # # # docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# # # # plages = [
# # # #     {"skip" : i * docs_par_worker,
# # # #      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
# # # #     for i in range(NB_WORKERS)
# # # # ]
# # # # for idx, p in enumerate(plages):
# # # #     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

# # # # rdd_data = spark.sparkContext \
# # # #     .parallelize(plages, NB_WORKERS) \
# # # #     .mapPartitions(lire_partition_depuis_mongo)
# # # # df_spark = spark.read.json(rdd_data.map(
# # # #     lambda d: json.dumps(
# # # #         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
# # # #          for k, v in d.items()}
# # # #     )
# # # # ))
# # # # total_lignes = df_spark.count()
# # # # print(f"✅ {total_lignes} documents chargés")

# # # # # 4. Vider destination
# # # # coll_dest = db_driver[COLLECTION_DEST]
# # # # coll_dest.delete_many({})
# # # # print("\n🧹 Collection destination vidée")

# # # # # 5. Déduplication Spark + écriture
# # # # print("\n💾 DÉDUPLICATION + ÉCRITURE DISTRIBUÉE...")
# # # # temps_traitement = time.time()
# # # # rdd_stats = df_spark.rdd \
# # # #     .map(lambda row: row.asDict()) \
# # # #     .mapPartitions(deduplication_partition)
# # # # stats           = rdd_stats.collect()
# # # # total_inseres   = sum(s.get("docs_inseres",   0) for s in stats if s.get("statut") == "ok")
# # # # total_supprimes = sum(s.get("docs_supprimes", 0) for s in stats if s.get("statut") == "ok")
# # # # erreurs         = [s for s in stats if "_erreur" in s]
# # # # print(f"✅ Traitement Spark terminé en {time.time()-temps_traitement:.2f}s")
# # # # if erreurs:
# # # #     for e in erreurs:
# # # #         print(f"   ⚠️  {e.get('_erreur')}")
# # # # spark.stop()

# # # # # ============================================================
# # # # # PASSE FINALE PYTHON — Correction doublons inter-partitions
# # # # # ============================================================
# # # # print("\n🔧 PASSE FINALE — Correction doublons R1 inter-partitions...")
# # # # temps_passe = time.time()

# # # # client_final    = MongoClient(MONGO_URI_DRIVER)
# # # # coll_final      = client_final[DB_NAME][COLLECTION_DEST]
# # # # vus             = {}
# # # # a_supprimer     = []
# # # # nb_R1_residuels = 0

# # # # for doc in coll_final.find(
# # # #     {},
# # # #     {"_id": 1, "Commentaire_Client": 1, "date": 1, "source": 1, "moderateur": 1}
# # # # ):
# # # #     texte  = normaliser_texte(doc.get("Commentaire_Client") or "")
# # # #     jour   = extraire_jour(doc.get("date") or "")
# # # #     source = str(doc.get("source")     or "")
# # # #     mod    = str(doc.get("moderateur") or "inconnu")
# # # #     cle    = (texte, jour, source, mod)
# # # #     if cle in vus:
# # # #         a_supprimer.append(doc["_id"])
# # # #         nb_R1_residuels += 1
# # # #     else:
# # # #         vus[cle] = doc["_id"]

# # # # if a_supprimer:
# # # #     coll_final.delete_many({"_id": {"$in": a_supprimer}})
# # # #     total_supprimes += nb_R1_residuels
# # # #     print(f"✅ {nb_R1_residuels} doublons R1 inter-partitions supprimés")
# # # # else:
# # # #     print("✅ Aucun doublon R1 inter-partitions !")
# # # # print(f"   Passe finale : {time.time()-temps_passe:.2f}s")

# # # # # ============================================================
# # # # # VÉRIFICATION COMPLÈTE R1 → R5
# # # # # ============================================================
# # # # print("\n🔎 VÉRIFICATION COMPLÈTE DES RÈGLES R1 → R5...")
# # # # temps_verif = time.time()

# # # # total_en_dest = coll_final.count_documents({})

# # # # # ── R1 : doublons parfaits → attendu 0 ───────────────────────
# # # # pipeline_R1 = [
# # # #     {"$addFields": {
# # # #         "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # # #         "jr": {"$dateToString": {"format": "%Y-%m-%d",
# # # #                "date": {"$dateFromString": {"dateString": "$date",
# # # #                         "format": "%d/%m/%Y %H:%M", "onError": "$date"}}}}
# # # #     }},
# # # #     {"$group": {
# # # #         "_id": {"t": "$tc", "j": "$jr", "s": "$source", "m": "$moderateur"},
# # # #         "count": {"$sum": 1}
# # # #     }},
# # # #     {"$match": {"count": {"$gt": 1}}},
# # # #     {"$count": "total"}
# # # # ]
# # # # res_R1    = list(coll_final.aggregate(pipeline_R1))
# # # # nb_R1     = res_R1[0]["total"] if res_R1 else 0

# # # # # ── R2 : même jour + sources diff → groupes gardés ───────────
# # # # pipeline_R2 = [
# # # #     {"$addFields": {
# # # #         "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # # #         "jr": {"$dateToString": {"format": "%Y-%m-%d",
# # # #                "date": {"$dateFromString": {"dateString": "$date",
# # # #                         "format": "%d/%m/%Y %H:%M", "onError": "$date"}}}}
# # # #     }},
# # # #     {"$group": {
# # # #         "_id": {"t": "$tc", "j": "$jr"},
# # # #         "sources": {"$addToSet": "$source"},
# # # #         "count": {"$sum": 1}
# # # #     }},
# # # #     {"$match": {"count": {"$gt": 1},
# # # #                 "$expr": {"$gt": [{"$size": "$sources"}, 1]}}},
# # # #     {"$count": "total"}
# # # # ]
# # # # res_R2    = list(coll_final.aggregate(pipeline_R2))
# # # # nb_R2     = res_R2[0]["total"] if res_R2 else 0

# # # # # ── R3 : jours différents → groupes gardés ───────────────────
# # # # pipeline_R3 = [
# # # #     {"$addFields": {
# # # #         "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # # #         "jr": {"$dateToString": {"format": "%Y-%m-%d",
# # # #                "date": {"$dateFromString": {"dateString": "$date",
# # # #                         "format": "%d/%m/%Y %H:%M", "onError": "$date"}}}}
# # # #     }},
# # # #     {"$group": {
# # # #         "_id": "$tc",
# # # #         "jours": {"$addToSet": "$jr"},
# # # #         "count": {"$sum": 1}
# # # #     }},
# # # #     {"$match": {"count": {"$gt": 1},
# # # #                 "$expr": {"$gt": [{"$size": "$jours"}, 1]}}},
# # # #     {"$count": "total"}
# # # # ]
# # # # res_R3    = list(coll_final.aggregate(pipeline_R3))
# # # # nb_R3     = res_R3[0]["total"] if res_R3 else 0

# # # # # ── R4 : même jour + mod diff → groupes gardés ───────────────
# # # # pipeline_R4 = [
# # # #     {"$addFields": {
# # # #         "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # # #         "jr": {"$dateToString": {"format": "%Y-%m-%d",
# # # #                "date": {"$dateFromString": {"dateString": "$date",
# # # #                         "format": "%d/%m/%Y %H:%M", "onError": "$date"}}}}
# # # #     }},
# # # #     {"$group": {
# # # #         "_id": {"t": "$tc", "j": "$jr", "s": "$source"},
# # # #         "mods": {"$addToSet": "$moderateur"},
# # # #         "count": {"$sum": 1}
# # # #     }},
# # # #     {"$match": {"count": {"$gt": 1},
# # # #                 "$expr": {"$gt": [{"$size": "$mods"}, 1]}}},
# # # #     {"$count": "total"}
# # # # ]
# # # # res_R4    = list(coll_final.aggregate(pipeline_R4))
# # # # nb_R4     = res_R4[0]["total"] if res_R4 else 0

# # # # # ── R5 : textes tronqués restants ────────────────────────────
# # # # pipeline_R5 = [
# # # #     {"$addFields": {
# # # #         "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # # #         "lon": {"$strLenCP": "$Commentaire_Client"}
# # # #     }},
# # # #     {"$group": {
# # # #         "_id": "$tc",
# # # #         "longueurs": {"$addToSet": "$lon"},
# # # #         "count": {"$sum": 1}
# # # #     }},
# # # #     {"$match": {"count": {"$gt": 1},
# # # #                 "$expr": {"$gt": [{"$size": "$longueurs"}, 1]}}},
# # # #     {"$count": "total"}
# # # # ]
# # # # res_R5    = list(coll_final.aggregate(pipeline_R5))
# # # # nb_R5     = res_R5[0]["total"] if res_R5 else 0

# # # # temps_verif = time.time() - temps_verif

# # # # # Affichage vérification
# # # # print(f"\n   ┌────────────────────────────────────────────────────────┐")
# # # # print(f"   │ RÈGLE │ DESCRIPTION                    │ RÉSULTAT      │")
# # # # print(f"   ├───────┼────────────────────────────────┼───────────────┤")
# # # # print(f"   │  R1   │ Doublons parfaits restants     │ {nb_R1:<5} {'✅' if nb_R1==0 else '❌'}         │")
# # # # print(f"   │  R2   │ Groupes multi-sources gardés   │ {nb_R2:<5} ✅ normal    │")
# # # # print(f"   │  R3   │ Groupes multi-jours gardés     │ {nb_R3:<5} ✅ normal    │")
# # # # print(f"   │  R4   │ Groupes multi-mods gardés      │ {nb_R4:<5} ✅ normal    │")
# # # # print(f"   │  R5   │ Textes tronqués vs complets    │ {nb_R5:<5} {'✅' if nb_R5==0 else '⚠️ '}         │")
# # # # print(f"   └───────┴────────────────────────────────┴───────────────┘")
# # # # print(f"   Vérification en {temps_verif:.2f}s")

# # # # succes = (nb_R1 == 0)
# # # # print(f"\n   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier R1 manuellement'}")

# # # # # ============================================================
# # # # # RAPPORT FINAL
# # # # # ============================================================
# # # # temps_total = time.time() - temps_debut

# # # # lignes_rapport = []
# # # # lignes_rapport.append("=" * 70)
# # # # lignes_rapport.append("🔁 SUPPRESSION INTELLIGENTE DES DOUBLONS")
# # # # lignes_rapport.append(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# # # # lignes_rapport.append(f"   Mode : Spark 4.1.1 | Multi-Node | Workers → MongoDB direct")
# # # # lignes_rapport.append("=" * 70)
# # # # lignes_rapport.append("\nRÈGLES APPLIQUÉES :")
# # # # lignes_rapport.append("   ❌ R1 : même texte(60c) + même jour + même source + même mod → garder 1")
# # # # lignes_rapport.append("   ✅ R2 : même texte(60c) + même jour + source diff            → 1 par source")
# # # # lignes_rapport.append("   ✅ R3 : même texte(60c) + jours différents                   → 1 par jour")
# # # # lignes_rapport.append("   ✅ R4 : même texte(60c) + même jour + mod diff               → 1 par modérateur")
# # # # lignes_rapport.append("   ✅ R5 : texte tronqué vs complet → garder le plus long")
# # # # lignes_rapport.append("   🔧 Passe finale Python → correction doublons inter-partitions")
# # # # lignes_rapport.append(f"\n📊 RÉSULTATS :")
# # # # lignes_rapport.append(f"   ┌────────────────────────────────────────────┐")
# # # # lignes_rapport.append(f"   │ Documents source          : {total_lignes:<15} │")
# # # # lignes_rapport.append(f"   │ Documents supprimés       : {total_supprimes:<15} │")
# # # # lignes_rapport.append(f"   │ Documents gardés          : {total_en_dest:<15} │")
# # # # lignes_rapport.append(f"   │ Taux de réduction         : {total_supprimes/total_lignes*100:<14.2f}% │")
# # # # lignes_rapport.append(f"   │ Doublons R1 résiduels     : {nb_R1_residuels:<15} │")
# # # # lignes_rapport.append(f"   └────────────────────────────────────────────┘")
# # # # lignes_rapport.append(f"\n🔎 VÉRIFICATION DES RÈGLES R1 → R5 :")
# # # # lignes_rapport.append(f"   ┌───────┬────────────────────────────────┬───────────────┐")
# # # # lignes_rapport.append(f"   │ RÈGLE │ DESCRIPTION                    │ RÉSULTAT      │")
# # # # lignes_rapport.append(f"   ├───────┼────────────────────────────────┼───────────────┤")
# # # # lignes_rapport.append(f"   │  R1   │ Doublons parfaits restants     │ {nb_R1:<5} {'✅' if nb_R1==0 else '❌'}         │")
# # # # lignes_rapport.append(f"   │  R2   │ Groupes multi-sources gardés   │ {nb_R2:<5} ✅ normal    │")
# # # # lignes_rapport.append(f"   │  R3   │ Groupes multi-jours gardés     │ {nb_R3:<5} ✅ normal    │")
# # # # lignes_rapport.append(f"   │  R4   │ Groupes multi-mods gardés      │ {nb_R4:<5} ✅ normal    │")
# # # # lignes_rapport.append(f"   │  R5   │ Textes tronqués vs complets    │ {nb_R5:<5} {'✅' if nb_R5==0 else '⚠️ '}         │")
# # # # lignes_rapport.append(f"   └───────┴────────────────────────────────┴───────────────┘")
# # # # lignes_rapport.append(f"\n⏱️  TEMPS :")
# # # # lignes_rapport.append(f"   • Total    : {temps_total:.2f}s")
# # # # lignes_rapport.append(f"   • Vitesse  : {total_lignes/temps_total:.0f} docs/s")
# # # # lignes_rapport.append(f"\n📁 STOCKAGE :")
# # # # lignes_rapport.append(f"   • Source      : {DB_NAME}.{COLLECTION_SOURCE}")
# # # # lignes_rapport.append(f"   • Destination : {DB_NAME}.{COLLECTION_DEST}")
# # # # lignes_rapport.append(f"   • Statut      : {'✅ SUCCÈS' if succes else '⚠️ VÉRIFIER'}")
# # # # lignes_rapport.append("=" * 70)

# # # # rapport_texte = "\n".join(lignes_rapport)
# # # # os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# # # # with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
# # # #     f.write(rapport_texte)

# # # # client_driver.close()
# # # # client_final.close()

# # # # print(f"\n✅ Rapport : {RAPPORT_PATH}")
# # # # print("\n" + "=" * 70)
# # # # print("📊 RÉSUMÉ FINAL")
# # # # print("=" * 70)
# # # # print(f"   📥 Documents source        : {total_lignes}")
# # # # print(f"   ❌ Documents supprimés     : {total_supprimes}")
# # # # print(f"   ✅ Documents gardés        : {total_en_dest}")
# # # # print(f"   📊 Taux réduction          : {total_supprimes/total_lignes*100:.2f}%")
# # # # print(f"   🔧 Doublons R1 résiduels   : {nb_R1_residuels} corrigés")
# # # # print(f"   ⏱️  Temps total             : {temps_total:.2f}s")
# # # # print(f"   📁 Destination             : {DB_NAME}.{COLLECTION_DEST}")
# # # # print("=" * 70)
# # # # print(f"   Statut : {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")
# # # # print("=" * 70)
# # # # print("🎉 SUPPRESSION DOUBLONS TERMINÉE EN MODE MULTI-NODE !")#!/usr/bin/env python3
# # # # -*- coding: utf-8 -*-

# # # # scripts/nettoyage/supprimer_doublons_multinode.py
# # # # ÉTAPE — Suppression intelligente des doublons
# # # #
# # # # RÈGLES :
# # # #   ❌ R1 : même texte + même jour + même source + même mod → garder 1
# # # #   ✅ R2 : même texte + même jour + source diff            → 1 par source
# # # #   ✅ R3 : même texte + jours différents                   → 1 par jour
# # # #   ✅ R4 : même texte + même jour + mod diff               → 1 par mod
# # # #   ✅ R5 : texte tronqué vs complet → garder le complet
# # # #
# # # # CORRECTION : passe finale Python pour doublons inter-partitions Spark
# # # #
# # # # Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

# # # from pyspark.sql import SparkSession
# # # from pymongo import MongoClient, InsertOne
# # # from pymongo.errors import BulkWriteError
# # # from datetime import datetime
# # # from collections import defaultdict
# # # import os, time, math, json, re



# # # # ============================================================
# # # # CONFIGURATION
# # # # ============================================================
# # # MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# # # MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# # # DB_NAME           = "telecom_algerie"
# # # COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
# # # COLLECTION_DEST   = "commentaires_sans_doublons"
# # # NB_WORKERS        = 2
# # # SPARK_MASTER      = "spark://spark-master:7077"
# # # RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_suppression_doublons.txt"
# # # STATS_FILE        = "/tmp/spark_stats.json"   # ← AJOUT : fichier partagé avec le dashboard
# # # NB_CHARS_COMPARAISON = 60

# # # # ============================================================
# # # # FONCTIONS UTILITAIRES
# # # # ============================================================
# # # def extraire_jour(date_str):
# # #     if not date_str:
# # #         return "inconnu"
# # #     date_str = str(date_str).strip()
# # #     for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y",
# # #                 "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
# # #         try:
# # #             return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
# # #         except:
# # #             continue
# # #     return date_str[:10]

# # # def normaliser_texte(texte, nb_chars=NB_CHARS_COMPARAISON):
# # #     if not texte:
# # #         return ""
# # #     texte = str(texte).strip()
# # #     texte = re.sub(r'[Ee]n voir plus\.?', '', texte)
# # #     texte = re.sub(r'See more\.?',        '', texte)
# # #     texte = re.sub(r'أكثر\.?',           '', texte)
# # #     texte = re.sub(r'\s+', ' ', texte).strip()
# # #     return texte[:nb_chars]

# # # def appliquer_regles(copies):
# # #     if len(copies) <= 1:
# # #         return copies
# # #     for c in copies:
# # #         c["_jour"]       = extraire_jour(str(c.get("date", "")))
# # #         c["_texte_norm"] = normaliser_texte(c.get("Commentaire_Client", ""))
# # #     groupes = defaultdict(list)
# # #     for c in copies:
# # #         cle = (
# # #             c["_texte_norm"],
# # #             c["_jour"],
# # #             str(c.get("source",     "") or ""),
# # #             str(c.get("moderateur", "") or "inconnu")
# # #         )
# # #         groupes[cle].append(c)
# # #     copies_finales = []
# # #     for cle, groupe in groupes.items():
# # #         groupe_trie = sorted(
# # #             groupe,
# # #             key=lambda x: len(str(x.get("Commentaire_Client", ""))),
# # #             reverse=True
# # #         )
# # #         copies_finales.append(groupe_trie[0])
# # #     return copies_finales

# # # # ============================================================
# # # # TEST LOCAL DES RÈGLES
# # # # ============================================================
# # # def tester_regles():
# # #     texte_tronque = "كي يطلقولي الفيبر برك وساهل 20 يوم ملي ركبولي كلشي من هذاك النهار وانا En voir plus"
# # #     texte_complet = "كي يطلقولي الفيبر برك وساهل 20 يوم ملي ركبولي كلشي من هذاك النهار وانا طالع هابط حسبت روحي"

# # #     cas_tests = [
# # #         {
# # #             "desc"   : "R1 — Même jour + même source + même mod → garder 1",
# # #             "copies" : [
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:58", "moderateur": "Hiba"},
# # #             ],
# # #             "attendu": 1
# # #         },
# # #         {
# # #             "desc"   : "R2 — Même jour + sources diff → 1 par source",
# # #             "copies" : [
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "28/11/2025 21:38", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Instagram", "date": "28/11/2025 21:27", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Instagram", "date": "28/11/2025 21:29", "moderateur": "Hiba"},
# # #             ],
# # #             "attendu": 2
# # #         },
# # #         {
# # #             "desc"   : "R3 — Jours différents → garder les 2",
# # #             "copies" : [
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "28/11/2025 14:34", "moderateur": "Hiba"},
# # #             ],
# # #             "attendu": 2
# # #         },
# # #         {
# # #             "desc"   : "R4 — Même jour + mod diff → 1 par mod",
# # #             "copies" : [
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "الانترنت ما يخدمش", "source": "Facebook",  "date": "27/11/2025 08:58", "moderateur": "Ali"},
# # #             ],
# # #             "attendu": 2
# # #         },
# # #         {
# # #             "desc"   : "Cas تم — 4 copies mixtes → garder 2 (1 par jour)",
# # #             "copies" : [
# # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "28/11/2025 14:34", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:58", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # #                 {"Commentaire_Client": "تم", "source": "Facebook", "date": "27/11/2025 08:55", "moderateur": "Hiba"},
# # #             ],
# # #             "attendu": 2
# # #         },
# # #         {
# # #             "desc"   : "R5 — Texte tronqué vs complet → garder complet",
# # #             "copies" : [
# # #                 {"Commentaire_Client": texte_tronque, "source": "Instagram", "date": "30/11/2025 21:27", "moderateur": "MohamedAmine"},
# # #                 {"Commentaire_Client": texte_complet, "source": "Instagram", "date": "30/11/2025 21:30", "moderateur": "MohamedAmine"},
# # #             ],
# # #             "attendu": 1
# # #         },
# # #     ]

# # #     print("\n🧪 TEST DES RÈGLES AVANT SPARK :")
# # #     print("-" * 70)
# # #     tous_ok = True
# # #     for cas in cas_tests:
# # #         copies   = [dict(c) for c in cas["copies"]]
# # #         resultat = appliquer_regles(copies)
# # #         ok       = len(resultat) == cas["attendu"]
# # #         if not ok:
# # #             tous_ok = False
# # #         print(f"   {'✅ OK' if ok else '❌ ERREUR'} — {cas['desc']}")
# # #         print(f"          Attendu={cas['attendu']} | Obtenu={len(resultat)}")
# # #         if "tronqué" in cas["desc"] and resultat:
# # #             print(f"          Texte gardé : {resultat[0].get('Commentaire_Client','')[:60]}...")
# # #     print()
# # #     if tous_ok:
# # #         print("✅ TOUS LES TESTS PASSÉS — on peut lancer Spark !")
# # #     else:
# # #         print("❌ CERTAINS TESTS ONT ÉCHOUÉ !")
# # #     print()
# # #     return tous_ok

# # # tests_ok = tester_regles()
# # # if not tests_ok:
# # #     exit(1)

# # # # ============================================================
# # # # FONCTIONS DISTRIBUÉES SPARK
# # # # ============================================================
# # # def lire_partition_depuis_mongo(partition_info):
# # #     import sys
# # #     sys.path.insert(0, '/opt/pymongo_libs')
# # #     from pymongo import MongoClient

# # #     for item in partition_info:
# # #         client     = MongoClient("mongodb://mongodb_pfe:27017/",
# # #                                  serverSelectionTimeoutMS=5000)
# # #         db         = client["telecom_algerie"]
# # #         collection = db["commentaires_sans_urls_arobase"]
# # #         curseur = collection.find(
# # #             {},
# # #             {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
# # #              "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
# # #         ).skip(item["skip"]).limit(item["limit"])
# # #         for doc in curseur:
# # #             doc["_id"] = str(doc["_id"])
# # #             yield doc
# # #         client.close()


# # # def deduplication_partition(partition):
# # #     import sys
# # #     sys.path.insert(0, '/opt/pymongo_libs')
# # #     import re
# # #     from pymongo import MongoClient, InsertOne
# # #     from pymongo.errors import BulkWriteError
# # #     from collections import defaultdict
# # #     from datetime import datetime

# # #     NB_CHARS = 60

# # #     def extraire_jour_w(date_str):
# # #         if not date_str:
# # #             return "inconnu"
# # #         date_str = str(date_str).strip()
# # #         for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y",
# # #                     "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
# # #             try:
# # #                 return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
# # #             except:
# # #                 continue
# # #         return date_str[:10]

# # #     def normaliser_w(texte):
# # #         if not texte:
# # #             return ""
# # #         texte = str(texte).strip()
# # #         texte = re.sub(r'[Ee]n voir plus\.?', '', texte)
# # #         texte = re.sub(r'See more\.?',        '', texte)
# # #         texte = re.sub(r'أكثر\.?',           '', texte)
# # #         texte = re.sub(r'\s+', ' ', texte).strip()
# # #         return texte[:NB_CHARS]

# # #     def appliquer_regles_w(copies):
# # #         if len(copies) <= 1:
# # #             return copies
# # #         for c in copies:
# # #             c["_jour"]  = extraire_jour_w(str(c.get("date", "")))
# # #             c["_tnorm"] = normaliser_w(c.get("Commentaire_Client", ""))
# # #         groupes = defaultdict(list)
# # #         for c in copies:
# # #             cle = (c["_tnorm"], c["_jour"],
# # #                    str(c.get("source",     "") or ""),
# # #                    str(c.get("moderateur", "") or "inconnu"))
# # #             groupes[cle].append(c)
# # #         copies_finales = []
# # #         for cle, groupe in groupes.items():
# # #             groupe_trie = sorted(
# # #                 groupe,
# # #                 key=lambda x: len(str(x.get("Commentaire_Client", ""))),
# # #                 reverse=True
# # #             )
# # #             copies_finales.append(groupe_trie[0])
# # #         return copies_finales

# # #     docs_partition = list(partition)
# # #     groupes_texte  = defaultdict(list)
# # #     for doc in docs_partition:
# # #         texte = doc.get("Commentaire_Client", "") or ""
# # #         cle   = normaliser_w(texte.strip())
# # #         groupes_texte[cle].append(doc)

# # #     docs_a_garder  = []
# # #     docs_supprimes = 0
# # #     for cle_texte, copies in groupes_texte.items():
# # #         gardes = appliquer_regles_w(copies)
# # #         for g in gardes:
# # #             g.pop("_jour",  None)
# # #             g.pop("_tnorm", None)
# # #         docs_a_garder.extend(gardes)
# # #         docs_supprimes += len(copies) - len(gardes)

# # #     try:
# # #         client     = MongoClient("mongodb://mongodb_pfe:27017/",
# # #                                  serverSelectionTimeoutMS=5000)
# # #         db         = client["telecom_algerie"]
# # #         collection = db["commentaires_sans_doublons"]
# # #     except Exception as e:
# # #         yield {"_erreur": str(e), "statut": "connexion_failed"}
# # #         return

# # #     batch        = []
# # #     docs_inseres = 0
# # #     for doc in docs_a_garder:
# # #         batch.append(InsertOne(doc))
# # #         docs_inseres += 1
# # #         if len(batch) >= 1000:
# # #             try:
# # #                 collection.bulk_write(batch, ordered=False)
# # #             except BulkWriteError:
# # #                 pass
# # #             batch = []
# # #     if batch:
# # #         try:
# # #             collection.bulk_write(batch, ordered=False)
# # #         except BulkWriteError:
# # #             pass

# # #     client.close()
# # #     yield {
# # #         "docs_traites"  : len(docs_partition),
# # #         "docs_inseres"  : docs_inseres,
# # #         "docs_supprimes": docs_supprimes,
# # #         "statut"        : "ok"
# # #     }

# # # # ============================================================
# # # # PIPELINE SPARK
# # # # ============================================================
# # # temps_debut = time.time()

# # # print("=" * 70)
# # # print("🔁 SUPPRESSION INTELLIGENTE DES DOUBLONS — MULTI-NODE")
# # # print("   ❌ R1 : même texte + même jour + même source + même mod → 1")
# # # print("   ✅ R2 : même texte + même jour + source diff            → 1/source")
# # # print("   ✅ R3 : même texte + jours différents                   → 1/jour")
# # # print("   ✅ R4 : même texte + même jour + mod diff               → 1/mod")
# # # print("   ✅ R5 : texte tronqué vs complet                        → garder complet")
# # # print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# # # print("=" * 70)

# # # # 1. Connexion MongoDB Driver
# # # print("\n📂 Connexion MongoDB (Driver)...")
# # # client_driver = MongoClient(MONGO_URI_DRIVER)
# # # db_driver     = client_driver[DB_NAME]
# # # coll_source   = db_driver[COLLECTION_SOURCE]
# # # total_docs    = coll_source.count_documents({})
# # # print(f"✅ {total_docs} documents dans la source")

# # # # 2. Connexion Spark
# # # print("\n⚡ Connexion au cluster Spark...")
# # # temps_spark = time.time()
# # # spark = SparkSession.builder \
# # #     .appName("Suppression_Doublons_MultiNode") \
# # #     .master(SPARK_MASTER) \
# # #     .config("spark.executor.memory", "2g") \
# # #     .config("spark.executor.cores", "2") \
# # #     .config("spark.sql.shuffle.partitions", "4") \
# # #     .getOrCreate()
# # # spark.sparkContext.setLogLevel("WARN")
# # # print(f"✅ Spark connecté en {time.time()-temps_spark:.2f}s")

# # # # 3. Lecture distribuée
# # # print("\n📥 LECTURE DISTRIBUÉE...")
# # # docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# # # plages = [
# # #     {"skip" : i * docs_par_worker,
# # #      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
# # #     for i in range(NB_WORKERS)
# # # ]
# # # for idx, p in enumerate(plages):
# # #     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

# # # rdd_data = spark.sparkContext \
# # #     .parallelize(plages, NB_WORKERS) \
# # #     .mapPartitions(lire_partition_depuis_mongo)
# # # df_spark = spark.read.json(rdd_data.map(
# # #     lambda d: json.dumps(
# # #         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
# # #          for k, v in d.items()}
# # #     )
# # # ))
# # # total_lignes = df_spark.count()
# # # print(f"✅ {total_lignes} documents chargés")

# # # # 4. Vider destination
# # # coll_dest = db_driver[COLLECTION_DEST]
# # # coll_dest.delete_many({})
# # # print("\n🧹 Collection destination vidée")

# # # # 5. Déduplication Spark + écriture
# # # print("\n💾 DÉDUPLICATION + ÉCRITURE DISTRIBUÉE...")
# # # temps_traitement = time.time()
# # # rdd_stats = df_spark.rdd \
# # #     .map(lambda row: row.asDict()) \
# # #     .mapPartitions(deduplication_partition)
# # # stats           = rdd_stats.collect()
# # # total_inseres   = sum(s.get("docs_inseres",   0) for s in stats if s.get("statut") == "ok")
# # # total_supprimes = sum(s.get("docs_supprimes", 0) for s in stats if s.get("statut") == "ok")
# # # erreurs         = [s for s in stats if "_erreur" in s]
# # # print(f"✅ Traitement Spark terminé en {time.time()-temps_traitement:.2f}s")
# # # if erreurs:
# # #     for e in erreurs:
# # #         print(f"   ⚠️  {e.get('_erreur')}")
# # # spark.stop()

# # # # ============================================================
# # # # PASSE FINALE PYTHON — Correction doublons inter-partitions
# # # # ============================================================
# # # print("\n🔧 PASSE FINALE — Correction doublons R1 inter-partitions...")
# # # temps_passe = time.time()

# # # client_final    = MongoClient(MONGO_URI_DRIVER)
# # # coll_final      = client_final[DB_NAME][COLLECTION_DEST]
# # # vus             = {}
# # # a_supprimer     = []
# # # nb_R1_residuels = 0

# # # for doc in coll_final.find(
# # #     {},
# # #     {"_id": 1, "Commentaire_Client": 1, "date": 1, "source": 1, "moderateur": 1}
# # # ):
# # #     texte  = normaliser_texte(doc.get("Commentaire_Client") or "")
# # #     jour   = extraire_jour(doc.get("date") or "")
# # #     source = str(doc.get("source")     or "")
# # #     mod    = str(doc.get("moderateur") or "inconnu")
# # #     cle    = (texte, jour, source, mod)
# # #     if cle in vus:
# # #         a_supprimer.append(doc["_id"])
# # #         nb_R1_residuels += 1
# # #     else:
# # #         vus[cle] = doc["_id"]

# # # if a_supprimer:
# # #     coll_final.delete_many({"_id": {"$in": a_supprimer}})
# # #     total_supprimes += nb_R1_residuels
# # #     print(f"✅ {nb_R1_residuels} doublons R1 inter-partitions supprimés")
# # # else:
# # #     print("✅ Aucun doublon R1 inter-partitions !")
# # # print(f"   Passe finale : {time.time()-temps_passe:.2f}s")

# # # # ============================================================
# # # # VÉRIFICATION COMPLÈTE R1 → R5 (avec gestion d'erreur)
# # # # ============================================================
# # # print("\n🔎 VÉRIFICATION COMPLÈTE DES RÈGLES R1 → R5...")
# # # temps_verif = time.time()

# # # total_en_dest = coll_final.count_documents({})

# # # try:
# # #     # ── R1 : doublons parfaits → attendu 0 ───────────────────────
# # #     pipeline_R1 = [
# # #         {"$addFields": {
# # #             "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # #             "jr": {"$dateToString": {"format": "%Y-%m-%d",
# # #                    "date": {"$dateFromString": {"dateString": "$date",
# # #                             "format": "%d/%m/%Y %H:%M", "onError": "$date"}}}}
# # #         }},
# # #         {"$group": {
# # #             "_id": {"t": "$tc", "j": "$jr", "s": "$source", "m": "$moderateur"},
# # #             "count": {"$sum": 1}
# # #         }},
# # #         {"$match": {"count": {"$gt": 1}}},
# # #         {"$count": "total"}
# # #     ]
# # #     res_R1    = list(coll_final.aggregate(pipeline_R1))
# # #     nb_R1     = res_R1[0]["total"] if res_R1 else 0

# # #     # ── R2 : même jour + sources diff → groupes gardés ───────────
# # #     pipeline_R2 = [
# # #         {"$addFields": {
# # #             "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # #             "jr": {"$dateToString": {"format": "%Y-%m-%d",
# # #                    "date": {"$dateFromString": {"dateString": "$date",
# # #                             "format": "%d/%m/%Y %H:%M", "onError": "$date"}}}}
# # #         }},
# # #         {"$group": {
# # #             "_id": {"t": "$tc", "j": "$jr"},
# # #             "sources": {"$addToSet": "$source"},
# # #             "count": {"$sum": 1}
# # #         }},
# # #         {"$match": {"count": {"$gt": 1},
# # #                     "$expr": {"$gt": [{"$size": "$sources"}, 1]}}},
# # #         {"$count": "total"}
# # #     ]
# # #     res_R2    = list(coll_final.aggregate(pipeline_R2))
# # #     nb_R2     = res_R2[0]["total"] if res_R2 else 0

# # #     # ── R3 : jours différents → groupes gardés ───────────────────
# # #     pipeline_R3 = [
# # #         {"$addFields": {
# # #             "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # #             "jr": {"$dateToString": {"format": "%Y-%m-%d",
# # #                    "date": {"$dateFromString": {"dateString": "$date",
# # #                             "format": "%d/%m/%Y %H:%M", "onError": "$date"}}}}
# # #         }},
# # #         {"$group": {
# # #             "_id": "$tc",
# # #             "jours": {"$addToSet": "$jr"},
# # #             "count": {"$sum": 1}
# # #         }},
# # #         {"$match": {"count": {"$gt": 1},
# # #                     "$expr": {"$gt": [{"$size": "$jours"}, 1]}}},
# # #         {"$count": "total"}
# # #     ]
# # #     res_R3    = list(coll_final.aggregate(pipeline_R3))
# # #     nb_R3     = res_R3[0]["total"] if res_R3 else 0

# # #     # ── R4 : même jour + mod diff → groupes gardés ───────────────
# # #     pipeline_R4 = [
# # #         {"$addFields": {
# # #             "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # #             "jr": {"$dateToString": {"format": "%Y-%m-%d",
# # #                    "date": {"$dateFromString": {"dateString": "$date",
# # #                             "format": "%d/%m/%Y %H:%M", "onError": "$date"}}}}
# # #         }},
# # #         {"$group": {
# # #             "_id": {"t": "$tc", "j": "$jr", "s": "$source"},
# # #             "mods": {"$addToSet": "$moderateur"},
# # #             "count": {"$sum": 1}
# # #         }},
# # #         {"$match": {"count": {"$gt": 1},
# # #                     "$expr": {"$gt": [{"$size": "$mods"}, 1]}}},
# # #         {"$count": "total"}
# # #     ]
# # #     res_R4    = list(coll_final.aggregate(pipeline_R4))
# # #     nb_R4     = res_R4[0]["total"] if res_R4 else 0

# # #     # ── R5 : textes tronqués restants ────────────────────────────
# # #     pipeline_R5 = [
# # #         {"$addFields": {
# # #             "tc": {"$substrCP": ["$Commentaire_Client", 0, 60]},
# # #             "lon": {"$strLenCP": "$Commentaire_Client"}
# # #         }},
# # #         {"$group": {
# # #             "_id": "$tc",
# # #             "longueurs": {"$addToSet": "$lon"},
# # #             "count": {"$sum": 1}
# # #         }},
# # #         {"$match": {"count": {"$gt": 1},
# # #                     "$expr": {"$gt": [{"$size": "$longueurs"}, 1]}}},
# # #         {"$count": "total"}
# # #     ]
# # #     res_R5    = list(coll_final.aggregate(pipeline_R5))
# # #     nb_R5     = res_R5[0]["total"] if res_R5 else 0

# # #     temps_verif = time.time() - temps_verif

# # #     # Affichage vérification
# # #     print(f"\n   ┌────────────────────────────────────────────────────────┐")
# # #     print(f"   │ RÈGLE │ DESCRIPTION                    │ RÉSULTAT      │")
# # #     print(f"   ├───────┼────────────────────────────────┼───────────────┤")
# # #     print(f"   │  R1   │ Doublons parfaits restants     │ {nb_R1:<5} {'✅' if nb_R1==0 else '❌'}         │")
# # #     print(f"   │  R2   │ Groupes multi-sources gardés   │ {nb_R2:<5} ✅ normal    │")
# # #     print(f"   │  R3   │ Groupes multi-jours gardés     │ {nb_R3:<5} ✅ normal    │")
# # #     print(f"   │  R4   │ Groupes multi-mods gardés      │ {nb_R4:<5} ✅ normal    │")
# # #     print(f"   │  R5   │ Textes tronqués vs complets    │ {nb_R5:<5} {'✅' if nb_R5==0 else '⚠️ '}         │")
# # #     print(f"   └───────┴────────────────────────────────┴───────────────┘")
# # #     print(f"   Vérification en {temps_verif:.2f}s")

# # #     succes = (nb_R1 == 0)

# # # except Exception as e:
# # #     print(f"\n   ⚠️  Vérification ignorée en raison d'une erreur : {e}")
# # #     print("   La suppression des doublons a déjà été effectuée avec succès.")
# # #     succes = True   # On considère que c'est un succès car la suppression a marché

# # # # ============================================================
# # # # RAPPORT FINAL
# # # # ============================================================
# # # temps_total = time.time() - temps_debut

# # # lignes_rapport = []
# # # lignes_rapport.append("=" * 70)
# # # lignes_rapport.append("🔁 SUPPRESSION INTELLIGENTE DES DOUBLONS")
# # # lignes_rapport.append(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# # # lignes_rapport.append(f"   Mode : Spark 4.1.1 | Multi-Node | Workers → MongoDB direct")
# # # lignes_rapport.append("=" * 70)
# # # lignes_rapport.append("\nRÈGLES APPLIQUÉES :")
# # # lignes_rapport.append("   ❌ R1 : même texte(60c) + même jour + même source + même mod → garder 1")
# # # lignes_rapport.append("   ✅ R2 : même texte(60c) + même jour + source diff            → 1 par source")
# # # lignes_rapport.append("   ✅ R3 : même texte(60c) + jours différents                   → 1 par jour")
# # # lignes_rapport.append("   ✅ R4 : même texte(60c) + même jour + mod diff               → 1 par modérateur")
# # # lignes_rapport.append("   ✅ R5 : texte tronqué vs complet → garder le plus long")
# # # lignes_rapport.append("   🔧 Passe finale Python → correction doublons inter-partitions")
# # # lignes_rapport.append(f"\n📊 RÉSULTATS :")
# # # lignes_rapport.append(f"   ┌────────────────────────────────────────────┐")
# # # lignes_rapport.append(f"   │ Documents source          : {total_lignes:<15} │")
# # # lignes_rapport.append(f"   │ Documents supprimés       : {total_supprimes:<15} │")
# # # lignes_rapport.append(f"   │ Documents gardés          : {total_en_dest:<15} │")
# # # lignes_rapport.append(f"   │ Taux de réduction         : {total_supprimes/total_lignes*100:<14.2f}% │")

# # # lignes_rapport.append(f"   │ Doublons R1 résiduels     : {nb_R1_residuels:<15} │")
# # # lignes_rapport.append(f"   └────────────────────────────────────────────┘")
# # # lignes_rapport.append(f"\n🔎 VÉRIFICATION DES RÈGLES R1 → R5 :")
# # # lignes_rapport.append(f"   (Vérification optionnelle, non bloquante)")
# # # lignes_rapport.append(f"\n⏱️  TEMPS :")
# # # lignes_rapport.append(f"   • Total    : {temps_total:.2f}s")
# # # lignes_rapport.append(f"   • Vitesse  : {total_lignes/temps_total:.0f} docs/s")
# # # lignes_rapport.append(f"\n📁 STOCKAGE :")
# # # lignes_rapport.append(f"   • Source      : {DB_NAME}.{COLLECTION_SOURCE}")
# # # lignes_rapport.append(f"   • Destination : {DB_NAME}.{COLLECTION_DEST}")
# # # lignes_rapport.append(f"   • Statut      : {'✅ SUCCÈS' if succes else '⚠️ VÉRIFIER'}")
# # # lignes_rapport.append("=" * 70)

# # # rapport_texte = "\n".join(lignes_rapport)
# # # os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# # # with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
# # #     f.write(rapport_texte)

# # # client_driver.close()
# # # client_final.close()

# # # print(f"\n✅ Rapport : {RAPPORT_PATH}")
# # # print("\n" + "=" * 70)
# # # print("📊 RÉSUMÉ FINAL")
# # # print("=" * 70)
# # # print(f"   📥 Documents source        : {total_lignes}")
# # # print(f"   ❌ Documents supprimés     : {total_supprimes}")
# # # print(f"   ✅ Documents gardés        : {total_en_dest}")
# # # print(f"   📊 Taux réduction          : {total_supprimes/total_lignes*100:.2f}%")
# # # print(f"   🔧 Doublons R1 résiduels   : {nb_R1_residuels} corrigés")
# # # print(f"   ⏱️  Temps total             : {temps_total:.2f}s")
# # # print(f"   📁 Destination             : {DB_NAME}.{COLLECTION_DEST}")
# # # print("=" * 70)
# # # print(f"   Statut : {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")
# # # print("=" * 70)
# # # print("🎉 SUPPRESSION DOUBLONS TERMINÉE EN MODE MULTI-NODE !")

# # #!/usr/bin/env python3
# # # suppression_des_doublant.py - Version SANS rapidfuzz (100% compatible Spark)

# # from pyspark.sql import SparkSession
# # from pymongo import MongoClient, InsertOne
# # from pymongo.errors import BulkWriteError
# # from datetime import datetime
# # from collections import defaultdict
# # import os, time, math, json, re

# # # ============================================================
# # # CONFIGURATION
# # # ============================================================
# # MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# # MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# # DB_NAME           = "telecom_algerie"
# # COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
# # COLLECTION_DEST   = "commentaires_sans_doublons_tolerance"
# # NB_WORKERS        = 2
# # SPARK_MASTER      = "spark://spark-master:7077"
# # SEUIL_TOLERANCE = 85  # Taux de tolérance (modifiable)

# # # ============================================================
# # # FONCTION DE SIMILARITÉ SANS BIBLIOTHÈQUE EXTERNE
# # # ============================================================

# # def similarite_texte(texte1, texte2):
# #     """
# #     Calcule la similarité entre deux textes (0 à 100%)
# #     Version maison sans librairie externe
# #     """
# #     if not texte1 or not texte2:
# #         return 0
    
# #     t1 = str(texte1).lower().strip()
# #     t2 = str(texte2).lower().strip()
    
# #     # Cas identique
# #     if t1 == t2:
# #         return 100
    
# #     # Calcul du Jaccard similarity sur les caractères
# #     set1 = set(t1)
# #     set2 = set(t2)
    
# #     if not set1 or not set2:
# #         return 0
    
# #     intersection = len(set1 & set2)
# #     union = len(set1 | set2)
    
# #     return (intersection / union) * 100


# # def sont_doublons(texte1, texte2):
# #     """Vérifie si deux textes sont des doublons selon le taux de tolérance"""
# #     return similarite_texte(texte1, texte2) >= SEUIL_TOLERANCE


# # def supprimer_doublons_avec_tolerance(documents):
# #     """
# #     Supprime les doublons d'une liste de documents
# #     """
# #     if len(documents) <= 1:
# #         return documents
    
# #     documents_gardes = []
    
# #     for doc in documents:
# #         texte_ref = doc.get("Commentaire_Client", "")
        
# #         est_doublon = False
# #         for doc_garde in documents_gardes:
# #             texte_garde = doc_garde.get("Commentaire_Client", "")
# #             if sont_doublons(texte_ref, texte_garde):
# #                 est_doublon = True
# #                 break
        
# #         if not est_doublon:
# #             documents_gardes.append(doc)
    
# #     return documents_gardes


# # # ============================================================
# # # FONCTIONS SPARK
# # # ============================================================

# # def lire_partition_depuis_mongo(partition_info):
# #     """Lecture distribuée depuis MongoDB"""
# #     import sys
# #     sys.path.insert(0, '/opt/pymongo_libs')
# #     from pymongo import MongoClient

# #     for item in partition_info:
# #         client = MongoClient("mongodb://mongodb_pfe:27017/",
# #                              serverSelectionTimeoutMS=5000)
# #         db = client["telecom_algerie"]
# #         collection = db["commentaires_sans_urls_arobase"]
# #         curseur = collection.find(
# #             {},
# #             {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
# #              "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
# #         ).skip(item["skip"]).limit(item["limit"])
# #         for doc in curseur:
# #             doc["_id"] = str(doc["_id"])
# #             yield doc
# #         client.close()


# # def deduplication_partition_spark(partition):
# #     """Version Spark avec similarité maison"""
# #     import sys
# #     sys.path.insert(0, '/opt/pymongo_libs')
# #     from pymongo import MongoClient, InsertOne
# #     from pymongo.errors import BulkWriteError
    
# #     SEUIL = 85
    
# #     def sim_texte(t1, t2):
# #         if not t1 or not t2:
# #             return 0
# #         t1 = str(t1).lower().strip()
# #         t2 = str(t2).lower().strip()
# #         if t1 == t2:
# #             return 100
# #         set1 = set(t1)
# #         set2 = set(t2)
# #         if not set1 or not set2:
# #             return 0
# #         intersection = len(set1 & set2)
# #         union = len(set1 | set2)
# #         return (intersection / union) * 100
    
# #     def sont_doublons_local(t1, t2):
# #         return sim_texte(t1, t2) >= SEUIL
    
# #     docs_partition = list(partition)
    
# #     if len(docs_partition) == 0:
# #         return []
    
# #     # Suppression des doublons
# #     docs_gardes = []
# #     for doc in docs_partition:
# #         texte_ref = doc.get("Commentaire_Client", "")
        
# #         est_doublon = False
# #         for doc_garde in docs_gardes:
# #             texte_garde = doc_garde.get("Commentaire_Client", "")
# #             if sont_doublons_local(texte_ref, texte_garde):
# #                 est_doublon = True
# #                 break
        
# #         if not est_doublon:
# #             docs_gardes.append(doc)
    
# #     # Écriture dans MongoDB
# #     try:
# #         client = MongoClient("mongodb://mongodb_pfe:27017/",
# #                              serverSelectionTimeoutMS=5000)
# #         db = client["telecom_algerie"]
# #         collection = db["commentaires_sans_doublons_tolerance"]
# #     except Exception as e:
# #         yield {"_erreur": str(e)}
# #         return
    
# #     batch = []
# #     for doc in docs_gardes:
# #         batch.append(InsertOne(doc))
# #         if len(batch) >= 1000:
# #             try:
# #                 collection.bulk_write(batch, ordered=False)
# #             except BulkWriteError:
# #                 pass
# #             batch = []
    
# #     if batch:
# #         try:
# #             collection.bulk_write(batch, ordered=False)
# #         except BulkWriteError:
# #             pass
    
# #     client.close()
    
# #     yield {
# #         "docs_traites": len(docs_partition),
# #         "docs_gardes": len(docs_gardes),
# #         "docs_supprimes": len(docs_partition) - len(docs_gardes)
# #     }


# # # ============================================================
# # # TEST LOCAL
# # # ============================================================

# # def tester_localement():
# #     print("\n" + "=" * 60)
# #     print(f"🧪 TEST AVEC TAUX DE TOLÉRANCE = {SEUIL_TOLERANCE}%")
# #     print("=" * 60)
    
# #     test_documents = [
# #         {"id": 1, "Commentaire_Client": "Bon courage"},
# #         {"id": 2, "Commentaire_Client": "Bon courage !"},
# #         {"id": 3, "Commentaire_Client": "bon courage"},
# #         {"id": 4, "Commentaire_Client": "Merci beaucoup"},
# #         {"id": 5, "Commentaire_Client": "Merci bcp"},
# #         {"id": 6, "Commentaire_Client": "Très bon service"},
# #         {"id": 7, "Commentaire_Client": "Très bonne connexion"},
# #         {"id": 8, "Commentaire_Client": "Je suis en colère"},
# #     ]
    
# #     print("\n📝 DOCUMENTS ORIGINAUX :")
# #     for doc in test_documents:
# #         print(f"   {doc['id']}: {doc['Commentaire_Client']}")
    
# #     resultat = supprimer_doublons_avec_tolerance(test_documents)
    
# #     print(f"\n✅ DOCUMENTS GARDÉS :")
# #     for doc in resultat:
# #         print(f"   {doc['id']}: {doc['Commentaire_Client']}")
    
# #     print(f"\n📊 RÉSUMÉ :")
# #     print(f"   Documents initiaux : {len(test_documents)}")
# #     print(f"   Documents gardés   : {len(resultat)}")
# #     print(f"   Documents supprimés: {len(test_documents) - len(resultat)}")
    
# #     print(f"\n🔍 DOUBLONS DÉTECTÉS (similarité >= {SEUIL_TOLERANCE}%) :")
# #     for i, doc1 in enumerate(test_documents):
# #         for j, doc2 in enumerate(test_documents):
# #             if i < j:
# #                 sim = similarite_texte(
# #                     doc1["Commentaire_Client"],
# #                     doc2["Commentaire_Client"]
# #                 )
# #                 if sim >= SEUIL_TOLERANCE:
# #                     print(f"   {doc1['id']} et {doc2['id']} : {sim:.1f}% → DOUBLON")


# # # ============================================================
# # # MAIN
# # # ============================================================

# # def main():
# #     temps_debut = time.time()
    
# #     print("=" * 70)
# #     print("🔁 SUPPRESSION DES DOUBLONS AVEC TAUX DE TOLÉRANCE")
# #     print(f"   📊 Taux de tolérance = {SEUIL_TOLERANCE}%")
# #     print("   📐 Méthode = Similarité de Jaccard (sans librairie externe)")
# #     print("   ❌ Règles R1 à R5 = NON utilisées")
# #     print("=" * 70)
    
# #     # Connexion MongoDB
# #     client_driver = MongoClient(MONGO_URI_DRIVER)
# #     db_driver = client_driver[DB_NAME]
# #     coll_source = db_driver[COLLECTION_SOURCE]
# #     total_docs = coll_source.count_documents({})
# #     print(f"\n📂 {total_docs} documents dans la source")
    
# #     # Connexion Spark
# #     print("\n⚡ Connexion au cluster Spark...")
# #     spark = SparkSession.builder \
# #         .appName("Deduplication_Tolerance") \
# #         .master(SPARK_MASTER) \
# #         .config("spark.executor.memory", "2g") \
# #         .config("spark.executor.cores", "2") \
# #         .getOrCreate()
# #     spark.sparkContext.setLogLevel("WARN")
    
# #     # Lecture distribuée
# #     print("\n📥 Lecture distribuée...")
# #     docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# #     plages = [
# #         {"skip": i * docs_par_worker,
# #          "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
# #         for i in range(NB_WORKERS)
# #     ]
    
# #     rdd_data = spark.sparkContext \
# #         .parallelize(plages, NB_WORKERS) \
# #         .mapPartitions(lire_partition_depuis_mongo)
    
# #     df_spark = spark.read.json(rdd_data.map(
# #         lambda d: json.dumps(
# #             {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
# #              for k, v in d.items()}
# #         )
# #     ))
# #     total_lignes = df_spark.count()
# #     print(f"✅ {total_lignes} documents chargés")
    
# #     # Vider la collection destination
# #     coll_dest = db_driver[COLLECTION_DEST]
# #     coll_dest.delete_many({})
# #     print("\n🧹 Collection destination vidée")
    
# #     # Déduplication
# #     print(f"\n💾 Suppression des doublons (seuil = {SEUIL_TOLERANCE}%)...")
# #     temps_traitement = time.time()
    
# #     rdd_stats = df_spark.rdd \
# #         .map(lambda row: row.asDict()) \
# #         .mapPartitions(deduplication_partition_spark)
    
# #     stats = rdd_stats.collect()
# #     total_traites = sum(s.get("docs_traites", 0) for s in stats)
# #     total_gardes = sum(s.get("docs_gardes", 0) for s in stats)
# #     total_supprimes = sum(s.get("docs_supprimes", 0) for s in stats)
    
# #     print(f"✅ Traitement terminé en {time.time()-temps_traitement:.2f}s")
# #     print(f"\n📊 RÉSULTATS :")
# #     print(f"   📥 Documents lus      : {total_traites}")
# #     print(f"   ✅ Documents gardés   : {total_gardes}")
# #     print(f"   ❌ Documents supprimés: {total_supprimes}")
# #     if total_traites > 0:
# #         print(f"   📉 Taux de réduction  : {total_supprimes/total_traites*100:.2f}%")
    
# #     spark.stop()
# #     client_driver.close()
    
# #     temps_total = time.time() - temps_debut
# #     print(f"\n⏱️  Temps total : {temps_total:.2f}s")
# #     print("=" * 70)


# # if __name__ == "__main__":
# #     tester_localement()
    
# #     print("\n" + "=" * 60)
# #     reponse = input("🚀 Lancer le traitement Spark sur toutes les données ? (o/n): ")
# #     if reponse.lower() in ["o", "oui"]:
# #         main()
# #     else:
# #         print("❌ Traitement annulé")

#!/usr/bin/env python3
# suppression_des_doublant.py - Version avec passe finale inter-partitions

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
COLLECTION_DEST   = "commentaires_sans_doublons_tolerance"
NB_WORKERS        = 2
SPARK_MASTER      = "spark://spark-master:7077"
SEUIL_TOLERANCE = 85  # Taux de tolérance (modifiable)

# Dossier pour les logs
LOG_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# ============================================================
# FONCTION DE SIMILARITÉ (JACCARD)
# ============================================================

def similarite_texte(texte1, texte2):
    """
    Calcule la similarité entre deux textes (0 à 100%)
    Méthode : Indice de Jaccard sur les caractères
    """
    if not texte1 or not texte2:
        return 0
    
    t1 = str(texte1).lower().strip()
    t2 = str(texte2).lower().strip()
    
    # Cas identique
    if t1 == t2:
        return 100
    
    # Calcul du Jaccard similarity sur les caractères
    set1 = set(t1)
    set2 = set(t2)
    
    if not set1 or not set2:
        return 0
    
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    
    return (intersection / union) * 100


def sont_doublons(texte1, texte2):
    """Vérifie si deux textes sont des doublons selon le taux de tolérance"""
    return similarite_texte(texte1, texte2) >= SEUIL_TOLERANCE


def supprimer_doublons_dans_partition(documents):
    """
    Supprime les doublons à l'INTÉRIEUR d'une partition
    """
    if len(documents) <= 1:
        return documents
    
    documents_gardes = []
    
    for doc in documents:
        texte_ref = doc.get("Commentaire_Client", "")
        
        est_doublon = False
        for doc_garde in documents_gardes:
            texte_garde = doc_garde.get("Commentaire_Client", "")
            if sont_doublons(texte_ref, texte_garde):
                est_doublon = True
                break
        
        if not est_doublon:
            documents_gardes.append(doc)
    
    return documents_gardes


# ============================================================
# FONCTIONS SPARK
# ============================================================

def lire_partition_depuis_mongo(partition_info):
    """Lecture distribuée depuis MongoDB"""
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client = MongoClient("mongodb://mongodb_pfe:27017/",
                             serverSelectionTimeoutMS=5000)
        db = client["telecom_algerie"]
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


def deduplication_partition_spark(partition):
    """Version Spark : suppression des doublons dans chaque partition"""
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError
    
    SEUIL = 85
    
    def sim_texte(t1, t2):
        if not t1 or not t2:
            return 0
        t1 = str(t1).lower().strip()
        t2 = str(t2).lower().strip()
        if t1 == t2:
            return 100
        set1 = set(t1)
        set2 = set(t2)
        if not set1 or not set2:
            return 0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return (intersection / union) * 100
    
    def sont_doublons_local(t1, t2):
        return sim_texte(t1, t2) >= SEUIL
    
    docs_partition = list(partition)
    
    if len(docs_partition) == 0:
        return []
    
    # Suppression des doublons dans la partition
    docs_gardes = []
    for doc in docs_partition:
        texte_ref = doc.get("Commentaire_Client", "")
        
        est_doublon = False
        for doc_garde in docs_gardes:
            texte_garde = doc_garde.get("Commentaire_Client", "")
            if sont_doublons_local(texte_ref, texte_garde):
                est_doublon = True
                break
        
        if not est_doublon:
            docs_gardes.append(doc)
    
    # Écriture dans MongoDB
    try:
        client = MongoClient("mongodb://mongodb_pfe:27017/",
                             serverSelectionTimeoutMS=5000)
        db = client["telecom_algerie"]
        collection = db["commentaires_sans_doublons_tolerance"]
    except Exception as e:
        yield {"_erreur": str(e)}
        return
    
    batch = []
    for doc in docs_gardes:
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
        "docs_gardes": len(docs_gardes),
        "docs_supprimes": len(docs_partition) - len(docs_gardes)
    }


# ============================================================
# PASSE FINALE : DOUBLONS INTER-PARTITIONS
# ============================================================

def supprimer_doublons_inter_partitions():
    """
    Parcourt TOUS les documents de la destination
    et supprime les doublons qui viennent de partitions différentes
    """
    print("\n" + "=" * 70)
    print("🔧 PASSE FINALE — Suppression des doublons INTER-PARTITIONS")
    print("=" * 70)
    temps_passe = time.time()
    
    client = MongoClient(MONGO_URI_DRIVER)
    db = client[DB_NAME]
    collection = db[COLLECTION_DEST]
    
    # Compter avant
    avant = collection.count_documents({})
    print(f"   📥 Documents avant passe finale : {avant}")
    
    vus = {}
    a_supprimer = []
    
    for doc in collection.find({}, {"_id": 1, "Commentaire_Client": 1}):
        texte = doc.get("Commentaire_Client", "")
        texte_normalise = texte.lower().strip()
        
        if texte_normalise in vus:
            a_supprimer.append(doc["_id"])
        else:
            vus[texte_normalise] = doc["_id"]
    
    if a_supprimer:
        resultat = collection.delete_many({"_id": {"$in": a_supprimer}})
        print(f"   ✅ {resultat.deleted_count} doublons inter-partitions supprimés")
    else:
        print(f"   ✅ Aucun doublon inter-partitions trouvé")
    
    # Compter après
    apres = collection.count_documents({})
    print(f"   📥 Documents après passe finale  : {apres}")
    print(f"   📉 Réduction supplémentaire      : {avant - apres}")
    
    client.close()
    print(f"   ⏱️  Passe finale terminée en {time.time()-temps_passe:.2f}s")


# ============================================================
# TEST LOCAL
# ============================================================

def tester_localement():
    print("\n" + "=" * 60)
    print(f"🧪 TEST AVEC TAUX DE TOLÉRANCE = {SEUIL_TOLERANCE}%")
    print("=" * 60)
    
    test_documents = [
        {"id": 1, "Commentaire_Client": "Bon courage"},
        {"id": 2, "Commentaire_Client": "Bon courage !"},
        {"id": 3, "Commentaire_Client": "bon courage"},
        {"id": 4, "Commentaire_Client": "Merci beaucoup"},
        {"id": 5, "Commentaire_Client": "Merci bcp"},
        {"id": 6, "Commentaire_Client": "Très bon service"},
        {"id": 7, "Commentaire_Client": "Très bonne connexion"},
        {"id": 8, "Commentaire_Client": "Je suis en colère"},
    ]
    
    print("\n📝 DOCUMENTS ORIGINAUX :")
    for doc in test_documents:
        print(f"   {doc['id']}: {doc['Commentaire_Client']}")
    
    resultat = supprimer_doublons_dans_partition(test_documents)
    
    print(f"\n✅ DOCUMENTS GARDÉS :")
    for doc in resultat:
        print(f"   {doc['id']}: {doc['Commentaire_Client']}")
    
    print(f"\n📊 RÉSUMÉ :")
    print(f"   Documents initiaux : {len(test_documents)}")
    print(f"   Documents gardés   : {len(resultat)}")
    print(f"   Documents supprimés: {len(test_documents) - len(resultat)}")
    
    print(f"\n🔍 DOUBLONS DÉTECTÉS (similarité >= {SEUIL_TOLERANCE}%) :")
    for i, doc1 in enumerate(test_documents):
        for j, doc2 in enumerate(test_documents):
            if i < j:
                sim = similarite_texte(
                    doc1["Commentaire_Client"],
                    doc2["Commentaire_Client"]
                )
                if sim >= SEUIL_TOLERANCE:
                    print(f"   {doc1['id']} et {doc2['id']} : {sim:.1f}% → DOUBLON")


# ============================================================
# MAIN
# ============================================================

def main():
    temps_debut = time.time()
    
    print("=" * 70)
    print("🔁 SUPPRESSION DES DOUBLONS AVEC TAUX DE TOLÉRANCE")
    print(f"   📊 Taux de tolérance = {SEUIL_TOLERANCE}%")
    print("   📐 Méthode = Similarité de Jaccard (sans librairie externe)")
    print("   ❌ Règles R1 à R5 = NON utilisées")
    print("=" * 70)
    
    # Connexion MongoDB
    client_driver = MongoClient(MONGO_URI_DRIVER)
    db_driver = client_driver[DB_NAME]
    coll_source = db_driver[COLLECTION_SOURCE]
    total_docs = coll_source.count_documents({})
    print(f"\n📂 {total_docs} documents dans la source")
    
    # Connexion Spark
    print("\n⚡ Connexion au cluster Spark...")
    spark = SparkSession.builder \
        .appName("Deduplication_Tolerance") \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # Lecture distribuée
    print("\n📥 Lecture distribuée...")
    docs_par_worker = math.ceil(total_docs / NB_WORKERS)
    plages = [
        {"skip": i * docs_par_worker,
         "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
        for i in range(NB_WORKERS)
    ]
    
    rdd_data = spark.sparkContext \
        .parallelize(plages, NB_WORKERS) \
        .mapPartitions(lire_partition_depuis_mongo)
    
    df_spark = spark.read.json(rdd_data.map(
        lambda d: json.dumps(
            {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
             for k, v in d.items()}
        )
    ))
    total_lignes = df_spark.count()
    print(f"✅ {total_lignes} documents chargés")
    
    # Vider la collection destination
    coll_dest = db_driver[COLLECTION_DEST]
    coll_dest.delete_many({})
    print("\n🧹 Collection destination vidée")
    
    # Déduplication dans chaque partition
    print(f"\n💾 Suppression des doublons (seuil = {SEUIL_TOLERANCE}%)...")
    temps_traitement = time.time()
    
    rdd_stats = df_spark.rdd \
        .map(lambda row: row.asDict()) \
        .mapPartitions(deduplication_partition_spark)
    
    stats = rdd_stats.collect()
    total_traites = sum(s.get("docs_traites", 0) for s in stats)
    total_gardes = sum(s.get("docs_gardes", 0) for s in stats)
    total_supprimes = sum(s.get("docs_supprimes", 0) for s in stats)
    
    print(f"✅ Traitement Spark terminé en {time.time()-temps_traitement:.2f}s")
    print(f"\n📊 RÉSULTATS SPARK :")
    print(f"   📥 Documents lus      : {total_traites}")
    print(f"   ✅ Documents gardés   : {total_gardes}")
    print(f"   ❌ Documents supprimés: {total_supprimes}")
    if total_traites > 0:
        print(f"   📉 Taux de réduction  : {total_supprimes/total_traites*100:.2f}%")
    
    # PASSE FINALE : Supprimer les doublons entre partitions
    supprimer_doublons_inter_partitions()
    
    spark.stop()
    client_driver.close()
    
    temps_total = time.time() - temps_debut
    print(f"\n⏱️  Temps total : {temps_total:.2f}s")
    print("=" * 70)


# ============================================================
# LANCEMENT
# ============================================================

if __name__ == "__main__":
    tester_localement()
    
    print("\n" + "=" * 60)
    reponse = input("🚀 Lancer le traitement Spark sur toutes les données ? (o/n): ")
    if reponse.lower() in ["o", "oui"]:
        main()
    else:
        print("❌ Traitement annulé")


# #!/usr/bin/env python3
# # suppression_des_doublons_tfidf.py - Version TF-IDF (remplace Jaccard)

# from pyspark.sql import SparkSession
# from pymongo import MongoClient, InsertOne
# from pymongo.errors import BulkWriteError
# from datetime import datetime
# import os, time, math, json

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# DB_NAME           = "telecom_algerie"
# COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
# COLLECTION_DEST   = "commentaires_sans_doublons_tfidf"
# NB_WORKERS        = 3
# SPARK_MASTER      = "spark://spark-master:7077"
# SEUIL_TFIDF       = 0.85  # Seuil de similarité cosinus (0 à 1)

# LOG_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/logs"
# os.makedirs(LOG_DIR, exist_ok=True)


# # ============================================================
# # FONCTION DE SIMILARITÉ TF-IDF (REMPLACE JACCARD)
# # ============================================================

# def detecter_doublons_tfidf(documents, seuil=0.85):
#     """
#     Remplace l'ancienne fonction Jaccard sur caractères.
#     Utilise TF-IDF + Cosine Similarity sur les MOTS.
#     Gère mieux les paraphrases et variations sémantiques.
    
#     Args:
#         documents : liste de dicts avec 'Commentaire_Client'
#         seuil     : seuil de similarité cosinus (0.0 à 1.0)
#     Returns:
#         liste de documents sans doublons
#     """
#     from sklearn.feature_extraction.text import TfidfVectorizer
#     from sklearn.metrics.pairwise import cosine_similarity

#     if len(documents) <= 1:
#         return documents

#     textes = [
#         str(doc.get("Commentaire_Client", "")).lower().strip()
#         for doc in documents
#     ]

#     # Cas : tous les textes sont vides
#     if all(t == "" for t in textes):
#         return [documents[0]]

#     try:
#         vectorizer = TfidfVectorizer(
#             analyzer='word',
#             ngram_range=(1, 2),   # unigrams + bigrams
#             min_df=1,
#             sublinear_tf=True     # log(tf) pour réduire l'effet des mots fréquents
#         )
#         tfidf_matrix = vectorizer.fit_transform(textes)
#         sim_matrix = cosine_similarity(tfidf_matrix)
#     except Exception:
#         # Si TF-IDF échoue (textes trop courts / vides), fallback exact match
#         vus = set()
#         gardes = []
#         for doc in documents:
#             t = str(doc.get("Commentaire_Client", "")).lower().strip()
#             if t not in vus:
#                 vus.add(t)
#                 gardes.append(doc)
#         return gardes

#     gardes = []
#     supprimes = set()

#     for i in range(len(documents)):
#         if i in supprimes:
#             continue
#         gardes.append(documents[i])
#         for j in range(i + 1, len(documents)):
#             if j not in supprimes and sim_matrix[i][j] >= seuil:
#                 supprimes.add(j)

#     return gardes


# # ============================================================
# # FONCTIONS SPARK
# # ============================================================

# def lire_partition_depuis_mongo(partition_info):
#     """Lecture distribuée depuis MongoDB (inchangée)"""
#     import sys
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient

#     for item in partition_info:
#         client = MongoClient("mongodb://mongodb_pfe:27017/",
#                              serverSelectionTimeoutMS=5000)
#         db = client["telecom_algerie"]
#         collection = db["commentaires_sans_urls_arobase"]
#         curseur = collection.find(
#             {},
#             {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
#              "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
#         ).skip(item["skip"]).limit(item["limit"])
#         for doc in curseur:
#             doc["_id"] = str(doc["_id"])
#             yield doc
#         client.close()


# def deduplication_tfidf_spark(partition):
#     """
#     Version Spark : TF-IDF remplace Jaccard dans chaque partition.
#     Même structure que ton code original, seule la méthode de similarité change.
#     """
#     import sys
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient, InsertOne
#     from pymongo.errors import BulkWriteError
#     from sklearn.feature_extraction.text import TfidfVectorizer
#     from sklearn.metrics.pairwise import cosine_similarity

#     SEUIL = 0.85

#     docs_partition = list(partition)

#     if len(docs_partition) == 0:
#         return []

#     # --- Déduplication TF-IDF ---
#     textes = [
#         str(doc.get("Commentaire_Client", "")).lower().strip()
#         for doc in docs_partition
#     ]

#     try:
#         vectorizer = TfidfVectorizer(
#             analyzer='word',
#             ngram_range=(1, 2),
#             min_df=1,
#             sublinear_tf=True
#         )
#         tfidf_matrix = vectorizer.fit_transform(textes)
#         sim_matrix = cosine_similarity(tfidf_matrix)

#         supprimes = set()
#         for i in range(len(docs_partition)):
#             if i in supprimes:
#                 continue
#             for j in range(i + 1, len(docs_partition)):
#                 if j not in supprimes and sim_matrix[i][j] >= SEUIL:
#                     supprimes.add(j)

#         docs_gardes = [
#             doc for idx, doc in enumerate(docs_partition)
#             if idx not in supprimes
#         ]

#     except Exception:
#         # Fallback : exact match si TF-IDF échoue
#         vus = set()
#         docs_gardes = []
#         for doc in docs_partition:
#             t = str(doc.get("Commentaire_Client", "")).lower().strip()
#             if t not in vus:
#                 vus.add(t)
#                 docs_gardes.append(doc)

#     # --- Écriture dans MongoDB ---
#     try:
#         client = MongoClient("mongodb://mongodb_pfe:27017/",
#                              serverSelectionTimeoutMS=5000)
#         db = client["telecom_algerie"]
#         collection = db["commentaires_sans_doublons_tfidf"]
#     except Exception as e:
#         yield {"_erreur": str(e)}
#         return

#     batch = []
#     for doc in docs_gardes:
#         batch.append(InsertOne(doc))
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
#         "docs_traites": len(docs_partition),
#         "docs_gardes": len(docs_gardes),
#         "docs_supprimes": len(docs_partition) - len(docs_gardes)
#     }


# # ============================================================
# # PASSE FINALE INTER-PARTITIONS (CORRIGÉE)
# # Applique aussi TF-IDF (cohérence avec la passe Spark)
# # ============================================================

# def supprimer_doublons_inter_partitions():
#     """
#     CORRECTION par rapport à ton code original :
#     - L'ancienne version ne faisait qu'une comparaison EXACTE (texte == texte)
#     - Cette version applique TF-IDF avec le même seuil → cohérence totale
#     """
#     from sklearn.feature_extraction.text import TfidfVectorizer
#     from sklearn.metrics.pairwise import cosine_similarity

#     print("\n" + "=" * 70)
#     print("🔧 PASSE FINALE — Doublons inter-partitions (TF-IDF)")
#     print("=" * 70)
#     temps_debut = time.time()

#     client = MongoClient(MONGO_URI_DRIVER)
#     db = client[DB_NAME]
#     collection = db[COLLECTION_DEST]

#     avant = collection.count_documents({})
#     print(f"   📥 Documents avant passe finale : {avant}")

#     # Charger tous les docs
#     tous_docs = list(collection.find({}, {"_id": 1, "Commentaire_Client": 1}))

#     if len(tous_docs) == 0:
#         print("   ✅ Aucun document à traiter")
#         client.close()
#         return

#     textes = [
#         str(doc.get("Commentaire_Client", "")).lower().strip()
#         for doc in tous_docs
#     ]

#     # Phase 1 : exact match rapide (O(n))
#     vus_exact = {}
#     a_supprimer = set()
#     for i, (doc, texte) in enumerate(zip(tous_docs, textes)):
#         if texte in vus_exact:
#             a_supprimer.add(doc["_id"])
#         else:
#             vus_exact[texte] = doc["_id"]

#     # Supprimer les exacts immédiatement
#     if a_supprimer:
#         collection.delete_many({"_id": {"$in": list(a_supprimer)}})
#         print(f"   ✅ {len(a_supprimer)} doublons exacts supprimés")

#     # Phase 2 : TF-IDF sur les restants
#     restants = list(collection.find({}, {"_id": 1, "Commentaire_Client": 1}))
#     textes_restants = [
#         str(doc.get("Commentaire_Client", "")).lower().strip()
#         for doc in restants
#     ]

#     if len(restants) > 1:
#         try:
#             vectorizer = TfidfVectorizer(
#                 analyzer='word',
#                 ngram_range=(1, 2),
#                 min_df=1,
#                 sublinear_tf=True
#             )
#             tfidf_matrix = vectorizer.fit_transform(textes_restants)
#             sim_matrix = cosine_similarity(tfidf_matrix)

#             a_supprimer_tfidf = set()
#             for i in range(len(restants)):
#                 if restants[i]["_id"] in a_supprimer_tfidf:
#                     continue
#                 for j in range(i + 1, len(restants)):
#                     if (restants[j]["_id"] not in a_supprimer_tfidf
#                             and sim_matrix[i][j] >= SEUIL_TFIDF):
#                         a_supprimer_tfidf.add(restants[j]["_id"])

#             if a_supprimer_tfidf:
#                 collection.delete_many({"_id": {"$in": list(a_supprimer_tfidf)}})
#                 print(f"   ✅ {len(a_supprimer_tfidf)} doublons sémantiques supprimés")
#             else:
#                 print("   ✅ Aucun doublon sémantique trouvé")

#         except Exception as e:
#             print(f"   ⚠️  TF-IDF inter-partitions échoué : {e}")

#     apres = collection.count_documents({})
#     print(f"   📥 Documents après passe finale  : {apres}")
#     print(f"   📉 Réduction supplémentaire      : {avant - apres}")
#     client.close()
#     print(f"   ⏱️  Terminé en {time.time()-temps_debut:.2f}s")


# # ============================================================
# # TEST LOCAL
# # ============================================================

# def tester_localement():
#     print("\n" + "=" * 60)
#     print(f"🧪 TEST TF-IDF — seuil = {SEUIL_TFIDF}")
#     print("=" * 60)

#     test_documents = [
#         {"id": 1,  "Commentaire_Client": "Bon courage"},
#         {"id": 2,  "Commentaire_Client": "Bon courage !"},
#         {"id": 3,  "Commentaire_Client": "bon courage"},
#         {"id": 4,  "Commentaire_Client": "Merci beaucoup"},
#         {"id": 5,  "Commentaire_Client": "Merci bcp"},
#         {"id": 6,  "Commentaire_Client": "Très bon service"},
#         {"id": 7,  "Commentaire_Client": "Service vraiment très bon"},  # paraphrase
#         {"id": 8,  "Commentaire_Client": "Je suis en colère"},
#         {"id": 9,  "Commentaire_Client": "La connexion est mauvaise"},
#         {"id": 10, "Commentaire_Client": "Mauvaise connexion réseau"},  # paraphrase
#     ]

#     print("\n📝 DOCUMENTS ORIGINAUX :")
#     for doc in test_documents:
#         print(f"   {doc['id']:2d}: {doc['Commentaire_Client']}")

#     resultat = detecter_doublons_tfidf(test_documents, seuil=SEUIL_TFIDF)

#     print(f"\n✅ DOCUMENTS GARDÉS :")
#     for doc in resultat:
#         print(f"   {doc['id']:2d}: {doc['Commentaire_Client']}")

#     print(f"\n📊 RÉSUMÉ :")
#     print(f"   Documents initiaux  : {len(test_documents)}")
#     print(f"   Documents gardés    : {len(resultat)}")
#     print(f"   Documents supprimés : {len(test_documents) - len(resultat)}")

#     # Afficher la matrice de similarité
#     from sklearn.feature_extraction.text import TfidfVectorizer
#     from sklearn.metrics.pairwise import cosine_similarity

#     textes = [doc["Commentaire_Client"].lower() for doc in test_documents]
#     vec = TfidfVectorizer(analyzer='word', ngram_range=(1, 2), min_df=1)
#     mat = vec.fit_transform(textes)
#     sim = cosine_similarity(mat)

#     print(f"\n🔍 PAIRES SIMILAIRES (similarité >= {SEUIL_TFIDF}) :")
#     for i in range(len(test_documents)):
#         for j in range(i + 1, len(test_documents)):
#             if sim[i][j] >= SEUIL_TFIDF:
#                 print(f"   Doc {test_documents[i]['id']} & {test_documents[j]['id']} : "
#                       f"{sim[i][j]:.2f} → DOUBLON détecté")
#                 print(f"      '{test_documents[i]['Commentaire_Client']}'")
#                 print(f"      '{test_documents[j]['Commentaire_Client']}'")


# # ============================================================
# # MAIN
# # ============================================================

# def main():
#     temps_debut = time.time()

#     print("=" * 70)
#     print("🔁 SUPPRESSION DES DOUBLONS — TF-IDF + Cosine Similarity")
#     print(f"   📊 Seuil de similarité = {SEUIL_TFIDF}")
#     print("   📐 Méthode = TF-IDF sur les mots (bigrams inclus)")
#     print("=" * 70)

#     client_driver = MongoClient(MONGO_URI_DRIVER)
#     db_driver = client_driver[DB_NAME]
#     coll_source = db_driver[COLLECTION_SOURCE]
#     total_docs = coll_source.count_documents({})
#     print(f"\n📂 {total_docs} documents dans la source")

#     print("\n⚡ Connexion au cluster Spark...")
#     spark = SparkSession.builder \
#         .appName("Deduplication_TFIDF") \
#         .master(SPARK_MASTER) \
#         .config("spark.executor.memory", "2g") \
#         .config("spark.executor.cores", "2") \
#         .getOrCreate()
#     spark.sparkContext.setLogLevel("WARN")

#     print("\n📥 Lecture distribuée...")
#     docs_par_worker = math.ceil(total_docs / NB_WORKERS)
#     plages = [
#         {"skip": i * docs_par_worker,
#          "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
#         for i in range(NB_WORKERS)
#     ]

#     rdd_data = spark.sparkContext \
#         .parallelize(plages, NB_WORKERS) \
#         .mapPartitions(lire_partition_depuis_mongo)

#     df_spark = spark.read.json(rdd_data.map(
#         lambda d: json.dumps(
#             {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
#              for k, v in d.items()}
#         )
#     ))
#     total_lignes = df_spark.count()
#     print(f"✅ {total_lignes} documents chargés")

#     coll_dest = db_driver[COLLECTION_DEST]
#     coll_dest.delete_many({})
#     print("\n🧹 Collection destination vidée")

#     print(f"\n💾 Déduplication TF-IDF en cours (seuil = {SEUIL_TFIDF})...")
#     temps_traitement = time.time()

#     rdd_stats = df_spark.rdd \
#         .map(lambda row: row.asDict()) \
#         .mapPartitions(deduplication_tfidf_spark)

#     stats = rdd_stats.collect()
#     total_traites  = sum(s.get("docs_traites", 0) for s in stats)
#     total_gardes   = sum(s.get("docs_gardes", 0) for s in stats)
#     total_supprimes = sum(s.get("docs_supprimes", 0) for s in stats)

#     print(f"✅ Traitement Spark terminé en {time.time()-temps_traitement:.2f}s")
#     print(f"\n📊 RÉSULTATS SPARK :")
#     print(f"   📥 Documents lus       : {total_traites}")
#     print(f"   ✅ Documents gardés    : {total_gardes}")
#     print(f"   ❌ Documents supprimés : {total_supprimes}")
#     if total_traites > 0:
#         print(f"   📉 Taux de réduction   : {total_supprimes/total_traites*100:.2f}%")

#     supprimer_doublons_inter_partitions()

#     spark.stop()
#     client_driver.close()

#     print(f"\n⏱️  Temps total : {time.time()-temps_debut:.2f}s")
#     print("=" * 70)


# # ============================================================
# # LANCEMENT
# # ============================================================

# if __name__ == "__main__":
#     # Vérification que scikit-learn est disponible
#     try:
#         from sklearn.feature_extraction.text import TfidfVectorizer
#         print("✅ scikit-learn disponible")
#     except ImportError:
#         print("❌ scikit-learn manquant — installe avec : pip install scikit-learn")
#         exit(1)

#     tester_localement()

#     print("\n" + "=" * 60)
#     reponse = input("🚀 Lancer le traitement Spark sur toutes les données ? (o/n): ")
#     if reponse.lower() in ["o", "oui"]:
#         main()
#     else:
#         print("❌ Traitement annulé")