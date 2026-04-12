# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# # test_etape1_kafka.py
# # Test de l'étape 1 - Ne traite que les documents avec traite=false

# from pymongo import MongoClient
# import subprocess
# import time
# import os
# from datetime import datetime
# from kafka import KafkaProducer
# import json

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# SCRIPTS_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/code"
# SCRIPT_ETAPE_1 = "suppression_urls.py"

# # Collections
# SOURCE_COLLECTION = "commentaires_bruts"
# TARGET_COLLECTION = "commentaires_sans_urls_arobase"

# # ============================================================
# # CONNEXION KAFKA
# # ============================================================
# producer = KafkaProducer(
#     bootstrap_servers=['localhost:9092'],
#     value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
# )

# def send_kafka_message(event_type, data):
#     """Envoie un message à Kafka"""
#     message = {
#         "event_type": event_type,
#         "timestamp": datetime.now().isoformat(),
#         "data": data
#     }
#     producer.send("pipeline_events", message)
#     producer.flush()
#     print(f"📨 Kafka: {event_type} - {data.get('message', '')}")

# # ============================================================
# # FONCTIONS MONGODB
# # ============================================================
# def compter_docs(collection_name, query=None):
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
#         db = client[DB_NAME]
#         if query:
#             count = db[collection_name].count_documents(query)
#         else:
#             count = db[collection_name].count_documents({})
#         client.close()
#         return count
#     except Exception as e:
#         print(f"⚠️ Erreur comptage: {e}")
#         return -1

# def get_nouveaux_ids():
#     """Récupère les IDs des documents avec traite=false"""
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
#         db = client[DB_NAME]
#         collection = db[SOURCE_COLLECTION]
        
#         query = {"traite": False}
#         docs = list(collection.find(query, {"_id": 1}))
#         client.close()
#         return [doc["_id"] for doc in docs]
#     except Exception as e:
#         print(f"⚠️ Erreur: {e}")
#         return []

# def marquer_comme_traites(ids):
#     """Marque les documents comme traités"""
#     if not ids:
#         return 0
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
#         db = client[DB_NAME]
#         collection = db[SOURCE_COLLECTION]
        
#         result = collection.update_many(
#             {"_id": {"$in": ids}},
#             {"$set": {"traite": True, "date_traitement": datetime.now()}}
#         )
#         client.close()
#         print(f"   ✅ {result.modified_count} documents marqués comme traités")
#         return result.modified_count
#     except Exception as e:
#         print(f"   ⚠️ Erreur marquage: {e}")
#         return 0

# def get_collections():
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
#         cols = client[DB_NAME].list_collection_names()
#         client.close()
#         return cols
#     except Exception as e:
#         print(f"⚠️ Erreur: {e}")
#         return []

# # ============================================================
# # ÉTAPE 1 (ne traite que les traite=false)
# # ============================================================
# def etape1_suppression_urls():
#     print("\n" + "="*60)
#     print("🔹 ÉTAPE 1 — Suppression des URLs, @, hashtags")
#     print("="*60)

#     # ⭐ Compter uniquement les documents NON traités
#     total = compter_docs(SOURCE_COLLECTION)
#     non_traites = compter_docs(SOURCE_COLLECTION, {"traite": False})
    
#     print(f"📊 Total dans {SOURCE_COLLECTION}: {total}")
#     print(f"⏳ Nouveaux à traiter (traite=false): {non_traites}")
    
#     if non_traites == 0:
#         print("✅ Aucun nouveau document à traiter !")
#         send_kafka_message("etape_1_skip", {
#             "etape": "Suppression URLs",
#             "message": "Aucun nouveau document à traiter"
#         })
#         return None

#     # Envoyer message Kafka début
#     send_kafka_message("etape_1_debut", {
#         "etape": "Suppression URLs",
#         "total_docs": non_traites,
#         "message": f"Début du nettoyage de {non_traites} nouveaux documents"
#     })

#     # Exécuter le script
#     script = os.path.join(SCRIPTS_DIR, SCRIPT_ETAPE_1)
#     print(f"🚀 Exécution: {script}")
    
#     debut = time.time()
#     result = subprocess.run(["python3", script], capture_output=True, text=True)
#     duree = time.time() - debut

#     if result.stdout:
#         print(result.stdout[-500:])

#     if result.returncode != 0:
#         print(f"❌ ERREUR:\n{result.stderr}")
#         send_kafka_message("etape_1_erreur", {
#             "etape": "Suppression URLs",
#             "error": result.stderr[-200:],
#             "message": "Échec de l'étape 1"
#         })
#         return None

#     # Compter après
#     apres = compter_docs(TARGET_COLLECTION)
#     print(f"📤 Résultat ({TARGET_COLLECTION}): {apres} documents")
#     print(f"⏱️  Durée: {duree:.1f}s")
    
#     # ⭐ Marquer les documents comme traités
#     ids_traites = get_nouveaux_ids()
#     marquer_comme_traites(ids_traites)
    
#     # Envoyer message Kafka fin
#     send_kafka_message("etape_1_fin", {
#         "etape": "Suppression URLs",
#         "avant": non_traites,
#         "apres": apres,
#         "duree": duree,
#         "message": f"Terminé: {non_traites} nouveaux documents traités"
#     })
    
#     print("✅ Étape 1 terminée !")
#     return {"avant": non_traites, "apres": apres, "duree": duree}

# # ============================================================
# # VÉRIFICATION DANS KAFKA
# # ============================================================
# def check_kafka():
#     """Affiche les messages Kafka"""
#     print("\n" + "="*60)
#     print("📡 Vérification Kafka")
#     print("="*60)
#     print("Ouvrez http://localhost:8088 pour voir les messages")
#     print("Ou exécutez: docker exec kafka_pfe kafka-console-consumer --topic pipeline_events --bootstrap-server localhost:9092 --from-beginning")

# # ============================================================
# # MAIN
# # ============================================================
# if __name__ == "__main__":
#     print("="*60)
#     print("🚀 TEST ÉTAPE 1 AVEC KAFKA (Mode incrémental)")
#     print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     print("="*60)
    
#     # Vérifier les collections
#     print("\n📁 Collections disponibles:")
#     for col in get_collections():
#         print(f"   - {col}")
    
#     # Lancer l'étape 1
#     resultat = etape1_suppression_urls()
    
#     if resultat:
#         print("\n" + "="*60)
#         print("📊 RÉSUMÉ")
#         print("="*60)
#         print(f"   📥 Nouveaux traités: {resultat['avant']}")
#         print(f"   📤 Résultat: {resultat['apres']}")
#         print(f"   ⏱️  Durée: {resultat['duree']:.1f}s")
#         print("="*60)
#         print("🎉 TEST TERMINÉ !")
    
#     check_kafka()
# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# # pipeline_complet.py
# # Pipeline complet avec Prefect Dashboard + Insertion commentaire

# from prefect import flow, task
# from pymongo import MongoClient
# import subprocess
# import time
# import os
# from datetime import datetime
# from kafka import KafkaProducer
# import json
# import random

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# SCRIPTS_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/code"

# SCRIPTS = {
#     "etape_1": "suppression_urls.py",
#     "etape_2": "suppression_des_doublant.py",
#     "etape_3": "normalisation_multinode.py",
#     "etape_4": "corriger_structure_emojis.py",
#     "etape_5": "supprimer_emojis_texte_local.py",
# }

# COLLECTIONS = {
#     "source": "commentaires_bruts",
#     "etape_1": "commentaires_sans_urls_arobase",
#     "etape_2": "commentaires_sans_doublons",
#     "etape_3": "commentaires_normalises",
#     "etape_4": "commentaires_emojis_corriges",
#     "etape_5": "commentaires_sans_emojis",
# }

# # ============================================================
# # CONNEXION KAFKA
# # ============================================================
# producer = KafkaProducer(
#     bootstrap_servers=['localhost:9092'],
#     value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
# )

# def send_kafka_message(event_type, data):
#     message = {
#         "event_type": event_type,
#         "timestamp": datetime.now().isoformat(),
#         "data": data
#     }
#     producer.send("pipeline_events", message)
#     producer.flush()
#     print(f"📨 Kafka: {event_type}")

# # ============================================================
# # FONCTIONS MONGODB
# # ============================================================
# def compter_docs(collection_name, query=None):
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
#         db = client[DB_NAME]
#         if query:
#             count = db[collection_name].count_documents(query)
#         else:
#             count = db[collection_name].count_documents({})
#         client.close()
#         return count
#     except Exception as e:
#         return -1

# def inserer_commentaire_test():
#     """Insère un commentaire de test avec traite=false et date au bon type"""
#     commentaires_test = [
#         "#algerei telecom votre réseau 4G est trop lent depuis 3 jours !!! 😡😡😡"
#     ]
#     texte = random.choice(commentaires_test)
    
#     client = MongoClient(MONGO_URI)
#     db = client[DB_NAME]
#     collection = db["commentaires_bruts"]
    
#     doc = {
#         "Commentaire_Client": texte,
#         "source": "Test_manuel",
#         "statut": "brut",
#         "traite": False,
#         "date": datetime.now(),                     # ✅ objet datetime
#         "metadata": {"fichier": "test_manuel"}
#     }
    
#     result = collection.insert_one(doc)
#     client.close()
#     print(f"✅ Commentaire test inséré: {texte[:50]}...")
#     return result.inserted_id

# def marquer_comme_traites(collection_name, ids):
#     if not ids:
#         return 0
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
#         db = client[DB_NAME]
#         collection = db[collection_name]
#         result = collection.update_many(
#             {"_id": {"$in": ids}},
#             {"$set": {"traite": True, "date_traitement": datetime.now()}}
#         )
#         client.close()
#         return result.modified_count
#     except Exception as e:
#         print(f"⚠️ Erreur marquage: {e}")
#         return 0

# def get_nouveaux_ids(collection_name):
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
#         db = client[DB_NAME]
#         collection = db[collection_name]
#         query = {"traite": False}
#         docs = list(collection.find(query, {"_id": 1}))
#         client.close()
#         return [doc["_id"] for doc in docs]
#     except Exception as e:
#         return []

# # ============================================================
# # TÂCHES PREFECT
# # ============================================================

# @task(name="Insertion commentaire test", retries=0)
# def task_inserer_commentaire():
#     print("\n" + "="*60)
#     print("📝 Insertion d'un commentaire de test")
#     print("="*60)
#     inserer_commentaire_test()
#     return {"status": "ok"}

# @task(name="Étape 1 — Suppression URLs", retries=1)
# def task_etape_1():
#     print("\n" + "="*60)
#     print("🔹 ÉTAPE 1 — Suppression des URLs")
#     print("="*60)
    
#     avant = compter_docs(COLLECTIONS["source"], {"traite": False})
#     print(f"📥 Nouveaux à traiter: {avant}")
    
#     if avant == 0:
#         print("✅ Aucun nouveau document")
#         return {"avant": 0, "apres": 0}
    
#     send_kafka_message("etape_1_debut", {"total": avant})
    
#     script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_1"])
#     debut = time.time()
#     result = subprocess.run(["python3", script], capture_output=True, text=True)
#     duree = time.time() - debut
    
#     if result.returncode != 0:
#         send_kafka_message("etape_1_erreur", {"error": result.stderr[-200:]})
#         raise Exception("Étape 1 échouée")
    
#     apres = compter_docs(COLLECTIONS["etape_1"])
    
#     ids = get_nouveaux_ids(COLLECTIONS["source"])
#     marquer_comme_traites(COLLECTIONS["source"], ids)
    
#     send_kafka_message("etape_1_fin", {"avant": avant, "apres": apres, "duree": duree})
#     print(f"✅ Terminé en {duree:.1f}s")
#     return {"avant": avant, "apres": apres}

# @task(name="Étape 2 — Suppression doublons", retries=1)
# def task_etape_2():
#     print("\n" + "="*60)
#     print("🔹 ÉTAPE 2 — Suppression des doublons")
#     print("="*60)
    
#     send_kafka_message("etape_2_debut", {})
    
#     script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_2"])
#     debut = time.time()
#     result = subprocess.run(["python3", script], capture_output=True, text=True)
#     duree = time.time() - debut
    
#     if result.returncode != 0:
#         send_kafka_message("etape_2_erreur", {"error": result.stderr[-200:]})
#         raise Exception("Étape 2 échouée")
    
#     apres = compter_docs(COLLECTIONS["etape_2"])
#     send_kafka_message("etape_2_fin", {"apres": apres, "duree": duree})
#     print(f"✅ Terminé en {duree:.1f}s")
#     return {"apres": apres}

# @task(name="Étape 3 — Normalisation", retries=1)
# def task_etape_3():
#     print("\n" + "="*60)
#     print("🔹 ÉTAPE 3 — Normalisation du texte")
#     print("="*60)
    
#     send_kafka_message("etape_3_debut", {})
    
#     script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_3"])
#     debut = time.time()
#     result = subprocess.run(["python3", script], capture_output=True, text=True)
#     duree = time.time() - debut
    
#     if result.returncode != 0:
#         raise Exception("Étape 3 échouée")
    
#     apres = compter_docs(COLLECTIONS["etape_3"])
#     send_kafka_message("etape_3_fin", {"apres": apres, "duree": duree})
#     print(f"✅ Terminé en {duree:.1f}s")
#     return {"apres": apres}

# @task(name="Étape 4 — Correction emojis", retries=1)
# def task_etape_4():
#     print("\n" + "="*60)
#     print("🔹 ÉTAPE 4 — Correction des emojis")
#     print("="*60)
    
#     send_kafka_message("etape_4_debut", {})
    
#     script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_4"])
#     debut = time.time()
#     result = subprocess.run(["python3", script], capture_output=True, text=True)
#     duree = time.time() - debut
    
#     if result.returncode != 0:
#         raise Exception("Étape 4 échouée")
    
#     apres = compter_docs(COLLECTIONS["etape_4"])
#     send_kafka_message("etape_4_fin", {"apres": apres, "duree": duree})
#     print(f"✅ Terminé en {duree:.1f}s")
#     return {"apres": apres}

# @task(name="Étape 5 — Suppression emojis", retries=1)
# def task_etape_5():
#     print("\n" + "="*60)
#     print("🔹 ÉTAPE 5 — Suppression des emojis")
#     print("="*60)
    
#     send_kafka_message("etape_5_debut", {})
    
#     script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_5"])
#     debut = time.time()
#     result = subprocess.run(["python3", script], capture_output=True, text=True)
#     duree = time.time() - debut
    
#     if result.returncode != 0:
#         raise Exception("Étape 5 échouée")
    
#     apres = compter_docs(COLLECTIONS["etape_5"])
#     send_kafka_message("etape_5_fin", {"apres": apres, "duree": duree})
#     print(f"✅ Terminé en {duree:.1f}s")
#     return {"apres": apres}

# # ============================================================
# # FLOW PRINCIPAL
# # ============================================================

# @flow(name="Pipeline Nettoyage Complet", log_prints=True)
# def pipeline_complet():
#     print("="*60)
#     print("🚀 PIPELINE COMPLET AVEC PREFECT DASHBOARD")
#     print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     print("="*60)
    
#     send_kafka_message("pipeline_debut", {"message": "Pipeline démarré"})
    
#     # 1. Insérer un commentaire test
#     task_inserer_commentaire()
    
#     # 2. Exécuter les 5 étapes
#     task_etape_1()
#     task_etape_2()
#     task_etape_3()
#     task_etape_4()
#     task_etape_5()
    
#     send_kafka_message("pipeline_fin", {"message": "Pipeline terminé"})
    
#     print("\n" + "="*60)
#     print("🎉 PIPELINE COMPLET TERMINÉ !")
#     print("="*60)

# # ============================================================
# # LANCEMENT
# # ============================================================
# if __name__ == "__main__":
#     pipeline_complet()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pipeline_complet.py
# Pipeline complet avec Prefect Dashboard + Insertion commentaire

from prefect import flow, task
from pymongo import MongoClient
import subprocess
import time
import os
from datetime import datetime
from kafka import KafkaProducer
import json
import random

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"

# Chemins absolus des scripts (car ils sont dans des dossiers différents)
BASE_DIR = "/home/mouna/projet_telecom/scripts/nettoyage"
SCRIPTS = {
    "etape_1": os.path.join(BASE_DIR, "Nettoyage_des_textes/code/suppression_urls.py"),
    "etape_2": os.path.join(BASE_DIR, "Nettoyage_des_textes/code/suppression_des_doublant.py"),
    "etape_3": os.path.join(BASE_DIR, "Nettoyage_des_textes/code/extraire_emojis_multinode.py"),
    "etape_4": os.path.join(BASE_DIR, "Normalisation/code/normalisation_multinode.py"),
   
}

# Collections correspondantes
COLLECTIONS = {
    "source": "commentaires_bruts",
    "etape_1": "commentaires_sans_urls_arobase",
    "etape_2": "commentaires_sans_doublons",
    "etape_3": "commentaires_sans_emojis",     # après extraire_emojis
    "etape_4": "commentaires_normalises",      # après normalisation
    # L'étape 5 modifie en place commentaires_normalises, pas de nouvelle collection
}

# ============================================================
# CONNEXION KAFKA
# ============================================================
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

def send_kafka_message(event_type, data):
    message = {
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }
    producer.send("pipeline_events", message)
    producer.flush()
    print(f"📨 Kafka: {event_type}")

# ============================================================
# FONCTIONS MONGODB
# ============================================================
def compter_docs(collection_name, query=None):
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[DB_NAME]
        if query:
            count = db[collection_name].count_documents(query)
        else:
            count = db[collection_name].count_documents({})
        client.close()
        return count
    except Exception as e:
        return -1

def inserer_commentaire_test():
    """Insère un commentaire de test avec traite=false et date au bon type"""
    commentaires_test = [
        "#ntouma 00000 3lablkom wlh ?/?//% !!! 😡😡😡"
    ]
    texte = random.choice(commentaires_test)
    
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db["commentaires_bruts"]
    
    doc = {
        "Commentaire_Client": texte,
        "source": "Test_manuel",
        "statut": "brut",
        "traite": False,
        "date": datetime.now(),
        "metadata": {"fichier": "test_manuel"}
    }
    
    result = collection.insert_one(doc)
    client.close()
    print(f"✅ Commentaire test inséré: {texte[:50]}...")
    return result.inserted_id

def marquer_comme_traites(collection_name, ids):
    if not ids:
        return 0
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[DB_NAME]
        collection = db[collection_name]
        result = collection.update_many(
            {"_id": {"$in": ids}},
            {"$set": {"traite": True, "date_traitement": datetime.now()}}
        )
        client.close()
        return result.modified_count
    except Exception as e:
        print(f"⚠️ Erreur marquage: {e}")
        return 0

def get_nouveaux_ids(collection_name):
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[DB_NAME]
        collection = db[collection_name]
        query = {"traite": False}
        docs = list(collection.find(query, {"_id": 1}))
        client.close()
        return [doc["_id"] for doc in docs]
    except Exception as e:
        return []

# ============================================================
# TÂCHES PREFECT (une par étape)
# ============================================================

@task(name="Insertion commentaire test", retries=0)
def task_inserer_commentaire():
    print("\n" + "="*60)
    print("📝 Insertion d'un commentaire de test")
    print("="*60)
    inserer_commentaire_test()
    return {"status": "ok"}

@task(name="Étape 1 — Suppression URLs", retries=1)
def task_etape_1():
    print("\n" + "="*60)
    print("🔹 ÉTAPE 1 — Suppression des URLs")
    print("="*60)
    
    avant = compter_docs(COLLECTIONS["source"], {"traite": False})
    print(f"📥 Nouveaux à traiter: {avant}")
    
    if avant == 0:
        print("✅ Aucun nouveau document")
        return {"avant": 0, "apres": 0}
    
    send_kafka_message("etape_1_debut", {"total": avant})
    
    script = SCRIPTS["etape_1"]
    debut = time.time()
    result = subprocess.run(["python3", script], capture_output=True, text=True)
    duree = time.time() - debut
    
    if result.returncode != 0:
        print("❌ Erreur:", result.stderr)
        send_kafka_message("etape_1_erreur", {"error": result.stderr[-200:]})
        raise Exception("Étape 1 échouée")
    
    apres = compter_docs(COLLECTIONS["etape_1"])
    
    ids = get_nouveaux_ids(COLLECTIONS["source"])
    marquer_comme_traites(COLLECTIONS["source"], ids)
    
    send_kafka_message("etape_1_fin", {"avant": avant, "apres": apres, "duree": duree})
    print(f"✅ Terminé en {duree:.1f}s")
    return {"avant": avant, "apres": apres}

@task(name="Étape 2 — Suppression doublons", retries=1)
def task_etape_2():
    print("\n" + "="*60)
    print("🔹 ÉTAPE 2 — Suppression des doublons")
    print("="*60)
    
    send_kafka_message("etape_2_debut", {})
    
    script = SCRIPTS["etape_2"]
    debut = time.time()
    result = subprocess.run(["python3", script], capture_output=True, text=True)
    duree = time.time() - debut
    
    if result.returncode != 0:
        print("❌ Erreur:", result.stderr)
        send_kafka_message("etape_2_erreur", {"error": result.stderr[-200:]})
        raise Exception("Étape 2 échouée")
    
    apres = compter_docs(COLLECTIONS["etape_2"])
    send_kafka_message("etape_2_fin", {"apres": apres, "duree": duree})
    print(f"✅ Terminé en {duree:.1f}s")
    return {"apres": apres}

@task(name="Étape 3 — Extraction des emojis", retries=1)
def task_etape_3():
    print("\n" + "="*60)
    print("🔹 ÉTAPE 3 — Extraction des emojis (extraire_emojis_multinode)")
    print("="*60)
    
    send_kafka_message("etape_3_debut", {})
    
    script = SCRIPTS["etape_3"]
    debut = time.time()
    result = subprocess.run(["python3", script], capture_output=True, text=True)
    duree = time.time() - debut
    
    if result.returncode != 0:
        print("❌ Erreur:", result.stderr)
        send_kafka_message("etape_3_erreur", {"error": result.stderr[-200:]})
        raise Exception("Étape 3 échouée")
    
    apres = compter_docs(COLLECTIONS["etape_3"])
    send_kafka_message("etape_3_fin", {"apres": apres, "duree": duree})
    print(f"✅ Terminé en {duree:.1f}s")
    return {"apres": apres}

@task(name="Étape 4 — Normalisation", retries=1)
def task_etape_4():
    print("\n" + "="*60)
    print("🔹 ÉTAPE 4 — Normalisation du texte (normalisation_multinode)")
    print("="*60)
    
    send_kafka_message("etape_4_debut", {})
    
    script = SCRIPTS["etape_4"]
    debut = time.time()
    # Important : exécuter le script dans son propre répertoire pour que les imports relatifs fonctionnent
    script_dir = os.path.dirname(script)
    result = subprocess.run(
        ["python3", script],
        cwd=script_dir,
        capture_output=True,
        text=True
    )
    duree = time.time() - debut
    
    # Afficher la sortie pour le débogage
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        print("❌ Erreur:", result.stderr)
        send_kafka_message("etape_4_erreur", {"error": result.stderr[-500:]})
        raise Exception(f"Étape 4 échouée : {result.stderr[-200:]}")
    
    apres = compter_docs(COLLECTIONS["etape_4"])
    send_kafka_message("etape_4_fin", {"apres": apres, "duree": duree})
    print(f"✅ Terminé en {duree:.1f}s")
    return {"apres": apres}

# ============================================================
# FLOW PRINCIPAL
# ============================================================

@flow(name="Pipeline Nettoyage Complet", log_prints=True)
def pipeline_complet():
    print("="*60)
    print("🚀 PIPELINE COMPLET AVEC PREFECT DASHBOARD")
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    send_kafka_message("pipeline_debut", {"message": "Pipeline démarré"})
    
    # 1. Insérer un commentaire test
    task_inserer_commentaire()
    
    # 2. Exécuter les 4 étapes
    task_etape_1()
    task_etape_2()
    task_etape_3()
    task_etape_4()
 
    
    send_kafka_message("pipeline_fin", {"message": "Pipeline terminé"})
    
    print("\n" + "="*60)
    print("🎉 PIPELINE COMPLET TERMINÉ !")
    print("="*60)

# ============================================================
# LANCEMENT
# ============================================================
if __name__ == "__main__":
    pipeline_complet()