# # #!/usr/bin/env python3
# # # kafka_producer.py

# # from kafka import KafkaProducer
# # import json
# # import time

# # producer = KafkaProducer(
# #     bootstrap_servers=['localhost:9092'],
# #     value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
# # )

# # print("="*60)
# # print("🚀 ENVOI DE COMMENTAIRES DE TEST")
# # print("="*60)

# # commentaires = [
# #     {"commentaire": "rependre moi algerei", "source": "facebook"},
# #     {"commentaire": "Très bon service !", "source": "twitter"},
# #     {"commentaire": "Service nul, connexion coupée", "source": "facebook"},
# # ]

# # for i, data in enumerate(commentaires, 1):
# #     print(f"\n📤 [{i}] Envoi: {data['commentaire']}")
# #     producer.send('commentaires_bruts', data)
# #     producer.flush()
# #     time.sleep(1)

# # print("\n✅ Terminé")
# # producer.close()


# #!/usr/bin/env python3
# # kafka_producer.py - Envoie les nouveaux commentaires vers Kafka

# from kafka import KafkaProducer
# import json
# from pymongo import MongoClient
# from datetime import datetime
# import time

# # ============================================================
# # CONFIGURATION
# # ============================================================
# KAFKA_BOOTSTRAP = "localhost:9092"
# TOPIC = "commentaires_bruts"

# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# COLLECTION_SOURCE = "commentaires_bruts"

# # ============================================================
# # FONCTION POUR RÉCUPÉRER LES NOUVEAUX COMMENTAIRES
# # ============================================================

# def get_nouveaux_commentaires():
#     """Récupère les commentaires avec traite=false"""
#     client = MongoClient(MONGO_URI)
#     db = client[DB_NAME]
#     collection = db[COLLECTION_SOURCE]
    
#     # Récupérer les commentaires non encore envoyés à Kafka
#     query = {"envoye_kafka": {"$ne": True}, "traite": False}
#     docs = list(collection.find(query))
#     client.close()
#     return docs

# def marquer_comme_envoye(ids):
#     """Marque les commentaires comme envoyés à Kafka"""
#     if not ids:
#         return
#     client = MongoClient(MONGO_URI)
#     db = client[DB_NAME]
#     collection = db[COLLECTION_SOURCE]
    
#     collection.update_many(
#         {"_id": {"$in": ids}},
#         {"$set": {"envoye_kafka": True, "date_envoi_kafka": datetime.now()}}
#     )
#     client.close()

# # ============================================================
# # PRODUCER KAFKA
# # ============================================================

# producer = KafkaProducer(
#     bootstrap_servers=[KAFKA_BOOTSTRAP],
#     value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
# )

# print("="*60)
# print("🚀 KAFKA PRODUCER - Envoi des nouveaux commentaires")
# print(f"   Topic: {TOPIC}")
# print("="*60)

# while True:
#     # Récupérer les nouveaux commentaires
#     nouveaux = get_nouveaux_commentaires()
    
#     if nouveaux:
#         print(f"\n📥 {len(nouveaux)} nouveaux commentaires trouvés")
#         ids_envoyes = []
        
#         for doc in nouveaux:
#             message = {
#                 "commentaire": doc.get("Commentaire_Client", ""),
#                 "source": doc.get("source", "inconnu"),
#                 "mongo_id": str(doc["_id"]),
#                 "timestamp": datetime.now().isoformat()
#             }
            
#             producer.send(TOPIC, message)
#             ids_envoyes.append(doc["_id"])
            
#             print(f"   📤 Envoyé: {message['commentaire'][:50]}...")
        
#         producer.flush()
#         marquer_comme_envoye(ids_envoyes)
#         print(f"   ✅ {len(ids_envoyes)} commentaires marqués comme envoyés")
#     else:
#         print(".", end="", flush=True)
    
#     time.sleep(5)  # Vérifier toutes les 5 secondes


#!/usr/bin/env python3
# kafka_producer.py - Version qui lit MongoDB et envoie à Kafka

from kafka import KafkaProducer
from pymongo import MongoClient
import json
import time
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "commentaires_bruts"

MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_bruts"

# ============================================================
# PRODUCER
# ============================================================

print("=" * 60)
print("🚀 KAFKA PRODUCER - Lecture MongoDB")
print(f"   Topic: {TOPIC}")
print("   En attente de nouveaux commentaires...")
print("=" * 60)

# Connexion Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

print("✅ Connecté à Kafka")

while True:
    try:
        # Connexion MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_SOURCE]
        
        # Chercher les commentaires non envoyés
        query = {
            "envoye_kafka": {"$ne": True},
            "traite": False
        }
        
        docs = list(collection.find(query).limit(10))
        
        if docs:
            print(f"\n📥 {len(docs)} nouveau(x) commentaire(s) trouvé(s)")
            
            for doc in docs:
                message = {
                    "commentaire": doc.get("Commentaire_Client", ""),
                    "source": doc.get("source", "inconnu"),
                    "mongo_id": str(doc["_id"]),
                    "timestamp": datetime.now().isoformat()
                }
                
                # Envoyer à Kafka
                future = producer.send(TOPIC, message)
                result = future.get(timeout=5)
                
                # Marquer comme envoyé
                collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {
                        "envoye_kafka": True,
                        "date_envoi_kafka": datetime.now()
                    }}
                )
                
                print(f"   ✅ Envoyé: {message['commentaire'][:50]}... (offset: {result.offset})")
        
        client.close()
        
    except Exception as e:
        print(f"⚠️ Erreur: {e}")
    
    time.sleep(5)  # Vérifier toutes les 5 secondes