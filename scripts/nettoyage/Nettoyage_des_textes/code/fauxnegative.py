# verifier_faux_negatifs.py
from pymongo import MongoClient
from collections import defaultdict

client = MongoClient("mongodb://localhost:27018/")
db = client["telecom_algerie"]

# 1. Chercher les textes IDENTIQUES dans la destination
collection = db["commentaires_sans_doublons_tolerance"]

pipeline = [
    {"$group": {
        "_id": "$Commentaire_Client",
        "count": {"$sum": 1},
        "ids": {"$push": "$_id"}
    }},
    {"$match": {"count": {"$gt": 1}}}
]

doublons_exacts = list(collection.aggregate(pipeline))

if len(doublons_exacts) == 0:
    print("✅ Aucun doublon exact restant !")
else:
    print(f"⚠️ {len(doublons_exacts)} doublons exacts trouvés :")
    for d in doublons_exacts[:5]:
        print(f"   '{d['_id'][:50]}' : {d['count']} exemplaires")