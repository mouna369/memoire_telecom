from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(MONGO_URI)
db = client["telecom_algerie_new"]

ancienne = db["dataset_unifie_copie"]
nouvelle = db["dataset_unifie_sans_doublons"]

print("="*60)
print("VÉRIFICATION DU REGROUPEMENT DES DOUBLONS")
print("="*60)

# 1. Comparaison des tailles
nb_ancien = ancienne.count_documents({})
nb_nouveau = nouvelle.count_documents({})
print(f"\n1. Taille des collections :")
print(f"   Ancienne : {nb_ancien}")
print(f"   Nouvelle : {nb_nouveau}")
print(f"   Différence : {nb_ancien - nb_nouveau} doublons supprimés")

# 2. Vérifier l'absence de doublons dans la nouvelle
pipeline = [
    {"$group": {"_id": "$normalized_arabert", "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}}
]
doublons = list(nouvelle.aggregate(pipeline))
print(f"\n2. Doublons dans nouvelle collection : {len(doublons)}")
if len(doublons) == 0:
    print("   ✅ Aucun doublon trouvé")
else:
    print(f"   ❌ {len(doublons)} textes en double")

# 3. Vérifier la somme des nb_occurrences
pipeline_sum = [
    {"$group": {"_id": None, "total": {"$sum": "$nb_occurrences"}}}
]
resultat = list(nouvelle.aggregate(pipeline_sum))
total_occurrences = resultat[0]['total'] if resultat else 0
print(f"\n3. Somme des nb_occurrences : {total_occurrences}")
print(f"   Doit être égal à {nb_ancien}")
if total_occurrences == nb_ancien:
    print("   ✅ Parfait !")
else:
    print(f"   ❌ Différence de {nb_ancien - total_occurrences}")

# 4. Afficher les textes qui avaient le plus de doublons
print(f"\n4. Top 10 des textes avec le plus de doublons :")
cursor = nouvelle.find().sort("nb_occurrences", -1).limit(10)
for doc in cursor:
    if doc['nb_occurrences'] > 1:
        print(f"   - '{doc['normalized_arabert'][:40]}' : {doc['nb_occurrences']} fois")

client.close()
print("\n🔒 Vérification terminée")