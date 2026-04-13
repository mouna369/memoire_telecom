# verifier_faux_positifs.py
from pymongo import MongoClient
import random

client = MongoClient("mongodb://localhost:27018/")
db = client["telecom_algerie"]

# Récupérer les commentaires supprimés (fichier log)
# Si tu n'as pas de log, prends un échantillon de la destination
destination = list(db["commentaires_sans_doublons_tolerance"].find().limit(500))
source = list(db["commentaires_sans_urls_arobase"].find().limit(500))

def similarite_jaccard(t1, t2):
    if not t1 or not t2:
        return 0
    set1 = set(str(t1).lower().strip())
    set2 = set(str(t2).lower().strip())
    if not set1 or not set2:
        return 0
    return (len(set1 & set2) / len(set1 | set2)) * 100

# Vérifier si des commentaires gardés sont très similaires entre eux
print("🔍 Vérification des faux positifs potentiels...")
faux_positifs = []

for i, doc1 in enumerate(destination):
    t1 = doc1.get("Commentaire_Client", "")
    for j, doc2 in enumerate(destination):
        if i < j:
            t2 = doc2.get("Commentaire_Client", "")
            sim = similarite_jaccard(t1, t2)
            if sim >= 85:  # Seuil utilisé
                faux_positifs.append({
                    "t1": t1[:50],
                    "t2": t2[:50],
                    "similarite": sim
                })

if len(faux_positifs) == 0:
    print("✅ Aucun faux positif détecté !")
else:
    print(f"⚠️ {len(faux_positifs)} paires similaires trouvées :")
    for fp in faux_positifs[:5]:
        print(f"   {fp['similarite']:.1f}% : {fp['t1']} ↔ {fp['t2']}")