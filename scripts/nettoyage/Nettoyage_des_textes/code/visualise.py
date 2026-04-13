# distribution_similarites.py
from pymongo import MongoClient
import random

client = MongoClient("mongodb://localhost:27018/")
db = client["telecom_algerie"]

# Prendre 200 commentaires
echantillon = list(db["commentaires_sans_urls_arobase"].find().limit(200))
textes = [doc.get("Commentaire_Client", "") for doc in echantillon if doc.get("Commentaire_Client")]

def similarite_jaccard(t1, t2):
    if not t1 or not t2:
        return 0
    set1 = set(str(t1).lower().strip())
    set2 = set(str(t2).lower().strip())
    if not set1 or not set2:
        return 0
    return (len(set1 & set2) / len(set1 | set2)) * 100

# Calculer les similarités
similarites = []
for i in range(min(100, len(textes))):
    for j in range(i+1, min(i+20, len(textes))):
        sim = similarite_jaccard(textes[i], textes[j])
        similarites.append(sim)

# Afficher la distribution
print("📊 DISTRIBUTION DES SIMILARITÉS :")
print(f"   Nombre de paires comparées : {len(similarites)}")
print(f"   Similarité moyenne : {sum(similarites)/len(similarites):.1f}%")
print(f"   Similarité min : {min(similarites):.1f}%")
print(f"   Similarité max : {max(similarites):.1f}%")

# Compter par tranche
tranches = [(0,50), (50,70), (70,80), (80,85), (85,90), (90,95), (95,100)]
print("\n📊 RÉPARTITION :")
for min_s, max_s in tranches:
    count = sum(1 for s in similarites if min_s <= s < max_s)
    print(f"   {min_s:3}-{max_s:3}% : {count:4} paires ({count/len(similarites)*100:.1f}%)")