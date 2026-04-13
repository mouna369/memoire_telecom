# test_seuils_optimaux.py
# À exécuter APRES avoir lancé ton script principal

from pymongo import MongoClient
from collections import defaultdict
import random

# Connexion MongoDB
client = MongoClient("mongodb://localhost:27018/")
db = client["telecom_algerie"]

# Récupérer un échantillon de la collection SOURCE (avant déduplication)
print("📥 Récupération d'un échantillon...")
echantillon = list(db["commentaires_sans_urls_arobase"].aggregate([
    {"$sample": {"size": 500}},  # Prendre 500 commentaires aléatoires
    {"$project": {"Commentaire_Client": 1, "_id": 0}}
]))

textes = [doc.get("Commentaire_Client", "") for doc in echantillon if doc.get("Commentaire_Client")]
print(f"✅ {len(textes)} commentaires chargés")

# ============================================================
# FONCTION DE SIMILARITÉ (identique à ton code)
# ============================================================

def similarite_jaccard(t1, t2):
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

# ============================================================
# TEST POUR DIFFÉRENTS SEUILS
# ============================================================

print("\n" + "=" * 70)
print("🔍 RECHERCHE DU TAUX DE TOLÉRANCE OPTIMAL")
print("=" * 70)

seuils_a_tester = [70, 75, 80, 85, 90, 95]
resultats = []

for seuil in seuils_a_tester:
    # Simuler la déduplication
    documents_gardes = []
    doublons_trouves = 0
    
    for texte in textes:
        est_doublon = False
        for texte_garde in documents_gardes:
            if similarite_jaccard(texte, texte_garde) >= seuil:
                est_doublon = True
                doublons_trouves += 1
                break
        if not est_doublon:
            documents_gardes.append(texte)
    
    # Calcul des statistiques
    nb_unique_original = len(set(textes))  # Textes exactement identiques
    nb_gardes = len(documents_gardes)
    nb_supprimes = len(textes) - nb_gardes
    
    resultats.append({
        "seuil": seuil,
        "gardes": nb_gardes,
        "supprimes": nb_supprimes,
        "taux_reduction": (nb_supprimes / len(textes)) * 100,
        "unique_theorique": nb_unique_original
    })

# ============================================================
# AFFICHAGE DES RÉSULTATS
# ============================================================

print("\n📊 RÉSULTATS PAR SEUIL :")
print("-" * 70)
print(f"{'Seuil':<10} {'Gardés':<10} {'Supprimés':<12} {'Taux réduction':<15} {'Commentaires uniques théoriques':<30}")
print("-" * 70)

for r in resultats:
    print(f"{r['seuil']}%{'':<5} {r['gardes']:<10} {r['supprimes']:<12} {r['taux_reduction']:.1f}%{'':<10} {r['unique_theorique']}")

print("-" * 70)

# ============================================================
# RECOMMANDATION
# ============================================================

print("\n💡 RECOMMANDATION :")
print("-" * 70)

# Trouver le seuil où le taux de réduction se stabilise
meilleur_seuil = 85  # par défaut
for i in range(1, len(resultats)):
    if resultats[i]["taux_reduction"] - resultats[i-1]["taux_reduction"] < 5:
        meilleur_seuil = resultats[i-1]["seuil"]
        break

print(f"✅ Seuil recommandé : {meilleur_seuil}%")
print(f"   → Équilibre entre suppression des doublons et conservation des commentaires uniques")

print("\n📝 INTERPRÉTATION :")
print("   - Seuil bas (70-75%) : supprime BEAUCOUP, mais risque de supprimer des commentaires différents")
print("   - Seuil moyen (80-85%) : équilibre recommandé")
print("   - Seuil haut (90-95%) : supprime PEU, plus sûr mais laisse des doublons")

# ============================================================
# VÉRIFICATION MANUELLE DES CAS LIMITES
# ============================================================

print("\n" + "=" * 70)
print("🔍 VÉRIFICATION MANUELLE DES CAS LIMITES")
print("=" * 70)

# Trouver des paires de textes avec similarité entre 80% et 90%
print("\n📝 Exemples de textes avec similarité entre 80% et 90% :")
exemples_trouves = 0
for i in range(min(100, len(textes))):
    for j in range(i+1, min(i+20, len(textes))):
        sim = similarite_jaccard(textes[i], textes[j])
        if 80 <= sim < 90:
            print(f"\n   Similarité : {sim:.1f}%")
            print(f"   Texte 1 : {textes[i][:80]}")
            print(f"   Texte 2 : {textes[j][:80]}")
            exemples_trouves += 1
            if exemples_trouves >= 5:
                break
    if exemples_trouves >= 5:
        break

if exemples_trouves == 0:
    print("   Aucun exemple trouvé dans l'échantillon")

print("\n" + "=" * 70)
print("✅ Test terminé !")