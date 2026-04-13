#!/usr/bin/env python3
# comparaison_500_commentaires.py
# Compare Edit Distance / Gower / Cosine TF-IDF / Jaccard Caractères / Jaccard Mots
# sur 500 vrais commentaires MongoDB

# pip install python-Levenshtein scikit-learn gower pandas numpy pymongo

import time
import pandas as pd
from pymongo import MongoClient

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI        = "mongodb://localhost:27018/"
DB_NAME          = "telecom_algerie"
COLLECTION       = "commentaires_sans_urls_arobase"
NB_DOCS          = 500
SEUIL_EDIT       = 0.85   # Edit Distance
SEUIL_GOWER      = 0.15   # Gower (distance, pas similarité)
SEUIL_TFIDF      = 0.85   # Cosine TF-IDF
SEUIL_JACCARD_C  = 0.85   # Jaccard sur caractères
SEUIL_JACCARD_M  = 0.85   # Jaccard sur mots


# ============================================================
# CHARGEMENT DEPUIS MONGODB
# ============================================================

def charger_commentaires():
    print("📥 Connexion MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION]

    total = collection.count_documents({})
    print(f"   Total dans la collection : {total}")
    print(f"   Chargement de {NB_DOCS} documents...")

    docs = list(collection.find(
        {},
        {"_id": 1, "Commentaire_Client": 1, "source": 1, "moderateur": 1}
    ).limit(NB_DOCS))

    # Nettoyage
    for i, doc in enumerate(docs):
        doc["id"] = i + 1
        doc["_id"] = str(doc["_id"])
        doc["Commentaire_Client"] = str(doc.get("Commentaire_Client", "")).strip()
        doc["source"] = str(doc.get("source", "inconnu"))
        doc["moderateur"] = str(doc.get("moderateur", "inconnu"))

    # Filtrer les commentaires vides
    docs = [d for d in docs if len(d["Commentaire_Client"]) > 0]

    client.close()
    print(f"   ✅ {len(docs)} documents chargés (après filtre vides)")
    return docs


# ============================================================
# MÉTHODE 1 — EDIT DISTANCE (LEVENSHTEIN)
# ============================================================

def deduplication_edit(docs, seuil=SEUIL_EDIT):
    import Levenshtein

    gardes = []
    supprimes = set()

    for i, doc in enumerate(docs):
        if i in supprimes:
            continue
        gardes.append(doc)
        t1 = doc["Commentaire_Client"].lower().strip()
        for j in range(i + 1, len(docs)):
            if j not in supprimes:
                t2 = docs[j]["Commentaire_Client"].lower().strip()
                if t1 == t2:
                    sim = 1.0
                else:
                    dist = Levenshtein.distance(t1, t2)
                    sim = 1 - dist / max(len(t1), len(t2), 1)
                if sim >= seuil:
                    supprimes.add(j)

    return gardes, supprimes


# ============================================================
# MÉTHODE 2 — GOWER
# ============================================================

def deduplication_gower(docs, seuil_distance=SEUIL_GOWER):
    import gower
    import numpy as np

    df = pd.DataFrame([{
        "longueur":   len(d["Commentaire_Client"]),
        "source":     d["source"],
        "moderateur": d["moderateur"],
    } for d in docs])

    dist_matrix = gower.gower_matrix(df)

    gardes = []
    supprimes = set()
    for i in range(len(docs)):
        if i in supprimes:
            continue
        gardes.append(docs[i])
        for j in range(i + 1, len(docs)):
            if j not in supprimes and dist_matrix[i][j] <= seuil_distance:
                supprimes.add(j)

    return gardes, supprimes


# ============================================================
# MÉTHODE 3 — COSINE TF-IDF
# ============================================================

def deduplication_tfidf(docs, seuil=SEUIL_TFIDF):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    textes = [d["Commentaire_Client"].lower().strip() for d in docs]

    vectorizer = TfidfVectorizer(
        analyzer='word',
        ngram_range=(1, 2),
        min_df=1,
        sublinear_tf=True
    )
    tfidf_matrix = vectorizer.fit_transform(textes)
    sim_matrix = cosine_similarity(tfidf_matrix)

    gardes = []
    supprimes = set()
    for i in range(len(docs)):
        if i in supprimes:
            continue
        gardes.append(docs[i])
        for j in range(i + 1, len(docs)):
            if j not in supprimes and sim_matrix[i][j] >= seuil:
                supprimes.add(j)

    return gardes, supprimes


# ============================================================
# MÉTHODE 4 — JACCARD SUR LES CARACTÈRES (ton code original)
# ============================================================

def jaccard_caracteres(t1, t2):
    """
    Jaccard sur les caractères — ton code original.
    Problème connu : 'bon courage' et 'bonne cuisine'
    partagent beaucoup de caractères mais ne sont pas similaires.
    """
    t1 = str(t1).lower().strip()
    t2 = str(t2).lower().strip()
    if t1 == t2:
        return 1.0
    set1 = set(t1)
    set2 = set(t2)
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union


def deduplication_jaccard_caracteres(docs, seuil=SEUIL_JACCARD_C):
    gardes = []
    supprimes = set()
    for i, doc in enumerate(docs):
        if i in supprimes:
            continue
        gardes.append(doc)
        t1 = doc["Commentaire_Client"]
        for j in range(i + 1, len(docs)):
            if j not in supprimes:
                sim = jaccard_caracteres(t1, docs[j]["Commentaire_Client"])
                if sim >= seuil:
                    supprimes.add(j)
    return gardes, supprimes


# ============================================================
# MÉTHODE 5 — JACCARD SUR LES MOTS (version améliorée)
# ============================================================

def jaccard_mots(t1, t2):
    """
    Jaccard sur les MOTS — version corrigée recommandée.
    Compare les ensembles de mots, pas de caractères.
    Meilleur pour détecter les vraies similarités textuelles.
    """
    t1 = str(t1).lower().strip()
    t2 = str(t2).lower().strip()
    if t1 == t2:
        return 1.0
    set1 = set(t1.split())
    set2 = set(t2.split())
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union


def deduplication_jaccard_mots(docs, seuil=SEUIL_JACCARD_M):
    gardes = []
    supprimes = set()
    for i, doc in enumerate(docs):
        if i in supprimes:
            continue
        gardes.append(doc)
        t1 = doc["Commentaire_Client"]
        for j in range(i + 1, len(docs)):
            if j not in supprimes:
                sim = jaccard_mots(t1, docs[j]["Commentaire_Client"])
                if sim >= seuil:
                    supprimes.add(j)
    return gardes, supprimes


# ============================================================
# AFFICHAGE DES EXEMPLES DE DOUBLONS DÉTECTÉS
# ============================================================

def afficher_exemples_doublons(docs, supprimes, methode, n=10):
    """Affiche les n premiers exemples de doublons détectés"""
    print(f"\n   📋 Exemples de doublons détectés ({methode}) :")
    count = 0
    for idx in sorted(supprimes):
        if count >= n:
            break
        print(f"      → supprimé : \"{docs[idx]['Commentaire_Client'][:80]}\"")
        count += 1


# ============================================================
# COMPARAISON PRINCIPALE
# ============================================================

def comparer():
    print("=" * 75)
    print("📊 COMPARAISON 5 MÉTHODES — 500 COMMENTAIRES RÉELS")
    print(f"   Source : {COLLECTION} | MongoDB : {MONGO_URI}")
    print("=" * 75)

    # Chargement
    docs = charger_commentaires()
    nb_total = len(docs)

    resultats = {}

    # --- Edit Distance ---
    print(f"\n⏳ [1/5] Edit Distance (seuil={SEUIL_EDIT})...")
    try:
        t0 = time.time()
        gardes, supprimes = deduplication_edit(docs)
        temps = time.time() - t0
        resultats["Edit Distance"] = {
            "gardes": gardes, "supprimes": supprimes,
            "temps": temps, "erreur": None
        }
        print(f"   ✅ Terminé en {temps:.2f}s — {len(supprimes)} doublons trouvés")
        afficher_exemples_doublons(docs, supprimes, "Edit Distance")
    except ImportError:
        print("   ❌ Module manquant : pip install python-Levenshtein")
        resultats["Edit Distance"] = {"erreur": "pip install python-Levenshtein"}

    # --- Gower ---
    print(f"\n⏳ [2/5] Gower Distance (seuil={SEUIL_GOWER})...")
    try:
        t0 = time.time()
        gardes, supprimes = deduplication_gower(docs)
        temps = time.time() - t0
        resultats["Gower"] = {
            "gardes": gardes, "supprimes": supprimes,
            "temps": temps, "erreur": None
        }
        print(f"   ✅ Terminé en {temps:.2f}s — {len(supprimes)} doublons trouvés")
        afficher_exemples_doublons(docs, supprimes, "Gower")
    except ImportError:
        print("   ❌ Module manquant : pip install gower pandas")
        resultats["Gower"] = {"erreur": "pip install gower pandas"}

    # --- TF-IDF ---
    print(f"\n⏳ [3/5] Cosine TF-IDF (seuil={SEUIL_TFIDF})...")
    try:
        t0 = time.time()
        gardes, supprimes = deduplication_tfidf(docs)
        temps = time.time() - t0
        resultats["Cosine TF-IDF"] = {
            "gardes": gardes, "supprimes": supprimes,
            "temps": temps, "erreur": None
        }
        print(f"   ✅ Terminé en {temps:.2f}s — {len(supprimes)} doublons trouvés")
        afficher_exemples_doublons(docs, supprimes, "Cosine TF-IDF")
    except ImportError:
        print("   ❌ Module manquant : pip install scikit-learn")
        resultats["Cosine TF-IDF"] = {"erreur": "pip install scikit-learn"}

    # --- Jaccard Caractères ---
    print(f"\n⏳ [4/5] Jaccard Caractères (seuil={SEUIL_JACCARD_C})...")
    t0 = time.time()
    gardes, supprimes = deduplication_jaccard_caracteres(docs)
    temps = time.time() - t0
    resultats["Jaccard Caractères"] = {
        "gardes": gardes, "supprimes": supprimes,
        "temps": temps, "erreur": None
    }
    print(f"   ✅ Terminé en {temps:.2f}s — {len(supprimes)} doublons trouvés")
    afficher_exemples_doublons(docs, supprimes, "Jaccard Caractères")

    # --- Jaccard Mots ---
    print(f"\n⏳ [5/5] Jaccard Mots (seuil={SEUIL_JACCARD_M})...")
    t0 = time.time()
    gardes, supprimes = deduplication_jaccard_mots(docs)
    temps = time.time() - t0
    resultats["Jaccard Mots"] = {
        "gardes": gardes, "supprimes": supprimes,
        "temps": temps, "erreur": None
    }
    print(f"   ✅ Terminé en {temps:.2f}s — {len(supprimes)} doublons trouvés")
    afficher_exemples_doublons(docs, supprimes, "Jaccard Mots")

    # --------------------------------------------------------
    # TABLEAU RÉCAPITULATIF
    # --------------------------------------------------------
    print(f"\n{'='*75}")
    print("📊 TABLEAU RÉCAPITULATIF FINAL")
    print(f"{'='*75}")
    print(f"{'Méthode':<20} {'Gardés':>8} {'Supprimés':>11} {'Réduction':>11} {'Temps':>10}")
    print("-" * 65)
    for nom, res in resultats.items():
        if res.get("erreur"):
            print(f"{nom:<20} {'N/A':>8} {'N/A':>11} {'N/A':>11} {'N/A':>10}")
        else:
            nb_s = len(res["supprimes"])
            nb_g = len(res["gardes"])
            taux = nb_s / nb_total * 100
            ms   = res["temps"]
            print(f"{nom:<20} {nb_g:>8} {nb_s:>11} {taux:>10.1f}% {ms:>9.2f}s")

    # --------------------------------------------------------
    # ANALYSE DES DIFFÉRENCES ENTRE MÉTHODES
    # --------------------------------------------------------
    ok = [n for n, r in resultats.items() if not r.get("erreur")]
    if len(ok) >= 2:
        print(f"\n{'='*75}")
        print("🔍 ANALYSE — DIFFÉRENCES ENTRE MÉTHODES")
        print(f"{'='*75}")

        sets = {nom: resultats[nom]["supprimes"] for nom in ok}

        for nom_a in ok:
            for nom_b in ok:
                if nom_a >= nom_b:
                    continue
                only_a = sets[nom_a] - sets[nom_b]
                only_b = sets[nom_b] - sets[nom_a]
                commun = sets[nom_a] & sets[nom_b]
                print(f"\n   {nom_a} vs {nom_b} :")
                print(f"      Commun          : {len(commun)} doublons")
                print(f"      Seulement {nom_a:<15}: {len(only_a)} doublons")
                print(f"      Seulement {nom_b:<15}: {len(only_b)} doublons")

                # Exemples uniques à chaque méthode
                if only_a:
                    ex = list(only_a)[:3]
                    print(f"      Exemples propres à {nom_a} :")
                    for idx in ex:
                        print(f"         \"{docs[idx]['Commentaire_Client'][:70]}\"")
                if only_b:
                    ex = list(only_b)[:3]
                    print(f"      Exemples propres à {nom_b} :")
                    for idx in ex:
                        print(f"         \"{docs[idx]['Commentaire_Client'][:70]}\"")

    # --------------------------------------------------------
    # CONCLUSION
    # --------------------------------------------------------
    print(f"\n{'='*75}")
    print("💡 CONCLUSION SUR TES DONNÉES RÉELLES")
    print(f"{'='*75}")
    if all(not r.get("erreur") for r in resultats.values()):
        meilleur = max(
            resultats.items(),
            key=lambda x: len(x[1]["supprimes"]) if not x[1].get("erreur") else -1
        )
        print(f"""
   Sur tes {nb_total} commentaires réels :

   • Edit Distance       → fautes de frappe et variantes mineures
   • Gower               → doublons multi-champs (source + modérateur + longueur)
   • Cosine TF-IDF       → paraphrases et reformulations
   • Jaccard Caractères  → partage de caractères (peut créer des faux positifs)
   • Jaccard Mots        → partage de mots (bon compromis sans librairie externe)

   🏆 Méthode qui a trouvé le plus de doublons : {meilleur[0]}
      ({len(meilleur[1]['supprimes'])} supprimés / {nb_total} = {len(meilleur[1]['supprimes'])/nb_total*100:.1f}%)

   👉 Pour ton projet télécom (commentaires arabes/français reformulés) :
      Cosine TF-IDF reste la meilleure option globale.
      Jaccard Mots est un bon choix si tu veux éviter les dépendances externes.
        """)
    print("=" * 75)


# ============================================================
# LANCEMENT
# ============================================================

if __name__ == "__main__":
    # Vérification des dépendances
    manquants = []
    try:
        import Levenshtein
    except ImportError:
        manquants.append("python-Levenshtein")
    try:
        import sklearn
    except ImportError:
        manquants.append("scikit-learn")
    try:
        import gower
    except ImportError:
        manquants.append("gower")

    if manquants:
        print(f"⚠️  Modules manquants : pip install {' '.join(manquants)}")
        print("   Le script continuera sans les méthodes indisponibles.\n")

    comparer()