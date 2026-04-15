#!/usr/bin/env python3
# comparaison_7_methodes_avec_embeddings.py
# Compare 7 méthodes sur 1000 commentaires

# pip install python-Levenshtein scikit-learn pymongo pandas numpy sentence-transformers

import time
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
import json
import os

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI        = "mongodb://localhost:27018/"
DB_NAME          = "telecom_algerie"
COLLECTION       = "commentaires_sans_urls_arobase"
NB_DOCS          = 2000
SEUIL            = 0.85

# ============================================================
# CHARGEMENT DES COMMENTAIRES
# ============================================================

def charger_commentaires():
    print("📥 Connexion MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION]

    docs = list(collection.find({}, {"_id": 1, "Commentaire_Client": 1}).limit(NB_DOCS))

    for i, doc in enumerate(docs):
        doc["index"] = i
        doc["Commentaire_Client"] = str(doc.get("Commentaire_Client", "")).strip()

    docs = [d for d in docs if len(d["Commentaire_Client"]) > 2]
    client.close()
    print(f"   ✅ {len(docs)} commentaires chargés")
    return docs


# ============================================================
# MÉTHODE 1: EDIT DISTANCE
# ============================================================

def methode_edit_distance(docs, seuil=SEUIL):
    import Levenshtein
    
    supprimes = set()
    for i in range(len(docs)):
        if i in supprimes:
            continue
        t1 = docs[i]["Commentaire_Client"].lower().strip()
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
    
    gardes = [docs[i] for i in range(len(docs)) if i not in supprimes]
    return gardes, supprimes


# ============================================================
# MÉTHODE 2: JACCARD CARACTÈRES
# ============================================================

def jaccard_caracteres(t1, t2):
    t1 = str(t1).lower().strip()
    t2 = str(t2).lower().strip()
    if t1 == t2:
        return 1.0
    set1 = set(t1)
    set2 = set(t2)
    if not set1 or not set2:
        return 0.0
    return len(set1 & set2) / len(set1 | set2)


def methode_jaccard_caracteres(docs, seuil=SEUIL):
    supprimes = set()
    for i in range(len(docs)):
        if i in supprimes:
            continue
        t1 = docs[i]["Commentaire_Client"]
        for j in range(i + 1, len(docs)):
            if j not in supprimes:
                sim = jaccard_caracteres(t1, docs[j]["Commentaire_Client"])
                if sim >= seuil:
                    supprimes.add(j)
    
    gardes = [docs[i] for i in range(len(docs)) if i not in supprimes]
    return gardes, supprimes


# ============================================================
# MÉTHODE 3: JACCARD MOTS
# ============================================================

def jaccard_mots(t1, t2):
    t1 = str(t1).lower().strip()
    t2 = str(t2).lower().strip()
    if t1 == t2:
        return 1.0
    set1 = set(t1.split())
    set2 = set(t2.split())
    if not set1 or not set2:
        return 0.0
    return len(set1 & set2) / len(set1 | set2)


def methode_jaccard_mots(docs, seuil=SEUIL):
    supprimes = set()
    for i in range(len(docs)):
        if i in supprimes:
            continue
        t1 = docs[i]["Commentaire_Client"]
        for j in range(i + 1, len(docs)):
            if j not in supprimes:
                sim = jaccard_mots(t1, docs[j]["Commentaire_Client"])
                if sim >= seuil:
                    supprimes.add(j)
    
    gardes = [docs[i] for i in range(len(docs)) if i not in supprimes]
    return gardes, supprimes


# ============================================================
# MÉTHODE 4: COSINE TF-IDF
# ============================================================

def methode_cosine_tfidf(docs, seuil=SEUIL):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    
    textes = [d["Commentaire_Client"].lower().strip() for d in docs]
    
    vectorizer = TfidfVectorizer(analyzer='word', ngram_range=(1, 2), min_df=1, sublinear_tf=True)
    tfidf_matrix = vectorizer.fit_transform(textes)
    sim_matrix = cosine_similarity(tfidf_matrix)
    
    supprimes = set()
    for i in range(len(docs)):
        if i in supprimes:
            continue
        for j in range(i + 1, len(docs)):
            if j not in supprimes and sim_matrix[i][j] >= seuil:
                supprimes.add(j)
    
    gardes = [docs[i] for i in range(len(docs)) if i not in supprimes]
    return gardes, supprimes


# ============================================================
# MÉTHODE 5: EMBEDDINGS (SBERT) - NOUVEAU !
# ============================================================

def methode_embeddings(docs, seuil=SEUIL):
    """
    Similarité cosinus sur embeddings SBERT (multilingue)
    Comprend le sens en arabe et français !
    """
    try:
        from sentence_transformers import SentenceTransformer
        from sklearn.metrics.pairwise import cosine_similarity
        
        print("   🔄 Chargement du modèle multilingue (première fois = lent)...")
        # Modèle multilingue qui supporte arabe + français
        model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        
        textes = [d["Commentaire_Client"] for d in docs]
        print(f"   🔄 Génération des embeddings pour {len(textes)} textes...")
        embeddings = model.encode(textes, show_progress_bar=True)
        
        print("   🔄 Calcul des similarités...")
        sim_matrix = cosine_similarity(embeddings)
        
        supprimes = set()
        for i in range(len(docs)):
            if i in supprimes:
                continue
            for j in range(i + 1, len(docs)):
                if j not in supprimes and sim_matrix[i][j] >= seuil:
                    supprimes.add(j)
        
        gardes = [docs[i] for i in range(len(docs)) if i not in supprimes]
        return gardes, supprimes
        
    except ImportError:
        print("   ❌ sentence-transformers non installé")
        print("   → pip install sentence-transformers")
        return None, None
    except Exception as e:
        print(f"   ❌ Erreur embeddings: {e}")
        return None, None


# ============================================================
# VÉRITÉ TERRAIN
# ============================================================

def creer_verite_terrain(docs):
    """Vérité terrain approximative (copies exactes)"""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    
    textes = [d["Commentaire_Client"].lower().strip() for d in docs]
    vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(3, 5))
    tfidf = vectorizer.fit_transform(textes)
    sim = cosine_similarity(tfidf)
    
    verite = {i: {"est_unique": True} for i in range(len(docs))}
    
    for i in range(len(docs)):
        if not verite[i]["est_unique"]:
            continue
        for j in range(i+1, len(docs)):
            if sim[i][j] >= 0.995:
                verite[j]["est_unique"] = False
    
    return verite


def calculer_metriques(supprimes, verite, total):
    tp = sum(1 for i in range(total) if i in supprimes and not verite[i]["est_unique"])
    fp = sum(1 for i in range(total) if i in supprimes and verite[i]["est_unique"])
    fn = sum(1 for i in range(total) if i not in supprimes and not verite[i]["est_unique"])
    tn = sum(1 for i in range(total) if i not in supprimes and verite[i]["est_unique"])
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    rappel = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * rappel) / (precision + rappel) if (precision + rappel) > 0 else 0
    
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": precision, "rappel": rappel, "f1": f1}


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 80)
    print("🔬 COMPARAISON DE 6 MÉTHODES (avec EMBEDDINGS SBERT)")
    print("=" * 80)
    
    docs = charger_commentaires()
    nb_total = len(docs)
    
    # Vérité terrain
    verite = creer_verite_terrain(docs)
    vrais_doublons = sum(1 for v in verite.values() if not v["est_unique"])
    print(f"\n📊 {vrais_doublons} vrais doublons dans les {nb_total} commentaires")
    
    resultats = {}
    
    # Liste des méthodes
    methodes = [
        ("Edit Distance", methode_edit_distance),
        ("Jaccard Caractères", methode_jaccard_caracteres),
        ("Jaccard Mots", methode_jaccard_mots),
        ("Cosine TF-IDF", methode_cosine_tfidf),
        ("Embeddings SBERT", methode_embeddings),
    ]
    
    for nom, methode in methodes:
        print(f"\n⏳ {nom}...")
        try:
            t0 = time.time()
            gardes, supprimes = methode(docs)
            temps = time.time() - t0
            
            if gardes is None:
                print(f"   ❌ Non disponible")
                continue
            
            metriques = calculer_metriques(supprimes, verite, nb_total)
            resultats[nom] = {"supprimes": supprimes, "temps": temps, "metriques": metriques}
            
            print(f"   ✅ Terminé en {temps:.2f}s")
            print(f"      → {len(supprimes)} supprimés | {len(gardes)} gardés")
            print(f"      → Précision: {metriques['precision']:.2%} | Rappel: {metriques['rappel']:.2%} | F1: {metriques['f1']:.2%}")
            
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
    
    # Tableau final
    print(f"\n{'='*80}")
    print("📊 TABLEAU COMPARATIF FINAL")
    print(f"{'='*80}")
    print(f"{'Méthode':<22} {'Supprimés':>10} {'Précision':>12} {'Rappel':>10} {'F1-Score':>10} {'Temps':>8}")
    print("-" * 75)
    
    for nom, res in resultats.items():
        m = res["metriques"]
        print(f"{nom:<22} {len(res['supprimes']):>10} {m['precision']:>11.2%} {m['rappel']:>9.2%} {m['f1']:>9.2%} {res['temps']:>7.2f}s")
    
    # Conclusion
    print(f"\n{'='*80}")
    print("💡 CONCLUSION AVEC EMBEDDINGS")
    print(f"{'='*80}")
    
    # Trouver la meilleure méthode (meilleur F1)
    meilleure = max(resultats.items(), key=lambda x: x[1]["metriques"]["f1"])
    
    print(f"""
   🏆 CLASSEMENT (par F1-Score) :
   
   1. {meilleure[0]} → F1 = {meilleure[1]['metriques']['f1']:.2%}
   
   📊 Analyse Embeddings :
   - Comprend le SÉMANTIQUE (pas juste les mots)
   - Excellent pour arabe/français
   - Détecte les PARAPHRASES
   - Mais plus lent et gourmand en mémoire
   
   👉 RECOMMANDATION FINALE :
   - Production (rapide) : Jaccard Mots ou Cosine TF-IDF
   - Recherche (précis) : Embeddings SBERT
    """)


if __name__ == "__main__":
    main()