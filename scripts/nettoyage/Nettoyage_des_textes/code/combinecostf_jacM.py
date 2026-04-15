#!/usr/bin/env python3
# combinaison_jaccard_tfidf.py
# Test de la combinaison Jaccard Mots + Cosine TF-IDF

import time
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
import os

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI        = "mongodb://localhost:27018/"
DB_NAME          = "telecom_algerie"
COLLECTION       = "commentaires_sans_urls_arobase"
NB_DOCS          = 1000
SEUIL            = 0.85

EXPORT_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ============================================================
# 1. CHARGEMENT
# ============================================================

def charger_commentaires():
    print("📥 Connexion MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION]

    docs = list(collection.find(
        {},
        {"_id": 1, "Commentaire_Client": 1, "source": 1}
    ).limit(NB_DOCS))

    for i, doc in enumerate(docs):
        doc["index"] = i
        doc["Commentaire_Client"] = str(doc.get("Commentaire_Client", "")).strip()

    docs = [d for d in docs if len(d["Commentaire_Client"]) > 2]
    client.close()
    print(f"   ✅ {len(docs)} commentaires chargés")
    return docs


# ============================================================
# 2. MÉTHODES INDIVIDUELLES
# ============================================================

def jaccard_mots(texte1, texte2):
    """Jaccard sur les mots - F1=94.74%"""
    if not texte1 or not texte2:
        return 0
    t1 = str(texte1).lower().strip()
    t2 = str(texte2).lower().strip()
    if t1 == t2:
        return 100
    set1 = set(t1.split())
    set2 = set(t2.split())
    if not set1 or not set2:
        return 0
    return len(set1 & set2) / len(set1 | set2) * 100


def cosine_tfidf(texte1, texte2):
    """Cosine TF-IDF - F1=92.31%"""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    
    vectorizer = TfidfVectorizer(analyzer='word', ngram_range=(1, 2), min_df=1, sublinear_tf=True)
    try:
        tfidf = vectorizer.fit_transform([texte1, texte2])
        return cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0] * 100
    except:
        return 0


# ============================================================
# 3. MODES DE COMBINAISON (sans Edit Distance)
# ============================================================

def mode_et(score_jaccard, score_tfidf, seuil=SEUIL):
    """Mode ET: les DEUX doivent être d'accord"""
    return (score_jaccard >= seuil and score_tfidf >= seuil)


def mode_ou(score_jaccard, score_tfidf, seuil=SEUIL):
    """Mode OU: une seule suffit"""
    return (score_jaccard >= seuil or score_tfidf >= seuil)


def mode_jaccard_confirme(score_jaccard, score_tfidf, seuil=SEUIL):
    """Mode JACCARD CONFIRMÉ par TF-IDF"""
    if score_jaccard >= 95:  # Jaccard très haut
        return True
    if score_jaccard >= seuil:
        return score_tfidf >= seuil
    return False


def mode_tfidf_confirme(score_jaccard, score_tfidf, seuil=SEUIL):
    """Mode TF-IDF CONFIRMÉ par Jaccard"""
    if score_tfidf >= 95:
        return True
    if score_tfidf >= seuil:
        return score_jaccard >= seuil
    return False


def mode_moyenne(score_jaccard, score_tfidf, seuil=SEUIL):
    """Mode MOYENNE: moyenne des deux scores"""
    moyenne = (score_jaccard + score_tfidf) / 2
    return moyenne >= seuil


def mode_pondere(score_jaccard, score_tfidf, seuil=SEUIL):
    """Mode PONDÉRÉ: Jaccard a plus de poids (0.6 vs 0.4)"""
    score_final = score_jaccard * 0.6 + score_tfidf * 0.4
    return score_final >= seuil


# ============================================================
# 4. DÉDUPLICATION AVEC MODE
# ============================================================

def deduplication_combinee(docs, mode_func, seuil=SEUIL):
    """Déduplication avec la combinaison Jaccard + TF-IDF"""
    supprimes = set()
    comparaisons = []
    
    for i in range(len(docs)):
        if i in supprimes:
            continue
        t1 = docs[i]["Commentaire_Client"]
        for j in range(i + 1, len(docs)):
            if j not in supprimes:
                score_jaccard = jaccard_mots(t1, docs[j]["Commentaire_Client"])
                score_tfidf = cosine_tfidf(t1, docs[j]["Commentaire_Client"])
                
                est_doublon = mode_func(score_jaccard, score_tfidf, seuil)
                
                comparaisons.append({
                    "idx1": i, "idx2": j,
                    "jaccard": round(score_jaccard, 2),
                    "tfidf": round(score_tfidf, 2),
                    "est_doublon": est_doublon
                })
                
                if est_doublon:
                    supprimes.add(j)
    
    gardes = [docs[i] for i in range(len(docs)) if i not in supprimes]
    return gardes, supprimes, comparaisons


# ============================================================
# 5. VÉRITÉ TERRAIN ET MÉTRIQUES
# ============================================================

def creer_verite_terrain(docs):
    print("\n🔍 Création vérité terrain...")
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
    
    nb_doublons = sum(1 for v in verite.values() if not v["est_unique"])
    print(f"   ✅ {nb_doublons} vrais doublons identifiés")
    return verite


def calculer_metriques(supprimes, verite, total):
    if supprimes is None:
        return {"tp": 0, "fp": 0, "fn": 0, "tn": 0, "precision": 0, "rappel": 0, "f1": 0}
    
    tp = sum(1 for i in range(total) if i in supprimes and not verite[i]["est_unique"])
    fp = sum(1 for i in range(total) if i in supprimes and verite[i]["est_unique"])
    fn = sum(1 for i in range(total) if i not in supprimes and not verite[i]["est_unique"])
    tn = sum(1 for i in range(total) if i not in supprimes and verite[i]["est_unique"])
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    rappel = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * rappel) / (precision + rappel) if (precision + rappel) > 0 else 0
    
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, 
            "precision": precision, "rappel": rappel, "f1": f1}


# ============================================================
# 6. EXPORT
# ============================================================

def exporter_resultats_csv(resultats, filename="resultats_jaccard_tfidf.csv"):
    rows = []
    for mode, res in resultats.items():
        if res["gardes"] is not None:
            rows.append({
                "Mode": mode,
                "Temps (s)": round(res["temps"], 2),
                "Gardés": len(res["gardes"]),
                "Supprimés": len(res["supprimes"]),
                "Taux (%)": round(len(res["supprimes"]) / res["total"] * 100, 2),
                "Précision": f"{res['metriques']['precision']:.2%}",
                "Rappel": f"{res['metriques']['rappel']:.2%}",
                "F1-Score": f"{res['metriques']['f1']:.2%}",
                "TP": res['metriques']['tp'],
                "FP": res['metriques']['fp']
            })
    
    if rows:
        df = pd.DataFrame(rows)
        filepath = os.path.join(EXPORT_DIR, filename)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"💾 Export CSV: {filepath}")


# ============================================================
# 7. MAIN
# ============================================================

def main():
    print("=" * 80)
    print("🔬 COMBINAISON: JACCARD MOTS + COSINE TF-IDF")
    print(f"   Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   (Sans Edit Distance - trop permissive)")
    print("=" * 80)
    
    # Charger
    docs = charger_commentaires()
    nb_total = len(docs)
    print(f"\n📊 {nb_total} commentaires analysés")
    
    # Vérité terrain
    verite = creer_verite_terrain(docs)
    
    # Modes à tester
    modes = {
        "🔴 MODE ET (Jaccard ET TF-IDF)": mode_et,
        "🟢 MODE OU (Jaccard OU TF-IDF)": mode_ou,
        "⭐ MODE JACCARD CONFIRMÉ (recommandé)": mode_jaccard_confirme,
        "🟡 MODE TF-IDF CONFIRMÉ": mode_tfidf_confirme,
        "🔵 MODE MOYENNE": mode_moyenne,
        "🟣 MODE PONDÉRÉ (Jaccard 60%)": mode_pondere,
    }
    
    resultats = {}
    
    # Tester chaque mode
    for nom_mode, fonction_mode in modes.items():
        print(f"\n⏳ {nom_mode}...")
        try:
            t0 = time.time()
            gardes, supprimes, comparaisons = deduplication_combinee(docs, fonction_mode)
            temps = time.time() - t0
            
            metriques = calculer_metriques(supprimes, verite, nb_total)
            
            resultats[nom_mode] = {
                "gardes": gardes,
                "supprimes": supprimes,
                "temps": temps,
                "metriques": metriques,
                "total": nb_total
            }
            
            print(f"   ✅ Terminé en {temps:.2f}s")
            print(f"      → {len(supprimes)} doublons trouvés ({len(supprimes)/nb_total*100:.1f}%)")
            print(f"      → Précision: {metriques['precision']:.2%} | Rappel: {metriques['rappel']:.2%} | F1: {metriques['f1']:.2%}")
            print(f"      → Vrais doublons: {metriques['tp']} | Faux positifs: {metriques['fp']}")
            
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
            resultats[nom_mode] = {"gardes": None, "supprimes": None, "temps": None, "total": nb_total}
    
    # Tableau récapitulatif
    print(f"\n{'='*80}")
    print("📊 TABLEAU COMPARATIF DES MODES")
    print(f"{'='*80}")
    print(f"{'Mode':<35} {'Supprimés':>10} {'Précision':>12} {'Rappel':>10} {'F1-Score':>10} {'Temps':>8} {'FP':>6}")
    print("-" * 95)
    
    for nom, res in resultats.items():
        if res["gardes"] is not None:
            nb_s = len(res["supprimes"])
            m = res["metriques"]
            print(f"{nom:<35} {nb_s:>10} {m['precision']:>11.2%} {m['rappel']:>9.2%} {m['f1']:>9.2%} {res['temps']:>7.2f}s {m['fp']:>6}")
    
    # Exporter
    exporter_resultats_csv(resultats)
    
    # Conclusion
    print(f"\n{'='*80}")
    print("💡 CONCLUSION")
    print(f"{'='*80}")
    
    meilleur = max([(nom, res) for nom, res in resultats.items() if res["gardes"] is not None], 
                   key=lambda x: x[1]["metriques"]["f1"])
    
    # Résultats individuels de référence
    print(f"""
   📊 RÉSULTATS INDIVIDUELS (d'après vos tests précédents) :
   ┌─────────────────────────────────────────────────────────────┐
   │ Jaccard Mots seul    : F1=94.74% | FP=4  | Temps=2.32s     │
   │ Cosine TF-IDF seul   : F1=92.31% | FP=6  | Temps=0.18s     │
   └─────────────────────────────────────────────────────────────┘

   🏆 MEILLEUR MODE COMBINÉ: {meilleur[0]}
      → F1-Score: {meilleur[1]['metriques']['f1']:.2%}
      → Précision: {meilleur[1]['metriques']['precision']:.2%}
      → Faux positifs: {meilleur[1]['metriques']['fp']}
      → Temps: {meilleur[1]['temps']:.2f}s

   📈 COMPARAISON AVEC JACCARD SEUL :
   ┌─────────────────────────────────────────────────────────────┐
   │ Jaccard seul    : F1=94.74% | FP=4  | Temps=2.32s          │
   │ {meilleur[0][:25]:<25}: F1={meilleur[1]['metriques']['f1']:.2%} | FP={meilleur[1]['metriques']['fp']} | Temps={meilleur[1]['temps']:.2f}s │
   └─────────────────────────────────────────────────────────────┘

   ✅ RECOMMANDATION FINALE :
   
   Si la combinaison donne FP < 4 et F1 > 94.74% → Prenez la combinaison
   Sinon → Gardez JACCARD MOTS SEUL (déjà excellent)

   📁 Fichier généré: {EXPORT_DIR}/resultats_jaccard_tfidf.csv
    """)
    
    print("=" * 80)


if __name__ == "__main__":
    main()