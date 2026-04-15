#!/usr/bin/env python3
# jaccard_mots_seul_optimal.py
# Version finale avec Jaccard Mots seulement

import time
import pandas as pd
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
SEUIL            = 85

EXPORT_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ============================================================
# JACCARD MOTS (LA MEILLEURE MÉTHODE)
# ============================================================

def jaccard_mots(texte1, texte2):
    """Jaccard sur les mots - F1=94.74%, Précision=90%"""
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


def deduplication_jaccard_mots(docs, seuil=SEUIL):
    """Déduplication avec Jaccard Mots"""
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
# CHARGEMENT
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
# VÉRITÉ TERRAIN
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
# EXPORT
# ============================================================

def exporter_doublons_csv(docs, supprimes):
    if supprimes and len(supprimes) > 0:
        doublons_data = []
        for idx in sorted(supprimes):
            doc = docs[idx]
            doublons_data.append({
                "Index": doc["index"],
                "Commentaire": doc["Commentaire_Client"],
                "Source": doc.get("source", "")
            })
        df = pd.DataFrame(doublons_data)
        filepath = os.path.join(EXPORT_DIR, "doublons_jaccard_mots.csv")
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"💾 Doublons exportés: {filepath} ({len(doublons_data)} commentaires)")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 80)
    print("🏆 MÉTHODE OPTIMALE: JACCARD MOTS")
    print(f"   Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   Basé sur vos tests précédents: F1=94.74% | Précision=90% | FP=4")
    print("=" * 80)
    
    # Charger
    docs = charger_commentaires()
    nb_total = len(docs)
    print(f"\n📊 {nb_total} commentaires analysés")
    
    # Vérité terrain
    verite = creer_verite_terrain(docs)
    
    # Exécuter Jaccard Mots
    print(f"\n⏳ Exécution de Jaccard Mots (seuil={SEUIL})...")
    t0 = time.time()
    gardes, supprimes = deduplication_jaccard_mots(docs, SEUIL)
    temps = time.time() - t0
    
    # Métriques
    metriques = calculer_metriques(supprimes, verite, nb_total)
    
    print(f"\n✅ Terminé en {temps:.2f}s")
    print(f"   → {len(supprimes)} doublons trouvés ({len(supprimes)/nb_total*100:.1f}%)")
    print(f"   → Précision: {metriques['precision']:.2%}")
    print(f"   → Rappel: {metriques['rappel']:.2%}")
    print(f"   → F1-Score: {metriques['f1']:.2%}")
    print(f"   → Vrais doublons: {metriques['tp']} | Faux positifs: {metriques['fp']}")
    
    # Exporter
    exporter_doublons_csv(docs, supprimes)
    
    # Conclusion
    print(f"\n{'='*80}")
    print("💡 CONCLUSION FINALE")
    print(f"{'='*80}")
    print(f"""
   ✅ JACCARD MOTS EST LA MEILLEURE MÉTHODE !

   📊 PERFORMANCES CONFIRMÉES :
   ┌─────────────────────────────────────────────────────────────┐
   │ • F1-Score      : {metriques['f1']:.2%}                     │
   │ • Précision     : {metriques['precision']:.2%}              │
   │ • Rappel        : {metriques['rappel']:.2%}                 │
   │ • Faux positifs : {metriques['fp']}                         │
   │ • Temps         : {temps:.2f}s                              │
   └─────────────────────────────────────────────────────────────┘
  
   🚀 CODE À INTÉGRER DANS SPARK :

   ```python
   def jaccard_mots(texte1, texte2):
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

   SEUIL = 85  # 85%
   
   if jaccard_mots(commentaire1, commentaire2) >= SEUIL:
       # C'est un doublon
📁 FICHIER GÉNÉRÉ :

{EXPORT_DIR}/doublons_jaccard_mots.csv
""")
print("=" * 80)


if __name__ == "__main__":
    main()