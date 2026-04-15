#!/usr/bin/env python3
# comparaison_complete_6_methodes.py
# Compare 6 méthodes sur 1000 commentaires réels avec sauvegarde complète

# pip install python-Levenshtein scikit-learn pymongo pandas numpy matplotlib seaborn

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
COLLECTION_RESULTS = "resultats_6_methodes"
COLLECTION_COMMENTS = "commentaires_analyse_1000"
NB_DOCS          = 1000
SEUIL            = 0.85

# Dossier pour les exports
EXPORT_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ============================================================
# 1. CHARGEMENT DES COMMENTAIRES
# ============================================================

def charger_commentaires():
    """Charge 1000 commentaires et les sauvegarde"""
    print("📥 Connexion MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION]

    docs = list(collection.find(
        {},
        {"_id": 1, "Commentaire_Client": 1, "source": 1, "moderateur": 1, "date": 1}
    ).limit(NB_DOCS))

    # Nettoyage et ajout d'index
    for i, doc in enumerate(docs):
        doc["index"] = i
        doc["_id_str"] = str(doc["_id"])
        doc["Commentaire_Client"] = str(doc.get("Commentaire_Client", "")).strip()
        doc["source"] = str(doc.get("source", "inconnu"))
        doc["moderateur"] = str(doc.get("moderateur", "inconnu"))
        doc["date"] = str(doc.get("date", datetime.now()))

    # Filtrer les vides
    docs = [d for d in docs if len(d["Commentaire_Client"]) > 2]
    
    # Sauvegarder les commentaires dans MongoDB
    commentaires_collection = db[COLLECTION_COMMENTS]
    commentaires_collection.delete_many({})
    if docs:
        commentaires_collection.insert_many(docs)
    
    client.close()
    print(f"   ✅ {len(docs)} commentaires chargés et sauvegardés dans {COLLECTION_COMMENTS}")
    return docs


# ============================================================
# 2. MÉTHODE 1: EDIT DISTANCE (Levenshtein)
# ============================================================

def levenshtein_distance(s1, s2):
    """Distance de Levenshtein manuelle (fallback si python-Levenshtein non installé)"""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    
    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def methode_edit_distance(docs, seuil=SEUIL):
    """Edit Distance / Levenshtein"""
    try:
        import Levenshtein
        has_lib = True
    except ImportError:
        has_lib = False
        print("   ⚠️ python-Levenshtein non installé, utilisation version manuelle (plus lente)")
    
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
                    if has_lib:
                        dist = Levenshtein.distance(t1, t2)
                    else:
                        dist = levenshtein_distance(t1, t2)
                    sim = 1 - dist / max(len(t1), len(t2), 1)
                
                if sim >= seuil:
                    supprimes.add(j)
    
    gardes = [docs[i] for i in range(len(docs)) if i not in supprimes]
    return gardes, supprimes


# ============================================================
# 3. MÉTHODE 2: JACCARD CARACTÈRES
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
# 4. MÉTHODE 3: JACCARD MOTS
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
# 5. MÉTHODE 4: COSINE TF-IDF
# ============================================================

def methode_cosine_tfidf(docs, seuil=SEUIL):
    """Cosine TF-IDF - méthode standard avec cosine_similarity"""
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
# 6. MÉTHODE 5: TF-IDF SEUL (version manuelle)
# ============================================================

def methode_tfidf_seul(docs, seuil=SEUIL):
    """TF-IDF seul - version manuelle (plus lente mais pédagogique)"""
    from sklearn.feature_extraction.text import TfidfVectorizer
    import numpy as np
    
    textes = [d["Commentaire_Client"].lower().strip() for d in docs]
    
    vectorizer = TfidfVectorizer(analyzer='word', ngram_range=(1, 2), min_df=1)
    tfidf_matrix = vectorizer.fit_transform(textes)
    
    supprimes = set()
    
    for i in range(len(docs)):
        if i in supprimes:
            continue
        for j in range(i + 1, len(docs)):
            if j not in supprimes:
                vecteur_i = tfidf_matrix[i].toarray().flatten()
                vecteur_j = tfidf_matrix[j].toarray().flatten()
                
                norm_i = np.linalg.norm(vecteur_i)
                norm_j = np.linalg.norm(vecteur_j)
                
                if norm_i == 0 or norm_j == 0:
                    sim = 0.0
                else:
                    sim = np.dot(vecteur_i, vecteur_j) / (norm_i * norm_j)
                
                if sim >= seuil:
                    supprimes.add(j)
    
    gardes = [docs[i] for i in range(len(docs)) if i not in supprimes]
    return gardes, supprimes


# ============================================================
# 7. MÉTHODE 6: EMBEDDINGS (Sentence-BERT) - Optionnel
# ============================================================

def methode_embeddings(docs, seuil=SEUIL):
    """Similarité cosinus sur embeddings SBERT (optionnel)"""
    try:
        from sentence_transformers import SentenceTransformer
        from sklearn.metrics.pairwise import cosine_similarity
        
        print("   🔄 Chargement du modèle SBERT (première fois = lent)...")
        model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        
        textes = [d["Commentaire_Client"].strip() for d in docs]
        embeddings = model.encode(textes, show_progress_bar=False)
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
        print("   ⚠️ sentence-transformers non installé, méthode ignorée")
        return None, None
    except Exception as e:
        print(f"   ❌ Erreur embeddings: {e}")
        return None, None


# ============================================================
# 8. MATRICE DE CONFUSION
# ============================================================

def creer_verite_terrain_manuel(docs):
    """Crée une vérité terrain approximative"""
    print("\n🔍 Création vérité terrain (basée sur similarité très haute)...")
    
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    
    textes = [d["Commentaire_Client"].lower().strip() for d in docs]
    vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(3, 5))
    tfidf = vectorizer.fit_transform(textes)
    sim = cosine_similarity(tfidf)
    
    verite = {i: {"est_unique": True, "doublon_de": None} for i in range(len(docs))}
    
    # Seuil très haut pour être sûr (99.5%)
    for i in range(len(docs)):
        if not verite[i]["est_unique"]:
            continue
        for j in range(i+1, len(docs)):
            if sim[i][j] >= 0.995:
                verite[j]["est_unique"] = False
                verite[j]["doublon_de"] = i
    
    nb_doublons = sum(1 for v in verite.values() if not v["est_unique"])
    print(f"   ✅ {nb_doublons} vrais doublons identifiés")
    return verite


def calculer_metriques(supprimes, verite_terrain, total_docs):
    """Calcule TP, FP, FN, TN, précision, rappel, F1"""
    if supprimes is None:
        return {"tp": 0, "fp": 0, "fn": 0, "tn": 0, "precision": 0, "rappel": 0, "f1": 0}
    
    y_true = []
    y_pred = []
    
    for i in range(total_docs):
        vrai = not verite_terrain[i]["est_unique"] if i in verite_terrain else False
        pred = i in supprimes
        y_true.append(vrai)
        y_pred.append(pred)
    
    tp = sum(1 for t, p in zip(y_true, y_pred) if t and p)
    fp = sum(1 for t, p in zip(y_true, y_pred) if not t and p)
    fn = sum(1 for t, p in zip(y_true, y_pred) if t and not p)
    tn = sum(1 for t, p in zip(y_true, y_pred) if not t and not p)
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    rappel = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * rappel) / (precision + rappel) if (precision + rappel) > 0 else 0
    
    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": precision, "rappel": rappel, "f1": f1
    }


# ============================================================
# 9. EXPORT CSV DES DOUBLONS PAR MÉTHODE
# ============================================================

def exporter_doublons_csv(docs, supprimes, methode_nom):
    """Exporte les commentaires doublons dans un CSV"""
    if supprimes is None or len(supprimes) == 0:
        print(f"   ⚠️ Aucun doublon à exporter pour {methode_nom}")
        return
    
    # Récupérer les commentaires doublons
    doublons_data = []
    for idx in sorted(supprimes):
        doc = docs[idx]
        doublons_data.append({
            "Index": doc["index"],
            "Commentaire": doc["Commentaire_Client"],
            "Source": doc["source"],
            "Moderateur": doc["moderateur"],
            "Date": doc.get("date", "")
        })
    
    # Créer DataFrame et exporter
    df = pd.DataFrame(doublons_data)
    filename = f"doublons_{methode_nom.replace(' ', '_').replace('.', '')}.csv"
    filepath = os.path.join(EXPORT_DIR, filename)
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"   💾 Doublons exportés: {filepath} ({len(doublons_data)} commentaires)")


# ============================================================
# 10. VÉRIFICATION ET EXPORT DE LA VÉRITÉ TERRAIN
# ============================================================

def verifier_et_exporter_verite_terrain(docs, verite_terrain):
    """
    Vérifie manuellement la vérité terrain et exporte les doublons identifiés
    """
    print("\n" + "=" * 80)
    print("🔍 VÉRIFICATION DE LA VÉRITÉ TERRAIN")
    print("=" * 80)
    
    # 1. Collecter tous les doublons identifiés
    vrais_doublons = []
    deja_vus = set()
    
    for i, doc in enumerate(docs):
        if not verite_terrain[i]["est_unique"]:
            doublon_de = verite_terrain[i].get("doublon_de")
            if doublon_de is not None and doublon_de not in deja_vus:
                # Trouver la paire de doublons
                paire = {
                    "original_index": doublon_de,
                    "original_texte": docs[doublon_de]["Commentaire_Client"],
                    "doublon_index": i,
                    "doublon_texte": docs[i]["Commentaire_Client"],
                    "source_original": docs[doublon_de].get("source", "inconnu"),
                    "source_doublon": docs[i].get("source", "inconnu")
                }
                vrais_doublons.append(paire)
                deja_vus.add(doublon_de)
                deja_vus.add(i)
    
    # 2. Afficher le résumé
    print(f"\n📊 RÉSUMÉ DE LA VÉRITÉ TERRAIN :")
    print(f"   → {len(vrais_doublons)} paires de doublons identifiées")
    print(f"   → {len(deja_vus)} commentaires concernés (sur {len(docs)})")
    
    # 3. Exporter en CSV pour vérification manuelle
    if vrais_doublons:
        df_verite = pd.DataFrame(vrais_doublons)
        filepath = os.path.join(EXPORT_DIR, "verite_terrain_doublons.csv")
        df_verite.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"\n💾 Doublons vérité terrain exportés : {filepath}")
        
        # 4. Afficher les premiers exemples pour vérification visuelle
        print(f"\n📋 PREMIERS EXEMPLES DE DOUBLONS IDENTIFIÉS (à vérifier manuellement) :")
        print("-" * 80)
        for i, paire in enumerate(vrais_doublons[:10]):
            print(f"\n🔹 Doublon #{i+1} :")
            print(f"   Original ({paire['original_index']}) : {paire['original_texte'][:100]}")
            print(f"   Doublon  ({paire['doublon_index']}) : {paire['doublon_texte'][:100]}")
            print(f"   Sources : {paire['source_original']} → {paire['source_doublon']}")
            
            # Calculer la similarité pour vérification
            sim_caracteres = jaccard_caracteres(paire['original_texte'], paire['doublon_texte'])
            sim_mots = jaccard_mots(paire['original_texte'], paire['doublon_texte'])
            print(f"   Similarité : Caractères={sim_caracteres:.1%} | Mots={sim_mots:.1%}")
        
        # 5. Générer un rapport HTML pour meilleure visualisation
        generer_rapport_verification_html(vrais_doublons, docs)
        
    else:
        print("\n⚠️ Aucun doublon trouvé dans la vérité terrain !")
    
    return vrais_doublons


def generer_rapport_verification_html(vrais_doublons, docs, filename="verification_verite_terrain.html"):
    """Génère un rapport HTML pour visualiser les doublons"""
    
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Vérification Vérité Terrain - Doublons Identifiés</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            h1 { color: #333; }
            .summary { background: #4CAF50; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
            .doublon-card { background: white; border: 1px solid #ddd; border-radius: 8px; margin: 15px 0; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .original { border-left: 5px solid #4CAF50; padding-left: 15px; margin: 10px 0; }
            .doublon { border-left: 5px solid #FF9800; padding-left: 15px; margin: 10px 0; }
            .similarite { background: #f0f0f0; padding: 8px; border-radius: 4px; font-family: monospace; margin-top: 10px; }
            .badge { display: inline-block; padding: 3px 8px; border-radius: 3px; font-size: 12px; margin: 2px; }
            .badge-source { background: #2196F3; color: white; }
            .footer { margin-top: 30px; text-align: center; color: #666; font-size: 12px; }
        </style>
    </head>
    <body>
        <h1>🔍 Vérification de la Vérité Terrain</h1>
    """
    
    html_content += f"""
        <div class="summary">
            <h2>📊 Résumé</h2>
            <p>✅ {len(vrais_doublons)} paires de doublons identifiées</p>
            <p>📝 Vérifiez manuellement que chaque paire est bien un vrai doublon</p>
            <p>🎯 Seuil utilisé : 99.5% de similarité caractères</p>
        </div>
    """
    
    for i, paire in enumerate(vrais_doublons):
        html_content += f"""
        <div class="doublon-card">
            <h3>🔹 Doublon #{i+1}</h3>
            <div class="original">
                <strong>📄 ORIGINAL (Index {paire['original_index']}) :</strong><br>
                {paire['original_texte']}<br>
                <span class="badge badge-source">Source: {paire['source_original']}</span>
            </div>
            <div class="doublon">
                <strong>🔄 DOUBLON (Index {paire['doublon_index']}) :</strong><br>
                {paire['doublon_texte']}<br>
                <span class="badge badge-source">Source: {paire['source_doublon']}</span>
            </div>
            <div class="similarite">
                📊 Similarité: {paire.get('similarite', 'N/A')}
            </div>
        </div>
        """
    
    html_content += """
        <div class="footer">
            <p>Généré automatiquement par comparaison_complete_6_methodes.py</p>
            <p>✅ Vérifiez manuellement que tous ces doublons sont corrects</p>
        </div>
    </body>
    </html>
    """
    
    filepath = os.path.join(EXPORT_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"💾 Rapport HTML généré : {filepath}")


def verifier_doublon_manuellement(docs, idx1, idx2):
    """Fonction interactive pour vérifier manuellement un doublon"""
    print("\n" + "=" * 60)
    print("🔍 VÉRIFICATION MANUELLE DE DOUBLON")
    print("=" * 60)
    print(f"\n📄 TEXTE 1 (Index {idx1}):")
    print(f"   {docs[idx1]['Commentaire_Client']}")
    print(f"\n📄 TEXTE 2 (Index {idx2}):")
    print(f"   {docs[idx2]['Commentaire_Client']}")
    
    sim_car = jaccard_caracteres(docs[idx1]['Commentaire_Client'], docs[idx2]['Commentaire_Client'])
    sim_mots = jaccard_mots(docs[idx1]['Commentaire_Client'], docs[idx2]['Commentaire_Client'])
    
    print(f"\n📊 Similarités calculées :")
    print(f"   Jaccard Caractères : {sim_car:.1%}")
    print(f"   Jaccard Mots : {sim_mots:.1%}")
    
    reponse = input("\n✅ Est-ce un vrai doublon ? (o/n) : ")
    return reponse.lower() == 'o'


# ============================================================
# 11. SAUVEGARDES
# ============================================================

def sauvegarder_dans_mongodb(resultats, docs, verite_terrain):
    """Sauvegarde tout dans MongoDB"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_RESULTS]
    
    document = {
        "date": datetime.now(),
        "nb_documents": len(docs),
        "seuil": SEUIL,
        "methodes": [],
        "commentaires": [{"index": d["index"], "texte": d["Commentaire_Client"], "source": d["source"]} for d in docs]
    }
    
    for nom, res in resultats.items():
        if res["gardes"] is not None:
            methode_data = {
                "nom": nom,
                "temps": res["temps"],
                "nb_gardes": len(res["gardes"]),
                "nb_supprimes": len(res["supprimes"]),
                "taux_reduction": len(res["supprimes"]) / len(docs) * 100,
                "supprimes_indices": list(res["supprimes"])
            }
            
            if "metriques" in res:
                methode_data["metriques"] = res["metriques"]
            
            document["methodes"].append(methode_data)
    
    collection.delete_many({})
    if document["methodes"]:
        collection.insert_one(document)
    
    print(f"\n💾 Résultats sauvegardés dans {DB_NAME}.{COLLECTION_RESULTS}")
    client.close()


def exporter_json(resultats, docs, filename="resultats_6_methodes.json"):
    """Export JSON complet"""
    export_data = {
        "date": datetime.now().isoformat(),
        "nb_documents": len(docs),
        "seuil": SEUIL,
        "commentaires": [{"index": d["index"], "texte": d["Commentaire_Client"]} for d in docs[:100]],
        "methodes": {}
    }
    
    for nom, res in resultats.items():
        if res["gardes"] is not None:
            export_data["methodes"][nom] = {
                "temps": res["temps"],
                "nb_gardes": len(res["gardes"]),
                "nb_supprimes": len(res["supprimes"]),
                "taux_reduction": len(res["supprimes"]) / len(docs) * 100
            }
            if "metriques" in res:
                export_data["methodes"][nom]["metriques"] = res["metriques"]
    
    filepath = os.path.join(EXPORT_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
    print(f"💾 Export JSON: {filepath}")


def exporter_excel_complet(resultats, docs, filename="resultats_6_methodes.xlsx"):
    """Export Excel avec plusieurs onglets"""
    filepath = os.path.join(EXPORT_DIR, filename)
    
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        # Onglet 1: Résumé
        rows = []
        for nom, res in resultats.items():
            if res["gardes"] is not None:
                rows.append({
                    "Méthode": nom,
                    "Temps (s)": round(res["temps"], 2),
                    "Gardés": len(res["gardes"]),
                    "Supprimés": len(res["supprimes"]),
                    "Taux (%)": round(len(res["supprimes"]) / len(docs) * 100, 2)
                })
        if rows:
            pd.DataFrame(rows).to_excel(writer, sheet_name="Resume", index=False)
        
        # Onglet 2: Métriques
        rows = []
        for nom, res in resultats.items():
            if res["gardes"] is not None and "metriques" in res:
                m = res["metriques"]
                rows.append({
                    "Méthode": nom,
                    "TP": m["tp"], "FP": m["fp"], "FN": m["fn"], "TN": m["tn"],
                    "Précision": f"{m['precision']:.2%}",
                    "Rappel": f"{m['rappel']:.2%}",
                    "F1-Score": f"{m['f1']:.2%}"
                })
        if rows:
            pd.DataFrame(rows).to_excel(writer, sheet_name="Matrices", index=False)
        
        # Onglet 3: Tous les commentaires
        comments_df = pd.DataFrame([{
            "Index": d["index"],
            "Commentaire": d["Commentaire_Client"][:200],
            "Source": d["source"]
        } for d in docs])
        comments_df.to_excel(writer, sheet_name="Commentaires", index=False)
    
    print(f"💾 Export Excel: {filepath}")


def visualiser(resultats, docs):
    """Crée des graphiques"""
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        sns.set_style("whitegrid")
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        noms = []
        temps = []
        reductions = []
        f1_scores = []
        
        for nom, res in resultats.items():
            if res["gardes"] is not None:
                noms.append(nom)
                temps.append(res["temps"])
                reductions.append(len(res["supprimes"]) / len(docs) * 100)
                if "metriques" in res:
                    f1_scores.append(res["metriques"]["f1"])
        
        if not noms:
            print("   ⚠️ Aucune donnée à visualiser")
            return
        
        # Graphique temps
        axes[0, 0].barh(noms, temps, color='skyblue')
        axes[0, 0].set_xlabel('Temps (secondes)')
        axes[0, 0].set_title('Temps d\'execution')
        
        # Graphique réduction
        axes[0, 1].barh(noms, reductions, color='lightcoral')
        axes[0, 1].set_xlabel('Taux de reduction (%)')
        axes[0, 1].set_title('Doublons supprimes')
        
        # Graphique F1-score
        if f1_scores:
            axes[1, 0].barh(noms, f1_scores, color='lightgreen')
            axes[1, 0].set_xlabel('F1-Score')
            axes[1, 0].set_title('Precision (F1-Score)')
            axes[1, 0].set_xlim(0, 1)
        
        # Graphique compromis
        axes[1, 1].scatter(temps, reductions, s=100, c=range(len(noms)), cmap='viridis')
        for i, nom in enumerate(noms):
            axes[1, 1].annotate(nom, (temps[i], reductions[i]), fontsize=8)
        axes[1, 1].set_xlabel('Temps (s)')
        axes[1, 1].set_ylabel('Taux reduction (%)')
        axes[1, 1].set_title('Compromis Temps vs Reduction')
        
        plt.tight_layout()
        filepath = os.path.join(EXPORT_DIR, "visualisation_6_methodes.png")
        plt.savefig(filepath, dpi=150)
        print(f"💾 Graphique: {filepath}")
        plt.close()
        
    except Exception as e:
        print(f"⚠️ Erreur visualisation: {e}")


# ============================================================
# 12. MAIN
# ============================================================

def main():
    print("=" * 80)
    print("🔬 COMPARAISON DES MÉTHODES DE DÉDUPLICATION")
    print(f"   Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   Méthodes: Edit Distance | Jaccard Caractères | Jaccard Mots")
    print("             TF-IDF seul | Cosine TF-IDF")
    print("=" * 80)
    
    # Charger les commentaires
    docs = charger_commentaires()
    nb_total = len(docs)
    print(f"\n📊 {nb_total} commentaires analysés")
    
    # Créer vérité terrain
    verite_terrain = creer_verite_terrain_manuel(docs)
    
    # NOUVEAU: Vérifier et exporter la vérité terrain
    vrais_doublons = verifier_et_exporter_verite_terrain(docs, verite_terrain)
    
    # Option: Vérification manuelle interactive
    print("\n" + "=" * 80)
    print("🔍 VÉRIFICATION MANUELLE INTERACTIVE")
    print("=" * 80)
    verif_manuelle = input("\nVoulez-vous vérifier manuellement quelques doublons ? (o/n): ")
    
    if verif_manuelle.lower() == 'o' and vrais_doublons:
        print("\n📋 Liste des doublons disponibles :")
        for i, paire in enumerate(vrais_doublons[:10]):
            print(f"   {i+1}. Doublon #{i+1} (Index {paire['original_index']} ↔ {paire['doublon_index']})")
        
        choix = input("\nEntrez le numéro du doublon à vérifier (ou 'q' pour quitter): ")
        if choix.isdigit() and 1 <= int(choix) <= len(vrais_doublons):
            paire = vrais_doublons[int(choix)-1]
            est_doublon = verifier_doublon_manuellement(docs, paire['original_index'], paire['doublon_index'])
            print(f"\n✅ Votre verdict : {'DOUBLON CONFIRMÉ' if est_doublon else 'PAS DOUBLON'}")
    
    resultats = {}
    
    # Liste des méthodes (sans embeddings par défaut)
    methodes = [
        ("Edit Distance", methode_edit_distance),
        ("Jaccard Caractères", methode_jaccard_caracteres),
        ("Jaccard Mots", methode_jaccard_mots),
        ("TF-IDF seul", methode_tfidf_seul),
        ("Cosine TF-IDF", methode_cosine_tfidf),
    ]
    
    # Tester chaque méthode
    for nom, methode in methodes:
        print(f"\n⏳ {nom}...")
        try:
            t0 = time.time()
            gardes, supprimes = methode(docs)
            temps = time.time() - t0
            
            if gardes is None:
                print(f"   ❌ Méthode non disponible")
                resultats[nom] = {"gardes": None, "supprimes": None, "temps": None}
                continue
            
            # Calculer métriques
            metriques = calculer_metriques(supprimes, verite_terrain, nb_total)
            
            resultats[nom] = {
                "gardes": gardes,
                "supprimes": supprimes,
                "temps": temps,
                "metriques": metriques
            }
            
            print(f"   ✅ Terminé en {temps:.2f}s")
            print(f"      → {len(supprimes)} doublons trouvés ({len(supprimes)/nb_total*100:.1f}%)")
            print(f"      → Précision: {metriques['precision']:.2%} | Rappel: {metriques['rappel']:.2%} | F1: {metriques['f1']:.2%}")
            
            # Exporter les doublons en CSV
            exporter_doublons_csv(docs, supprimes, nom)
            
            # Afficher quelques exemples
            if supprimes:
                exemples = list(supprimes)[:3]
                print(f"      → Exemples: {[docs[idx]['Commentaire_Client'][:50] for idx in exemples]}")
                
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
            resultats[nom] = {"gardes": None, "supprimes": None, "temps": None}
    
    # Tableau récapitulatif
    print(f"\n{'='*80}")
    print("📊 TABLEAU RÉCAPITULATIF COMPLET")
    print(f"{'='*80}")
    print(f"{'Méthode':<22} {'Gardés':>7} {'Supprimés':>10} {'Réduction':>10} {'Temps':>8} {'F1-Score':>10}")
    print("-" * 75)

    for nom, res in resultats.items():
        if res["gardes"] is not None:
            nb_s = len(res["supprimes"])
            print(f"{nom:<22} {len(res['gardes']):>7} {nb_s:>10} {nb_s/nb_total*100:>9.1f}% {res['temps']:>7.2f}s {res['metriques']['f1']:>9.2%}")
        else:
            print(f"{nom:<22} {'ERR':>7} {'ERR':>10} {'ERR':>10} {'ERR':>8} {'ERR':>10}")
    
    # Sauvegardes
    print(f"\n{'='*80}")
    print("💾 SAUVEGARDES")
    print(f"{'='*80}")
    
    sauvegarder_dans_mongodb(resultats, docs, verite_terrain)
    exporter_json(resultats, docs)
    exporter_excel_complet(resultats, docs)
    visualiser(resultats, docs)
    
    # Conclusion
    print(f"\n{'='*80}")
    print("💡 CONCLUSION")
    print(f"{'='*80}")
    
    # Trouver meilleure méthode (meilleur F1)
    methodes_valides = [(nom, res) for nom, res in resultats.items() 
                        if res["gardes"] is not None and "metriques" in res]
    
    if methodes_valides:
        meilleure = max(methodes_valides, key=lambda x: x[1]["metriques"]["f1"])
        
        print(f"""
   🏆 MEILLEURE MÉTHODE: {meilleure[0]}
      → F1-Score: {meilleure[1]['metriques']['f1']:.2%}
      → {len(meilleure[1]['supprimes'])} doublons sur {nb_total} commentaires
      → Temps: {meilleure[1]['temps']:.2f}s
      → Précision: {meilleure[1]['metriques']['precision']:.2%}
      → Rappel: {meilleure[1]['metriques']['rappel']:.2%}

   📁 Fichiers générés dans {EXPORT_DIR}:
      - resultats_6_methodes.xlsx (Excel complet)
      - resultats_6_methodes.json (JSON)
      - visualisation_6_methodes.png (Graphique)
      - doublons_*.csv (CSV par méthode)
      - verite_terrain_doublons.csv (Doublons vérité terrain)
      - verification_verite_terrain.html (Rapport HTML)
      - MongoDB: {DB_NAME}.{COLLECTION_RESULTS}
      - MongoDB: {DB_NAME}.{COLLECTION_COMMENTS}

   ✅ RECOMMANDATION FINALE: {meilleure[0]}
      → À intégrer dans votre script Spark avec seuil {SEUIL}
    """)
    else:
        print("   ❌ Aucune méthode n'a fonctionné correctement")
    
    print("=" * 80)


if __name__ == "__main__":
    main()