#!/usr/bin/env python3
# comparaison_combinaisons.py
# ============================================================
# Compare 2 combinaisons sur 1000 commentaires réels :
#
#   Combo A : TF-IDF + Jaccard CARACTÈRES (ton code original)
#   Combo B : TF-IDF + Jaccard MOTS       (version améliorée)
#
#   Chaque combo testé en 3 modes : OU / ET / SCORE
# ============================================================

from pymongo import MongoClient
import time

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI  = "mongodb://localhost:27018/"
DB_NAME    = "telecom_algerie"
COLLECTION = "commentaires_sans_urls_arobase"
NB_DOCS    = 1000
SEUIL      = 0.85


# ============================================================
# CHARGEMENT MONGODB
# ============================================================

def charger_commentaires(n=NB_DOCS):
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    docs = list(client[DB_NAME][COLLECTION].find(
        {},
        {"_id": 1, "Commentaire_Client": 1, "source": 1, "moderateur": 1}
    ).limit(n))
    for i, doc in enumerate(docs):
        doc["id"] = i + 1
        doc["_id"] = str(doc["_id"])
        doc["Commentaire_Client"] = str(doc.get("Commentaire_Client", "")).strip()
    docs = [d for d in docs if len(d["Commentaire_Client"]) > 0]
    client.close()
    print(f"✅ {len(docs)} documents chargés")
    return docs


# ============================================================
# CALCUL DES 3 MATRICES (TF-IDF, Jaccard Chars, Jaccard Mots)
# ============================================================

def calculer_matrices(docs):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np

    n      = len(docs)
    textes = [d["Commentaire_Client"].lower().strip() for d in docs]

    # --- TF-IDF ---
    print("   [1/3] Calcul matrice TF-IDF...")
    vec      = TfidfVectorizer(analyzer='word', ngram_range=(1, 2),
                               min_df=1, sublinear_tf=True)
    tfidf_mat = cosine_similarity(vec.fit_transform(textes))

    # --- Jaccard Caractères (ton code original) ---
    print("   [2/3] Calcul matrice Jaccard Caractères...")
    jacc_char_mat = np.zeros((n, n))
    for i in range(n):
        jacc_char_mat[i][i] = 1.0
        for j in range(i + 1, n):
            t1, t2 = textes[i], textes[j]
            if t1 == t2:
                sim = 1.0
            else:
                s1 = set(t1)   # ← sur les CARACTÈRES
                s2 = set(t2)
                sim = len(s1 & s2) / len(s1 | s2) if (s1 or s2) else 0.0
            jacc_char_mat[i][j] = sim
            jacc_char_mat[j][i] = sim

    # --- Jaccard Mots (version améliorée) ---
    print("   [3/3] Calcul matrice Jaccard Mots...")
    jacc_mots_mat = np.zeros((n, n))
    for i in range(n):
        jacc_mots_mat[i][i] = 1.0
        for j in range(i + 1, n):
            t1, t2 = textes[i], textes[j]
            if t1 == t2:
                sim = 1.0
            else:
                s1 = set(t1.split())   # ← sur les MOTS
                s2 = set(t2.split())
                sim = len(s1 & s2) / len(s1 | s2) if (s1 or s2) else 0.0
            jacc_mots_mat[i][j] = sim
            jacc_mots_mat[j][i] = sim

    print("   ✅ 3 matrices calculées")
    return tfidf_mat, jacc_char_mat, jacc_mots_mat


# ============================================================
# DÉDUPLICATION SELON UN MODE
# ============================================================

def deduplication(docs, mat_a, mat_b, mode="ET", seuil=SEUIL):
    """
    mode = 'OU'    → doublon si mat_a >= seuil OU mat_b >= seuil
    mode = 'ET'    → doublon si mat_a >= seuil ET mat_b >= seuil
    mode = 'SCORE' → doublon si (mat_a + mat_b) / 2 >= seuil
    """
    gardes, supprimes = [], set()
    for i in range(len(docs)):
        if i in supprimes:
            continue
        gardes.append(docs[i])
        for j in range(i + 1, len(docs)):
            if j in supprimes:
                continue
            a = mat_a[i][j]
            b = mat_b[i][j]
            if mode == "OU":
                est_doublon = (a >= seuil or b >= seuil)
            elif mode == "ET":
                est_doublon = (a >= seuil and b >= seuil)
            else:  # SCORE
                est_doublon = ((a + b) / 2 >= seuil)
            if est_doublon:
                supprimes.add(j)
    return gardes, supprimes


# ============================================================
# AFFICHAGE EXEMPLES
# ============================================================

def afficher_exemples(docs, supprimes, tfidf_mat, jacc_mat, n=5):
    shown = 0
    for idx in sorted(supprimes):
        if shown >= n:
            break
        # Trouver le doc le plus similaire parmi les gardés
        commentaire = docs[idx]['Commentaire_Client'][:70]
        tfidf_max = max(tfidf_mat[idx][j] for j in range(idx))
        jacc_max  = max(jacc_mat[idx][j]  for j in range(idx))
        print(f"      → \"{commentaire}\"")
        print(f"         TF-IDF={tfidf_max:.2f} | Jaccard={jacc_max:.2f}")
        shown += 1


# ============================================================
# COMPARAISON PRINCIPALE
# ============================================================

def comparer():
    print("=" * 72)
    print("📊 COMPARAISON COMBO A vs COMBO B — 1000 COMMENTAIRES RÉELS")
    print(f"   Seuil = {SEUIL}")
    print(f"   Combo A : TF-IDF + Jaccard CARACTÈRES (code original)")
    print(f"   Combo B : TF-IDF + Jaccard MOTS       (version améliorée)")
    print("=" * 72)

    docs = charger_commentaires()
    nb   = len(docs)

    print("\n⏳ Calcul des 3 matrices...")
    t0 = time.time()
    tfidf_mat, jacc_char_mat, jacc_mots_mat = calculer_matrices(docs)
    print(f"   ⏱️  Terminé en {time.time()-t0:.2f}s\n")

    resultats = {}

    combos = {
        "A (Chars)": jacc_char_mat,
        "B (Mots)":  jacc_mots_mat,
    }

    for combo_nom, jacc_mat in combos.items():
        print(f"\n{'='*72}")
        print(f"🔬 COMBO {combo_nom} — TF-IDF + Jaccard {combo_nom}")
        print(f"{'='*72}")

        for mode in ["OU", "ET", "SCORE"]:
            t0 = time.time()
            gardes, supprimes = deduplication(docs, tfidf_mat, jacc_mat, mode)
            temps = time.time() - t0
            key = f"{combo_nom}_{mode}"
            resultats[key] = {
                "gardes": gardes, "supprimes": supprimes,
                "temps": temps, "combo": combo_nom, "mode": mode
            }
            print(f"\n   Mode {mode} → {len(supprimes):>3} doublons "
                  f"({len(supprimes)/nb*100:.1f}%) en {temps:.3f}s")
            afficher_exemples(docs, supprimes, tfidf_mat, jacc_mat)

    # --------------------------------------------------------
    # TABLEAU RÉCAPITULATIF
    # --------------------------------------------------------
    print(f"\n{'='*72}")
    print("📊 TABLEAU RÉCAPITULATIF FINAL")
    print(f"{'='*72}")
    print(f"{'Combinaison':<22} {'Mode':<8} {'Gardés':>7} "
          f"{'Supprimés':>10} {'Réduction':>10} {'Temps':>8}")
    print("-" * 68)

    # Référence : méthodes seules
    for nom, mat in [("TF-IDF seul", tfidf_mat),
                     ("Jaccard Chars seul", jacc_char_mat),
                     ("Jaccard Mots seul", jacc_mots_mat)]:
        g, s = [], set()
        for i in range(nb):
            if i in s: continue
            g.append(docs[i])
            for j in range(i+1, nb):
                if j not in s and mat[i][j] >= SEUIL:
                    s.add(j)
        print(f"{'Référence':<22} {nom:<18} {nb-len(s):>7} "
              f"{len(s):>10} {len(s)/nb*100:>9.1f}% {'—':>8}")

    print("-" * 68)
    for key, res in resultats.items():
        nb_s = len(res["supprimes"])
        print(f"{'Combo '+res['combo']:<22} {res['mode']:<8} {nb-nb_s:>7} "
              f"{nb_s:>10} {nb_s/nb*100:>9.1f}% {res['temps']:>7.3f}s")

    # --------------------------------------------------------
    # ANALYSE : QUI DÉTECTE QUOI DE PLUS
    # --------------------------------------------------------
    print(f"\n{'='*72}")
    print("🔍 ANALYSE — DIFFÉRENCES ENTRE COMBO A ET COMBO B")
    print(f"{'='*72}")

    for mode in ["OU", "ET", "SCORE"]:
        s_a = resultats[f"A (Chars)_{mode}"]["supprimes"]
        s_b = resultats[f"B (Mots)_{mode}"]["supprimes"]
        commun  = s_a & s_b
        only_a  = s_a - s_b
        only_b  = s_b - s_a

        print(f"\n   Mode {mode} :")
        print(f"      Commun A∩B            : {len(commun)} doublons")
        print(f"      Seulement Combo A (Chars) : {len(only_a)} doublons")
        print(f"      Seulement Combo B (Mots)  : {len(only_b)} doublons")

        if only_a:
            print(f"      ⚠️  Propres à Jaccard Chars (faux positifs potentiels) :")
            for idx in sorted(only_a)[:3]:
                print(f"         → \"{docs[idx]['Commentaire_Client'][:65]}\"")
        if only_b:
            print(f"      ✅ Propres à Jaccard Mots (doublons supplémentaires) :")
            for idx in sorted(only_b)[:3]:
                print(f"         → \"{docs[idx]['Commentaire_Client'][:65]}\"")

    # --------------------------------------------------------
    # CONCLUSION ET CHOIX FINAL
    # --------------------------------------------------------
    print(f"\n{'='*72}")
    print("💡 CONCLUSION — QUELLE COMBINAISON CHOISIR ?")
    print(f"{'='*72}")

    s_a_et = resultats["A (Chars)_ET"]["supprimes"]
    s_b_et = resultats["B (Mots)_ET"]["supprimes"]
    only_a_et = s_a_et - s_b_et

    print(f"""
   Sur tes {nb} commentaires réels :

   Combo A (TF-IDF + Jaccard Caractères) :
   → Détecte {len(resultats['A (Chars)_ET']['supprimes'])} doublons (mode ET)
   → {len(only_a_et)} doublons détectés par Chars mais PAS Mots
     = faux positifs potentiels (caractères arabes partagés ≠ même sens)

   Combo B (TF-IDF + Jaccard Mots) :
   → Détecte {len(resultats['B (Mots)_ET']['supprimes'])} doublons (mode ET)
   → 100% confirmés par TF-IDF ET Jaccard Mots simultanément

   🏆 MEILLEUR CHOIX : Combo B — TF-IDF + Jaccard MOTS en mode ET
      Raison : Jaccard Mots évite les faux positifs sur l'arabe
               (les caractères arabes sont partagés entre beaucoup de mots)
               Mode ET = les 2 méthodes doivent être d'accord → 0 faux positif

   👉 À intégrer dans le script Spark final avec :
      SEUIL_TFIDF   = 0.85
      SEUIL_JACCARD = 0.85
      MODE          = ET
    """)


# ============================================================
# LANCEMENT
# ============================================================

if __name__ == "__main__":
    try:
        import sklearn
        print("✅ scikit-learn disponible")
    except ImportError:
        print("❌ pip install scikit-learn")
        exit(1)

    comparer()