#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pipeline_nettoyage.py
# Pipeline automatique — Nettoyage corpus télécom DZ
# Prefect | Spark 4.1.1 | Multi-Node
#
# ORDRE :
#   Étape 1 → 01_supprimer_urls_multinode.py
#   Étape 2 → 03_supprimer_chiffres_multinode.py
#   Étape 3 → Supprimer_doublons_multinode.py
#   Étape 4 → extraire_emojis_multinode.py

from prefect import flow, task
from pymongo import MongoClient
import subprocess, time, os
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI   = "mongodb://localhost:27018/"
DB_NAME     = "telecom_algerie"
SCRIPTS_DIR = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/code"

SCRIPTS = {
    "etape_1": "01_supprimer_urls_multinode.py",
    "etape_2": "03_supprimer_chiffres_multinode.py",
    "etape_3": "Supprimer_doublons_multinode.py",
    "etape_4": "extraire_emojis_multinode.py",
}

COLLECTIONS = {
    "source"   : "commentaires_bruts",
    "etape_1"  : "commentaires_sans_urls_arobase",
    "etape_2"  : "commentaires_sans_chiffres_certains",
    "etape_3"  : "commentaires_sans_doublons",
    "etape_4"  : "commentaires_sans_emojis",
}

RAPPORT_PATH = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_pipeline.txt"

# ============================================================
# FONCTION UTILITAIRE — Compter les documents
# ============================================================
def compter_docs(collection_name):
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        count  = client[DB_NAME][collection_name].count_documents({})
        client.close()
        return count
    except:
        return -1

# ============================================================
# TÂCHES PREFECT
# ============================================================

@task(name="Étape 1 — Supprimer URLs + @arobase + Hashtags + Ponctuations",
      retries=1, retry_delay_seconds=10)
def etape_1_urls():
    print("\n" + "="*60)
    print("🔹 ÉTAPE 1 — URLs + @arobase + Hashtags + Ponctuations")
    print("="*60)

    avant = compter_docs(COLLECTIONS["source"])
    print(f"   📥 Source : {avant} documents")

    script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_1"])
    debut  = time.time()

    result = subprocess.run(
        ["python3", script],
        capture_output=True, text=True
    )

    duree = time.time() - debut

    if result.returncode != 0:
        print(f"❌ ERREUR :\n{result.stderr}")
        raise Exception(f"Étape 1 échouée : {result.stderr[-200:]}")

    apres = compter_docs(COLLECTIONS["etape_1"])
    print(f"   📤 Résultat : {apres} documents")
    print(f"   ⏱️  Durée    : {duree:.1f}s")
    print("   ✅ Étape 1 terminée !")

    return {"avant": avant, "apres": apres, "duree": duree}


@task(name="Étape 2 — Supprimer Chiffres",
      retries=1, retry_delay_seconds=10)
def etape_2_chiffres():
    print("\n" + "="*60)
    print("🔹 ÉTAPE 2 — Suppression Chiffres")
    print("="*60)

    avant = compter_docs(COLLECTIONS["etape_1"])
    print(f"   📥 Source : {avant} documents")

    script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_2"])
    debut  = time.time()

    result = subprocess.run(
        ["python3", script],
        capture_output=True, text=True
    )

    duree = time.time() - debut

    if result.returncode != 0:
        print(f"❌ ERREUR :\n{result.stderr}")
        raise Exception(f"Étape 2 échouée : {result.stderr[-200:]}")

    apres = compter_docs(COLLECTIONS["etape_2"])
    print(f"   📤 Résultat : {apres} documents")
    print(f"   ⏱️  Durée    : {duree:.1f}s")
    print("   ✅ Étape 2 terminée !")

    return {"avant": avant, "apres": apres, "duree": duree}


@task(name="Étape 3 — Supprimer Doublons",
      retries=1, retry_delay_seconds=10)
def etape_3_doublons():
    print("\n" + "="*60)
    print("🔹 ÉTAPE 3 — Suppression Doublons")
    print("="*60)

    avant = compter_docs(COLLECTIONS["etape_2"])
    print(f"   📥 Source : {avant} documents")

    script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_3"])
    debut  = time.time()

    result = subprocess.run(
        ["python3", script],
        capture_output=True, text=True
    )

    duree = time.time() - debut

    if result.returncode != 0:
        print(f"❌ ERREUR :\n{result.stderr}")
        raise Exception(f"Étape 3 échouée : {result.stderr[-200:]}")

    apres = compter_docs(COLLECTIONS["etape_3"])
    print(f"   📤 Résultat : {apres} documents")
    print(f"   ❌ Supprimés : {avant - apres} doublons")
    print(f"   ⏱️  Durée    : {duree:.1f}s")
    print("   ✅ Étape 3 terminée !")

    return {"avant": avant, "apres": apres, "duree": duree}


@task(name="Étape 4 — Extraire Emojis",
      retries=1, retry_delay_seconds=10)
def etape_4_emojis():
    print("\n" + "="*60)
    print("🔹 ÉTAPE 4 — Extraction Emojis")
    print("="*60)

    avant = compter_docs(COLLECTIONS["etape_3"])
    print(f"   📥 Source : {avant} documents")

    script = os.path.join(SCRIPTS_DIR, SCRIPTS["etape_4"])
    debut  = time.time()

    result = subprocess.run(
        ["python3", script],
        capture_output=True, text=True
    )

    duree = time.time() - debut

    if result.returncode != 0:
        print(f"❌ ERREUR :\n{result.stderr}")
        raise Exception(f"Étape 4 échouée : {result.stderr[-200:]}")

    apres = compter_docs(COLLECTIONS["etape_4"])
    print(f"   📤 Résultat : {apres} documents")
    print(f"   ⏱️  Durée    : {duree:.1f}s")
    print("   ✅ Étape 4 terminée !")

    return {"avant": avant, "apres": apres, "duree": duree}


# ============================================================
# FLOW PRINCIPAL
# ============================================================

@flow(name="Pipeline Nettoyage — Télécom DZ")
def pipeline_nettoyage():

    debut_pipeline = time.time()

    print("\n" + "="*60)
    print("🚀 PIPELINE NETTOYAGE — TÉLÉCOM DZ")
    print(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Scripts : {SCRIPTS_DIR}")
    print("="*60)

    # ── Lancer les étapes dans l'ordre ───────────────────────
    stats_1 = etape_1_urls()
    stats_2 = etape_2_chiffres()
    stats_3 = etape_3_doublons()
    stats_4 = etape_4_emojis()

    # ── Résumé final ─────────────────────────────────────────
    duree_totale = time.time() - debut_pipeline

    print("\n" + "="*60)
    print("📊 RÉSUMÉ FINAL DU PIPELINE")
    print("="*60)
    print(f"   Étape 1 — URLs+Hashtags : {stats_1['avant']:>6} → {stats_1['apres']:>6} docs  ⏱️  {stats_1['duree']:.1f}s")
    print(f"   Étape 2 — Chiffres      : {stats_2['avant']:>6} → {stats_2['apres']:>6} docs  ⏱️  {stats_2['duree']:.1f}s")
    print(f"   Étape 3 — Doublons      : {stats_3['avant']:>6} → {stats_3['apres']:>6} docs  ⏱️  {stats_3['duree']:.1f}s")
    print(f"   Étape 4 — Emojis        : {stats_4['avant']:>6} → {stats_4['apres']:>6} docs  ⏱️  {stats_4['duree']:.1f}s")
    print(f"   {'─'*50}")
    print(f"   ⏱️  Temps total           : {duree_totale:.1f}s")
    print(f"   📥 Docs initiaux         : {stats_1['avant']}")
    print(f"   📤 Docs finaux           : {stats_4['apres']}")
    print(f"   ❌ Docs supprimés        : {stats_1['avant'] - stats_4['apres']}")
    print("="*60)
    print("🎉 PIPELINE TERMINÉ AVEC SUCCÈS !")

    # ── Rapport ───────────────────────────────────────────────
    rapport = f"""
{"="*60}
RAPPORT PIPELINE NETTOYAGE — TÉLÉCOM DZ
{"="*60}
Date    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Scripts : {SCRIPTS_DIR}

RÉSULTATS :
   Étape 1 — URLs+Hashtags : {stats_1['avant']} → {stats_1['apres']} docs ({stats_1['duree']:.1f}s)
   Étape 2 — Chiffres      : {stats_2['avant']} → {stats_2['apres']} docs ({stats_2['duree']:.1f}s)
   Étape 3 — Doublons      : {stats_3['avant']} → {stats_3['apres']} docs ({stats_3['duree']:.1f}s)
   Étape 4 — Emojis        : {stats_4['avant']} → {stats_4['apres']} docs ({stats_4['duree']:.1f}s)

TOTAL :
   Docs initiaux  : {stats_1['avant']}
   Docs finaux    : {stats_4['apres']}
   Docs supprimés : {stats_1['avant'] - stats_4['apres']}
   Temps total    : {duree_totale:.1f}s
   Statut         : ✅ SUCCÈS
{"="*60}
"""
    os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
    with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
        f.write(rapport)

    print(f"✅ Rapport : {RAPPORT_PATH}")


# ============================================================
# LANCEMENT
# ============================================================
if __name__ == "__main__":
    pipeline_nettoyage()