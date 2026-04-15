# #!/usr/bin/env python3
# # kafka_consumer.py - Orchestrateur qui appelle les vrais scripts

# from kafka import KafkaConsumer
# import json
# import subprocess
# import os
# from datetime import datetime

# # Configuration
# KAFKA_BOOTSTRAP = "localhost:9092"
# TOPIC = "commentaires_bruts"

# # Chemins des scripts de nettoyage
# BASE_DIR = "/home/mouna/projet_telecom/scripts/nettoyage"

# SCRIPTS = {
#     "normalisation": os.path.join(BASE_DIR, "Nettoyage_des_textes/code/suppression_urls.py"),
#     "emojis": os.path.join(BASE_DIR, "Nettoyage_des_textes/code/extraire_emojis_multinode.py"),
#     "normalisation_finale": os.path.join(BASE_DIR, "Normalisation/code/normalisation_multinode.py"),
#     "flags": os.path.join(BASE_DIR, "Flags/flags_complet.py"),
# }

# def executer_script(script_path):
#     """Exécute un script Python et retourne le résultat"""
#     try:
#         result = subprocess.run(
#             ["python3", script_path],
#             capture_output=True,
#             text=True,
#             timeout=300
#         )
#         return result.returncode == 0, result.stdout
#     except Exception as e:
#         print(f"❌ Erreur: {e}")
#         return False, ""

# print("=" * 60)
# print("🚀 CONSUMER KAFKA EN MARCHE")
# print(f"   Topic: {TOPIC}")
# print("=" * 60)

# consumer = KafkaConsumer(
#     TOPIC,
#     bootstrap_servers=[KAFKA_BOOTSTRAP],
#     auto_offset_reset='earliest',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# for message in consumer:
#     data = message.value
#     commentaire = data.get("commentaire", "")
    
#     print(f"\n📥 Nouveau commentaire: {commentaire}")
#     print("🔄 Application du pipeline de nettoyage...")
    
#     # Étape 1: Normalisation (suppression URLs)
#     executer_script(SCRIPTS["normalisation"])
    
#     # Étape 3: Extraction emojis
#     executer_script(SCRIPTS["emojis"])
    
#     # Étape 4: Normalisation finale
#     executer_script(SCRIPTS["normalisation_finale"])
    
#     # Étape 5: Extraction des flags
#     executer_script(SCRIPTS["flags"])
    
#     print(f"✅ Commentaire traité: {commentaire}")

#!/usr/bin/env python3
# =============================================================================
#  KAFKA CONSUMER — VERSION COMPLÈTE CORRIGÉE
# =============================================================================
#
#  CE QUE CE SCRIPT FAIT :
#  ─────────────────────────────────────────────────────────────
#  1. Reçoit le commentaire depuis Kafka
#  2. Normalisation légère en Python pur (URLs, emojis, ponctuation)
#     → PAS de Spark pour un seul commentaire (trop lourd)
#  3. Vérifie si le commentaire est un doublon
#     → Si oui  : incrémente fréquence, ne rappelle pas le modèle
#     → Si non  : envoie à l'API Kaggle pour prédiction
#  4. Sauvegarde dans MongoDB avec label + fréquence
#  5. Dashboard lit MongoDB → affiche les vrais chiffres
#
#  PRÉREQUIS :
#  pip install kafka-python pymongo requests
# =============================================================================

import re
import json
import time
import hashlib
import requests
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient

# =============================================================================
#  CONFIGURATION
# =============================================================================
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC           = "commentaires_bruts"

MONGO_URI   = "mongodb://localhost:27018/"
DB_NAME     = "telecom_algerie"
COL_PRED    = "commentaires_predictions"    # collection finale pour le dashboard

# ── URL de ton API Kaggle (à remplacer après avoir lancé kaggle_api_server.py)
# Ligne à modifier
KAGGLE_API_URL = "https://imprint-nerd-wok.ngrok-free.dev/predict"
# Exemple : "https://abc123.ngrok.io/predict"

# =============================================================================
#  1. NORMALISATION LÉGÈRE (Python pur, sans Spark)
#     Supprime URLs, mentions, hashtags, emojis, caractères spéciaux
# =============================================================================
def normaliser_texte(texte: str) -> str:
    """
    Normalisation légère pour un commentaire unique en temps réel.
    Pour le batch (corpus historique), tes scripts Spark restent utilisés.
    """
    if not texte or not isinstance(texte, str):
        return ""

    # ── Supprimer URLs ──
    texte = re.sub(r'http\S+|www\.\S+|ftp\.\S+', '', texte)

    # ── Supprimer mentions et hashtags ──
    texte = re.sub(r'@\w+', '', texte)
    texte = re.sub(r'#\w+', '', texte)

    # ── Extraire le sens des emojis (avant suppression) ──
    # Certains emojis portent un sentiment — on les convertit en texte
    emoji_map = {
        '😊': 'mliha', '😍': 'mziana bzaf', '👍': 'bravo',
        '❤️': 'mliha bzaf', '😡': 'ghezeb', '😠': 'ghezeb',
        '👎': 'mazyench', '😢': 'mhazoun', '😭': 'mhazoun bzaf',
        '🔥': 'top', '✅': 'mlih', '❌': 'machi mlih',
        '⭐': 'mziana', '💯': 'top bzaf', '🙏': 'chokran',
    }
    for emoji, sens in emoji_map.items():
        texte = texte.replace(emoji, f' {sens} ')

    # ── Supprimer tous les emojis restants ──
    texte = re.sub(
        r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF'
        r'\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF'
        r'\U00002702-\U000027B0\U000024C2-\U0001F251'
        r'\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F]+',
        '', texte
    )

    # ── Normalisation des caractères arabes ──
    # Unifier les formes de alef
    texte = re.sub(r'[إأآا]', 'ا', texte)
    # Supprimer le tatweel (étirement décoratif)
    texte = re.sub(r'ـ', '', texte)
    # Supprimer les tashkil (voyelles courtes)
    texte = re.sub(r'[\u064B-\u065F]', '', texte)

    # ── Supprimer ponctuation excessive (garder les arabes) ──
    texte = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', texte)

    # ── Normaliser les répétitions (hhhhhh → h, !!! → !) ──
    texte = re.sub(r'(.)\1{3,}', r'\1', texte)

    # ── Normaliser les espaces ──
    texte = re.sub(r'\s+', ' ', texte).strip().lower()

    return texte


# =============================================================================
#  2. DOUBLON — DÉTECTION ET COMPTAGE
#     Si 100 clients envoient le même commentaire :
#     → on prédit 1 seule fois
#     → on stocke frequence=100
#     → le dashboard affiche 100 (pas 1)
# =============================================================================
def calculer_hash(texte_normalise: str) -> str:
    """Hash MD5 du texte normalisé pour détecter les doublons."""
    return hashlib.md5(texte_normalise.encode('utf-8')).hexdigest()


def verifier_doublon(texte_normalise: str, db) -> dict | None:
    """
    Vérifie si ce texte existe déjà dans la collection.
    Retourne le document existant ou None.
    """
    hash_texte = calculer_hash(texte_normalise)
    return db[COL_PRED].find_one({"hash_texte": hash_texte})


def incrementer_frequence(doc_id, db):
    """Incrémente la fréquence d'un commentaire doublon."""
    db[COL_PRED].update_one(
        {"_id": doc_id},
        {
            "$inc": {"frequence": 1},
            "$set": {"derniere_occurrence": datetime.now()}
        }
    )


# =============================================================================
#  3. APPEL À L'API KAGGLE
#     Le modèle tourne sur Kaggle (GPU gratuit)
#     Ton WSL envoie juste le texte et reçoit le label
#     → Zéro charge sur ton CPU/RAM local
# =============================================================================
def predire_via_kaggle(texte_normalise: str) -> dict:
    """
    Envoie le texte à l'API Kaggle et retourne la prédiction.
    Retry automatique en cas d'erreur réseau.
    """
    payload = {"commentaire": texte_normalise}

    for tentative in range(3):    # 3 tentatives
        try:
            response = requests.post(
                KAGGLE_API_URL,
                json=payload,
                timeout=15
            )
            if response.status_code == 200:
                return response.json()
            else:
                print(f"   ⚠️  API Kaggle erreur {response.status_code} "
                      f"(tentative {tentative+1}/3)")
        except requests.exceptions.Timeout:
            print(f"   ⚠️  Timeout API Kaggle (tentative {tentative+1}/3)")
        except requests.exceptions.ConnectionError:
            print(f"   ⚠️  Connexion API Kaggle impossible (tentative {tentative+1}/3)")
        except Exception as e:
            print(f"   ⚠️  Erreur API Kaggle : {e} (tentative {tentative+1}/3)")

        if tentative < 2:
            time.sleep(2 * (tentative + 1))   # attente croissante

    # Après 3 échecs → retourner NEUTRE par défaut
    print("   ❌ API Kaggle inaccessible → label NEUTRE par défaut")
    return {
        "label": "NEUTRE",
        "confidence": 0.0,
        "probabilities": {"NEGATIF": 0.33, "NEUTRE": 0.34, "POSITIF": 0.33},
        "erreur": "API Kaggle inaccessible"
    }


# =============================================================================
#  4. SAUVEGARDE DANS MONGODB
# =============================================================================
def sauvegarder(
    commentaire_original: str,
    texte_normalise: str,
    source: str,
    mongo_id: str,
    prediction: dict,
    db
):
    """Sauvegarde la prédiction avec fréquence dans MongoDB."""
    hash_texte = calculer_hash(texte_normalise)
    doc = {
        "commentaire_original": commentaire_original,
        "texte_normalise"     : texte_normalise,
        "hash_texte"          : hash_texte,
        "source"              : source,
        "mongo_id_original"   : mongo_id,
        "label"               : prediction.get("label", "NEUTRE"),
        "confidence"          : prediction.get("confidence", 0.0),
        "probabilities"       : prediction.get("probabilities", {}),
        "frequence"           : 1,
        "date_creation"       : datetime.now(),
        "derniere_occurrence" : datetime.now(),
    }
    db[COL_PRED].insert_one(doc)
    print(f"   💾 Sauvegardé dans MongoDB (fréquence=1)")


# =============================================================================
#  5. MAIN — BOUCLE CONSUMER KAFKA
# =============================================================================
def main():
    print("=" * 65)
    print("  🚀 KAFKA CONSUMER — Pipeline complet Darija")
    print("=" * 65)
    print("  Étapes par commentaire :")
    print("    1. Normalisation légère (Python pur)")
    print("    2. Détection doublon → si oui : fréquence+1 (fin)")
    print("    3. Appel API Kaggle → prédiction sentiment")
    print("    4. Sauvegarde MongoDB avec label + fréquence")
    print("=" * 65)

    # Connexion MongoDB
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    # Index pour accélérer la recherche de doublons
    db[COL_PRED].create_index("hash_texte")
    print("✅ Connecté à MongoDB")

    # Connexion Kafka Consumer
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("✅ Connecté à Kafka")
    print(f"\n📡 En attente de messages sur le topic '{TOPIC}'...\n")

    compteur       = 0
    doublons       = 0
    nouvelles_pred = 0

    for message in consumer:
        compteur += 1
        data = message.value

        commentaire_original = data.get("commentaire", "")
        source               = data.get("source", "inconnu")
        mongo_id             = data.get("mongo_id", "")

        print(f"{'='*65}")
        print(f"📥 [{compteur}] Nouveau message — source: {source}")
        print(f"   Texte : {commentaire_original[:70]}...")

        # ── ÉTAPE 1 : Normalisation ──────────────────────────────────────
        print("\n🔧 [1/4] Normalisation...")
        texte_normalise = normaliser_texte(commentaire_original)
        print(f"   Résultat : '{texte_normalise[:70]}'")

        if not texte_normalise:
            print("   ⚠️  Texte vide après normalisation — ignoré")
            continue

        # ── ÉTAPE 2 : Vérification doublon ───────────────────────────────
        print("\n🔍 [2/4] Vérification doublon...")
        doc_existant = verifier_doublon(texte_normalise, db)

        if doc_existant:
            doublons += 1
            incrementer_frequence(doc_existant["_id"], db)
            freq = doc_existant.get("frequence", 1) + 1
            emoji = {"NEGATIF": "🔴", "NEUTRE": "🟡", "POSITIF": "🟢"}.get(
                doc_existant.get("label", ""), "⚪"
            )
            print(f"   ♻️  DOUBLON détecté ! Label conservé : "
                  f"{doc_existant.get('label')} {emoji}")
            print(f"   📊 Nouvelle fréquence : {freq}")
            print(f"   (pas d'appel API — économie de ressources)")
            continue

        print("   ✅ Commentaire unique — continuation du pipeline")

        # ── ÉTAPE 3 : Prédiction via API Kaggle ──────────────────────────
        print("\n🤖 [3/4] Appel API Kaggle...")
        prediction = predire_via_kaggle(texte_normalise)

        emoji = {"NEGATIF": "🔴", "NEUTRE": "🟡", "POSITIF": "🟢"}.get(
            prediction.get("label", ""), "⚪"
        )
        print(f"   🎯 Prédiction : {prediction.get('label')} {emoji}")
        print(f"   📊 Confiance  : {prediction.get('confidence', 0):.2%}")
        print(f"   Probabilités  : "
              f"neg={prediction['probabilities'].get('NEGATIF', 0):.2f} | "
              f"neu={prediction['probabilities'].get('NEUTRE', 0):.2f} | "
              f"pos={prediction['probabilities'].get('POSITIF', 0):.2f}")

        # ── ÉTAPE 4 : Sauvegarde MongoDB ─────────────────────────────────
        print("\n💾 [4/4] Sauvegarde MongoDB...")
        sauvegarder(
            commentaire_original=commentaire_original,
            texte_normalise=texte_normalise,
            source=source,
            mongo_id=mongo_id,
            prediction=prediction,
            db=db
        )
        nouvelles_pred += 1

        # ── Statistiques ─────────────────────────────────────────────────
        print(f"\n📈 Stats session : "
              f"{nouvelles_pred} nouvelles prédictions | "
              f"{doublons} doublons comptabilisés")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Consumer arrêté proprement")
