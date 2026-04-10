#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
supprimer_emojis_texte_local.py – Supprime les emojis du texte dans 3 colonnes
(après les avoir extraits et sauvegardés ailleurs)
"""

from pymongo import MongoClient
import re
from datetime import datetime

# ============================================================
# CONFIGURATION (MongoDB local)
# ============================================================
MONGO_HOST = 'localhost'
MONGO_PORT = 27018          # ← vérifiez votre port (27017 ou 27018)
DB_NAME = 'telecom_algerie'   # ← adaptez si nécessaire
COLLECTION_NAME = 'commentaires_normalises'

# Regex pour détecter TOUS les emojis (identique à celle utilisée pour l'extraction)
_EMOJI_RANGES = (
    '\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF'
    '\U0001F1E0-\U0001F1FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF'
    '\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F'
    '\U0001FA70-\U0001FAFF\U0001FB00-\U0001FBFF\u2600-\u26FF\u2700-\u27BF'
    '\u231A-\u231B\u23E9-\u23F3\u23F8-\u23FA\u25AA-\u25AB\u25B6\u25C0'
    '\u25FB-\u25FE\u2614-\u2615\u2648-\u2653\u267F\u2693\u26A1\u26AA-\u26AB'
    '\u26BD-\u26BE\u26C4-\u26C5\u26CE\u26D4\u26EA\u26F2-\u26F3\u26F5\u26FA'
    '\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721'
    '\u2728\u2733-\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763-\u2764'
    '\u2795-\u2797\u27A1\u27B0\u27BF\u2934-\u2935\u2B05-\u2B07\u2B1B-\u2B1C'
    '\u2B50\u2B55\u3030\u303D\u3297\u3299\uFE0F\u200D'
)
EMOJI_REGEX = re.compile(f"[{_EMOJI_RANGES}]+", flags=re.UNICODE)

def remove_emojis(text):
    """Supprime tous les emojis d'une chaîne et nettoie les espaces"""
    if not text or not isinstance(text, str):
        return text
    cleaned = EMOJI_REGEX.sub('', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def nettoyer_trois_colonnes():
    print("=" * 80)
    print("🗑️  SUPPRESSION DES EMOJIS DANS 3 COLONNES (MongoDB local)")
    print("=" * 80)

    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ Connecté à MongoDB local")
        
        db = client[DB_NAME]
        coll = db[COLLECTION_NAME]

        total_docs = coll.count_documents({})
        print(f"📊 Total documents : {total_docs}")

        # Demander confirmation
        print("\n⚠️  Cette opération va supprimer DÉFINITIVEMENT les emojis")
        print("   dans les colonnes : Commentaire_Client, normalized_arabert, normalized_full")
        print("   (les emojis sont déjà sauvegardés dans emojis_originaux/emojis_sentiment)")
        reponse = input("\nVoulez-vous continuer ? (oui/non) : ")
        if reponse.lower() != 'oui':
            print("❌ Annulé.")
            client.close()
            return

        # Traitement par lots
        print("\n🔄 Suppression en cours...")
        batch_size = 500
        modified_count = 0
        count = 0

        for doc in coll.find():
            modified = False
            update_fields = {}

            for field in ["Commentaire_Client", "normalized_arabert", "normalized_full"]:
                original = doc.get(field, "")
                if original and isinstance(original, str):
                    cleaned = remove_emojis(original)
                    if original != cleaned:
                        update_fields[field] = cleaned
                        modified = True

            if modified:
                update_fields["emojis_supprimes_du_texte"] = True
                update_fields["date_suppression_emojis"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                coll.update_one({"_id": doc["_id"]}, {"$set": update_fields})
                modified_count += 1

            count += 1
            if count % 1000 == 0:
                print(f"   → {count}/{total_docs} documents traités ({modified_count} modifiés)")

        print(f"\n✅ Terminé !")
        print(f"   - Documents traités : {count}")
        print(f"   - Documents modifiés : {modified_count}")

        # Vérification sur un exemple
        sample = coll.find_one({"emojis_supprimes_du_texte": True})
        if sample:
            print("\n📝 EXEMPLE APRÈS SUPPRESSION :")
            print(f"   Commentaire_Client : {sample.get('Commentaire_Client')}")
            print(f"   normalized_arabert : {sample.get('normalized_arabert')}")
            print(f"   normalized_full : {sample.get('normalized_full')}")
            print(f"   emojis_originaux (sauvegardés) : {sample.get('emojis_originaux')}")
            print(f"   emojis_sentiment (sauvegardés) : {sample.get('emojis_sentiment')}")

    except Exception as e:
        print(f"❌ Erreur : {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("\n🔌 Connexion fermée")

if __name__ == "__main__":
    nettoyer_trois_colonnes()