# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# fix_emojis_columns.py
# =====================
# Récupère les emojis depuis la collection SOURCE (brute) 
# et met à jour les colonnes 'emojis_originaux' et 'emojis_sentiment' 
# dans la collection 'commentaires_normalises'.
# """

# import re
# from pymongo import MongoClient, UpdateOne
# from configuration2 import MONGO_URI, DB_NAME_ATLAS

# # ============================================================
# # CONFIGURATION
# # ============================================================
# COL_SOURCE = "commentaires_bruts"          # Ta collection ORIGINALE (avec emojis)
# COL_DEST = "commentaires_normalises"       # Ta collection NETTOYÉE (à corriger)
# TEXT_COL_SOURCE = "Commentaire_Client"     # Colonne texte dans la source
# TEXT_COL_DEST = "normalized_arabert"       # Colonne texte dans la dest (déjà clean)
# BATCH_SIZE = 500

# # Dictionnaire simple Emoji -> Sentiment (Adapte-le si tu en as un plus gros)
# # Si tu as le master_dict.json, on peut le charger, ici je mets les bases
# EMOJI_TO_SENTIMENT = {
#     "🔴": "alerte", "❗": "important", "❌": "erreur", "✅": "succès",
#     "😡": "colère", "😠": "mécontentement", "😢": "tristesse", "😍": "amour",
#     "👍": "approbation", "👎": "désapprobation", "🤔": "réflexion","🤮": "dégoût"
#     # Ajoute tous les emojis de ton master_dict.json ici si besoin
# }

# # Regex pour trouver TOUS les emojis
# _EMOJI_RANGES = (
#     '\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF'
#     '\U0001F1E0-\U0001F1FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF'
#     '\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F'
#     '\U0001FA70-\U0001FAFF\U0001FB00-\U0001FBFF\u2600-\u26FF\u2700-\u27BF'
#     '\u231A-\u231B\u23E9-\u23F3\u23F8-\u23FA\u25AA-\u25AB\u25B6\u25C0'
#     '\u25FB-\u25FE\u2614-\u2615\u2648-\u2653\u267F\u2693\u26A1\u26AA-\u26AB'
#     '\u26BD-\u26BE\u26C4-\u26C5\u26CE\u26D4\u26EA\u26F2-\u26F3\u26F5\u26FA'
#     '\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721'
#     '\u2728\u2733-\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763-\u2764'
#     '\u2795-\u2797\u27A1\u27B0\u27BF\u2934-\u2935\u2B05-\u2B07\u2B1B-\u2B1C'
#     '\u2B50\u2B55\u3030\u303D\u3297\u3299\uFE0F\u200D'
# )
# EMOJI_REGEX = re.compile(f"[{_EMOJI_RANGES}]+", flags=re.UNICODE)

# print("=" * 80)
# print("🔧 CORRECTION DES COLONNES EMOJIS MANQUANTES")
# print("=" * 80)
# print(f"   Source (pour lire les emojis) : {COL_SOURCE}")
# print(f"   Destination (à mettre à jour) : {COL_DEST}")
# print("=" * 80)

# try:
#     client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
#     client.admin.command('ping')
#     db = client[DB_NAME_ATLAS]
    
#     coll_source = db[COL_SOURCE]
#     coll_dest = db[COL_DEST]
    
#     print("✅ Connecté à MongoDB Atlas")

#     total_docs = coll_dest.count_documents({})
#     print(f"📊 Total documents à vérifier : {total_docs}")

#     updates = []
#     docs_corrected = 0

#     # On parcourt la collection DESTINATION pour trouver ceux qui ont des problèmes
#     # Astuce : On cherche ceux où normalized_arabert != Commentaire_Client (s'ils existent encore) 
#     # OU simplement on met à jour tout le monde en relisant la source.
#     # Pour être sûr, on va lire la SOURCE et mettre à jour la DESTINATION par ID.
    
#     print("\n🔄 Lecture de la collection SOURCE pour extraire les emojis...")
    
#     cursor = coll_source.find({}, {"_id": 1, TEXT_COL_SOURCE: 1})
    
#     for doc in cursor:
#         doc_id = doc["_id"]
#         text_original = doc.get(TEXT_COL_SOURCE, "")
        
#         if not text_original:
#             continue

#         # 1. Détecter les emojis dans le texte ORIGINAL
#         found_emojis = EMOJI_REGEX.findall(text_original)
        
#         if found_emojis:
#             # Il y a des emojis ! On prépare la mise à jour
            
#             # Créer la liste des sentiments correspondants
#             sentiments = []
#             for e in found_emojis:
#                 # Cherche dans le dico, sinon met "emoji" par défaut
#                 sent = EMOJI_TO_SENTIMENT.get(e, "emoji")
#                 sentiments.append(sent)
            
#             # Préparer l'opération de mise à jour
#             update_op = UpdateOne(
#                 {"_id": doc_id},
#                 {
#                     "$set": {
#                         "emojis_originaux": found_emojis,
#                         "emojis_sentiment": sentiments
#                         # On ne touche PAS à normalized_arabert, il est déjà clean
#                     }
#                 }
#             )
#             updates.append(update_op)
#             docs_corrected += 1
            
#             if len(updates) >= BATCH_SIZE:
#                 result = coll_dest.bulk_write(updates, ordered=False)
#                 print(f"   → Lot mis à jour : {result.modified_count} docs")
#                 updates = []
#         else:
#             # Pas d'emojis dans la source non plus ? On s'assure que les champs sont vides proprement
#             # (Optionnel, seulement si tu veux uniformiser les données)
#             pass

#     # Dernier lot
#     if updates:
#         result = coll_dest.bulk_write(updates, ordered=False)
#         print(f"   → Dernier lot : {result.modified_count} docs")

#     print("\n" + "=" * 80)
#     print("📈 RÉSULTATS DE LA CORRECTION")
#     print("=" * 80)
#     print(f"   • Documents scannés dans la source : {total_docs}")
#     print(f"   • Documents ayant des emojis (corrigés) : {docs_corrected}")
    
#     if docs_corrected > 0:
#         print(f"   ✅ SUCCÈS : Les colonnes 'emojis_originaux' et 'emojis_sentiment' sont maintenant remplies !")
#     else:
#         print(f"   ℹ️  Aucun emoji trouvé dans la source (ou déjà tout bon).")

#     # Vérification sur l'exemple cité (si possible)
#     print("\n🔍 Vérification aléatoire sur un document corrigé...")
#     sample = coll_dest.find_one({"emojis_originaux": {"$ne": []}})
#     if sample:
#         print(f"   • ID: {sample['_id']}")
#         print(f"   • Emojis trouvés: {sample.get('emojis_originaux')}")
#         print(f"   • Sentiments: {sample.get('emojis_sentiment')}")
#         print(f"   • Texte clean: {sample.get('normalized_arabert', '')[:50]}...")
#     else:
#         print("   (Aucun document avec emojis trouvé pour l'exemple)")

#     print("=" * 80)

# except Exception as e:
#     print(f"\n❌ Erreur critique : {e}")
#     import traceback
#     traceback.print_exc()

# finally:
#     if 'client' in locals():
#         client.close()
#         print("\n🔌 Connexion fermée.")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
supprimer_emojis.py – Supprime tous les emojis des colonnes Commentaire_Client, normalized_arabert, normalized_full
"""

from pymongo import MongoClient
from configuration2 import MONGO_URI
import re
import sys
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# FONCTIONS DE NETTOYAGE
# ═══════════════════════════════════════════════════════════════════════════

def remove_emojis(text):
    """
    Supprime tous les emojis et symboles spéciaux d'un texte
    """
    if not text or not isinstance(text, str):
        return text
    
    # Pattern pour détecter les emojis (plage Unicode des emojis)
    # Cela couvre la plupart des emojis standards
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # Émoticônes
        "\U0001F300-\U0001F5FF"  # Symboles et pictogrammes
        "\U0001F680-\U0001F6FF"  # Transport et symboles
        "\U0001F700-\U0001F77F"  # Symboles alchimiques
        "\U0001F780-\U0001F7FF"  # Formes géométriques
        "\U0001F800-\U0001F8FF"  # Flèches supplémentaires
        "\U0001F900-\U0001F9FF"  # Emojis supplémentaires
        "\U0001FA00-\U0001FA6F"  # Échecs, cartes, etc.
        "\U0001FA70-\U0001FAFF"  # Objets divers
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251" 
        "]+",
        flags=re.UNICODE
    )
    
    # Supprimer les emojis
    cleaned_text = emoji_pattern.sub('', text)
    
    # Nettoyer les espaces multiples
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    
    # Supprimer les espaces au début et à la fin
    cleaned_text = cleaned_text.strip()
    
    return cleaned_text

def clean_commentaire_client(text):
    """
    Nettoie le champ Commentaire_Client
    """
    return remove_emojis(text)

def clean_normalized_arabert(text):
    """
    Nettoie le champ normalized_arabert
    """
    return remove_emojis(text)

def clean_normalized_full(text):
    """
    Nettoie le champ normalized_full
    """
    return remove_emojis(text)

# ═══════════════════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════

def supprimer_emojis_des_colonnes():
    """
    Supprime tous les emojis des colonnes spécifiées
    """
    
    print("=" * 80)
    print("🗑️  SUPPRESSION DES EMOJIS DANS LES 3 COLONNES")
    print("=" * 80)
    print("Colonnes concernées:")
    print("   - Commentaire_Client")
    print("   - normalized_arabert")
    print("   - normalized_full")
    print("=" * 80)
    
    try:
        # Connexion à MongoDB Atlas
        print("\n🔌 Connexion à MongoDB Atlas...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        print("✅ Connexion réussie")
        
        db = client["telecom_algerie_new"]
        collection = db["commentaires_normalises"]
        
        # Vérifier la collection
        total_docs = collection.count_documents({})
        print(f"\n📊 Total de documents dans la collection: {total_docs}")
        
        # Compter les documents qui ont des emojis
        print("\n🔍 Recherche des documents avec emojis...")
        
        # Échantillon pour vérifier
        docs_with_emojis = []
        for doc in collection.find().limit(100):
            text = doc.get("Commentaire_Client", "")
            if any(ord(c) > 0xFFFF for c in text if c):  # Détection simple d'emojis
                docs_with_emojis.append(doc)
        
        print(f"📊 Échantillon: {len(docs_with_emojis)} documents avec emojis sur 100 testés")
        
        if len(docs_with_emojis) == 0:
            print("✅ Aucun emoji trouvé dans les 100 premiers documents!")
            reponse = input("\nVoulez-vous quand même continuer la vérification complète? (oui/non): ")
            if reponse.lower() != 'oui':
                client.close()
                return
        
        # Demander confirmation
        print("\n⚠️  ATTENTION: Cette opération va supprimer définitivement tous les emojis")
        print("   dans les 3 colonnes de tous les documents.")
        reponse = input("\nVoulez-vous continuer? (oui/non): ")
        
        if reponse.lower() != 'oui':
            print("❌ Opération annulée")
            client.close()
            return
        
        # Traitement par lots
        print("\n🔄 Suppression des emojis en cours...")
        batch_size = 500
        count = 0
        modified_count = 0
        
        for doc in collection.find():
            modified = False
            update_fields = {}
            
            # Nettoyer Commentaire_Client
            if "Commentaire_Client" in doc and doc["Commentaire_Client"]:
                original = doc["Commentaire_Client"]
                cleaned = clean_commentaire_client(original)
                if original != cleaned:
                    update_fields["Commentaire_Client"] = cleaned
                    modified = True
            
            # Nettoyer normalized_arabert
            if "normalized_arabert" in doc and doc["normalized_arabert"]:
                original = doc["normalized_arabert"]
                cleaned = clean_normalized_arabert(original)
                if original != cleaned:
                    update_fields["normalized_arabert"] = cleaned
                    modified = True
            
            # Nettoyer normalized_full
            if "normalized_full" in doc and doc["normalized_full"]:
                original = doc["normalized_full"]
                cleaned = clean_normalized_full(original)
                if original != cleaned:
                    update_fields["normalized_full"] = cleaned
                    modified = True
            
            # Appliquer les modifications si nécessaire
            if modified:
                update_fields["emojis_supprimes"] = True
                update_fields["date_nettoyage_emojis"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": update_fields}
                )
                modified_count += 1
            
            count += 1
            
            # Afficher la progression
            if count % 1000 == 0:
                print(f"   ✅ {count}/{total_docs} documents traités ({modified_count} modifiés)")
        
        print(f"\n✅ Traitement terminé!")
        print(f"   - Total documents traités: {count}")
        print(f"   - Documents modifiés: {modified_count}")
        
        # Vérification finale
        print("\n🔍 VÉRIFICATION FINALE:")
        
        # Prendre un échantillon de documents modifiés
        sample = collection.find_one({"emojis_supprimes": True})
        if sample:
            print(f"\n📝 EXEMPLE DE DOCUMENT NETTOYÉ:")
            print(f"   - ID: {sample['_id']}")
            print(f"   - Commentaire_Client: {sample.get('Commentaire_Client', 'N/A')[:100]}")
            print(f"   - normalized_arabert: {sample.get('normalized_arabert', 'N/A')[:100]}")
            print(f"   - normalized_full: {sample.get('normalized_full', 'N/A')[:100]}")
        
        # Vérifier s'il reste des emojis
        print("\n🔍 Vérification des emojis restants...")
        remaining_emojis = 0
        for doc in collection.find().limit(1000):
            for field in ["Commentaire_Client", "normalized_arabert", "normalized_full"]:
                text = doc.get(field, "")
                if text and any(ord(c) > 0xFFFF for c in text):
                    remaining_emojis += 1
                    break
        
        if remaining_emojis == 0:
            print("   ✅ Aucun emoji détecté dans l'échantillon de 1000 documents")
        else:
            print(f"   ⚠️ {remaining_emojis} documents avec emojis détectés sur 1000")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("\n🔌 Connexion fermée")

# ═══════════════════════════════════════════════════════════════════════════
# FONCTION DE TEST (pour un seul document)
# ═══════════════════════════════════════════════════════════════════════════

def tester_nettoyage():
    """
    Teste le nettoyage sur un seul document
    """
    print("=" * 80)
    print("🧪 TEST DE NETTOYAGE SUR UN SEUL DOCUMENT")
    print("=" * 80)
    
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db = client["telecom_algerie_new"]
        collection = db["commentaires_normalises"]
        
        # Trouver un document avec des emojis
        test_doc = collection.find_one({
            "Commentaire_Client": {"$regex": "🫩", "$options": "i"}
        })
        
        if not test_doc:
            print("❌ Aucun document avec emoji 🫩 trouvé")
            client.close()
            return
        
        print("\n📝 DOCUMENT AVANT NETTOYAGE:")
        print(f"   ID: {test_doc['_id']}")
        print(f"   Commentaire_Client: {test_doc.get('Commentaire_Client', 'N/A')}")
        print(f"   normalized_arabert: {test_doc.get('normalized_arabert', 'N/A')}")
        print(f"   normalized_full: {test_doc.get('normalized_full', 'N/A')}")
        
        # Nettoyer
        cleaned_comment = clean_commentaire_client(test_doc.get("Commentaire_Client", ""))
        cleaned_arabert = clean_normalized_arabert(test_doc.get("normalized_arabert", ""))
        cleaned_full = clean_normalized_full(test_doc.get("normalized_full", ""))
        
        print("\n📝 DOCUMENT APRÈS NETTOYAGE:")
        print(f"   Commentaire_Client: {cleaned_comment}")
        print(f"   normalized_arabert: {cleaned_arabert}")
        print(f"   normalized_full: {cleaned_full}")
        
        reponse = input("\n✅ Appliquer ce nettoyage à ce document? (oui/non): ")
        
        if reponse.lower() == "oui":
            collection.update_one(
                {"_id": test_doc["_id"]},
                {
                    "$set": {
                        "Commentaire_Client": cleaned_comment,
                        "normalized_arabert": cleaned_arabert,
                        "normalized_full": cleaned_full,
                        "emojis_supprimes": True,
                        "date_nettoyage_emojis": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
            )
            print("✅ Nettoyage appliqué!")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Erreur: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "="*80)
    print("📌 MENU")
    print("="*80)
    print("1. Tester le nettoyage sur un seul document")
    print("2. Nettoyer tous les documents (supprimer tous les emojis)")
    print("="*80)
    
    choix = input("\nVotre choix (1 ou 2): ")
    
    if choix == "1":
        tester_nettoyage()
    elif choix == "2":
        supprimer_emojis_des_colonnes()
    else:
        print("❌ Choix invalide")