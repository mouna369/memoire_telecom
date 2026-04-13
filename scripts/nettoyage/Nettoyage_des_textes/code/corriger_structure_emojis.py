# # # #!/usr/bin/env python3
# # # # -*- coding: utf-8 -*-
# # # """
# # # fix_emojis_columns.py
# # # =====================
# # # Récupère les emojis depuis la collection SOURCE (brute) 
# # # et met à jour les colonnes 'emojis_originaux' et 'emojis_sentiment' 
# # # dans la collection 'commentaires_normalises'.
# # # """

# # # import re
# # # from pymongo import MongoClient, UpdateOne
# # # from configuration2 import MONGO_URI, DB_NAME_ATLAS

# # # # ============================================================
# # # # CONFIGURATION
# # # # ============================================================
# # # COL_SOURCE = "commentaires_bruts"          # Ta collection ORIGINALE (avec emojis)
# # # COL_DEST = "commentaires_normalises"       # Ta collection NETTOYÉE (à corriger)
# # # TEXT_COL_SOURCE = "Commentaire_Client"     # Colonne texte dans la source
# # # TEXT_COL_DEST = "normalized_arabert"       # Colonne texte dans la dest (déjà clean)
# # # BATCH_SIZE = 500

# # # # Dictionnaire simple Emoji -> Sentiment (Adapte-le si tu en as un plus gros)
# # # # Si tu as le master_dict.json, on peut le charger, ici je mets les bases
# # # EMOJI_TO_SENTIMENT = {
# # #     "🔴": "alerte", "❗": "important", "❌": "erreur", "✅": "succès",
# # #     "😡": "colère", "😠": "mécontentement", "😢": "tristesse", "😍": "amour",
# # #     "👍": "approbation", "👎": "désapprobation", "🤔": "réflexion","🤮": "dégoût"
# # #     # Ajoute tous les emojis de ton master_dict.json ici si besoin
# # # }

# # # # Regex pour trouver TOUS les emojis
# # # _EMOJI_RANGES = (
# # #     '\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF'
# # #     '\U0001F1E0-\U0001F1FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF'
# # #     '\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F'
# # #     '\U0001FA70-\U0001FAFF\U0001FB00-\U0001FBFF\u2600-\u26FF\u2700-\u27BF'
# # #     '\u231A-\u231B\u23E9-\u23F3\u23F8-\u23FA\u25AA-\u25AB\u25B6\u25C0'
# # #     '\u25FB-\u25FE\u2614-\u2615\u2648-\u2653\u267F\u2693\u26A1\u26AA-\u26AB'
# # #     '\u26BD-\u26BE\u26C4-\u26C5\u26CE\u26D4\u26EA\u26F2-\u26F3\u26F5\u26FA'
# # #     '\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721'
# # #     '\u2728\u2733-\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763-\u2764'
# # #     '\u2795-\u2797\u27A1\u27B0\u27BF\u2934-\u2935\u2B05-\u2B07\u2B1B-\u2B1C'
# # #     '\u2B50\u2B55\u3030\u303D\u3297\u3299\uFE0F\u200D'
# # # )
# # # EMOJI_REGEX = re.compile(f"[{_EMOJI_RANGES}]+", flags=re.UNICODE)

# # # print("=" * 80)
# # # print("🔧 CORRECTION DES COLONNES EMOJIS MANQUANTES")
# # # print("=" * 80)
# # # print(f"   Source (pour lire les emojis) : {COL_SOURCE}")
# # # print(f"   Destination (à mettre à jour) : {COL_DEST}")
# # # print("=" * 80)

# # # try:
# # #     client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
# # #     client.admin.command('ping')
# # #     db = client[DB_NAME_ATLAS]
    
# # #     coll_source = db[COL_SOURCE]
# # #     coll_dest = db[COL_DEST]
    
# # #     print("✅ Connecté à MongoDB Atlas")

# # #     total_docs = coll_dest.count_documents({})
# # #     print(f"📊 Total documents à vérifier : {total_docs}")

# # #     updates = []
# # #     docs_corrected = 0

# # #     # On parcourt la collection DESTINATION pour trouver ceux qui ont des problèmes
# # #     # Astuce : On cherche ceux où normalized_arabert != Commentaire_Client (s'ils existent encore) 
# # #     # OU simplement on met à jour tout le monde en relisant la source.
# # #     # Pour être sûr, on va lire la SOURCE et mettre à jour la DESTINATION par ID.
    
# # #     print("\n🔄 Lecture de la collection SOURCE pour extraire les emojis...")
    
# # #     cursor = coll_source.find({}, {"_id": 1, TEXT_COL_SOURCE: 1})
    
# # #     for doc in cursor:
# # #         doc_id = doc["_id"]
# # #         text_original = doc.get(TEXT_COL_SOURCE, "")
        
# # #         if not text_original:
# # #             continue

# # #         # 1. Détecter les emojis dans le texte ORIGINAL
# # #         found_emojis = EMOJI_REGEX.findall(text_original)
        
# # #         if found_emojis:
# # #             # Il y a des emojis ! On prépare la mise à jour
            
# # #             # Créer la liste des sentiments correspondants
# # #             sentiments = []
# # #             for e in found_emojis:
# # #                 # Cherche dans le dico, sinon met "emoji" par défaut
# # #                 sent = EMOJI_TO_SENTIMENT.get(e, "emoji")
# # #                 sentiments.append(sent)
            
# # #             # Préparer l'opération de mise à jour
# # #             update_op = UpdateOne(
# # #                 {"_id": doc_id},
# # #                 {
# # #                     "$set": {
# # #                         "emojis_originaux": found_emojis,
# # #                         "emojis_sentiment": sentiments
# # #                         # On ne touche PAS à normalized_arabert, il est déjà clean
# # #                     }
# # #                 }
# # #             )
# # #             updates.append(update_op)
# # #             docs_corrected += 1
            
# # #             if len(updates) >= BATCH_SIZE:
# # #                 result = coll_dest.bulk_write(updates, ordered=False)
# # #                 print(f"   → Lot mis à jour : {result.modified_count} docs")
# # #                 updates = []
# # #         else:
# # #             # Pas d'emojis dans la source non plus ? On s'assure que les champs sont vides proprement
# # #             # (Optionnel, seulement si tu veux uniformiser les données)
# # #             pass

# # #     # Dernier lot
# # #     if updates:
# # #         result = coll_dest.bulk_write(updates, ordered=False)
# # #         print(f"   → Dernier lot : {result.modified_count} docs")

# # #     print("\n" + "=" * 80)
# # #     print("📈 RÉSULTATS DE LA CORRECTION")
# # #     print("=" * 80)
# # #     print(f"   • Documents scannés dans la source : {total_docs}")
# # #     print(f"   • Documents ayant des emojis (corrigés) : {docs_corrected}")
    
# # #     if docs_corrected > 0:
# # #         print(f"   ✅ SUCCÈS : Les colonnes 'emojis_originaux' et 'emojis_sentiment' sont maintenant remplies !")
# # #     else:
# # #         print(f"   ℹ️  Aucun emoji trouvé dans la source (ou déjà tout bon).")

# # #     # Vérification sur l'exemple cité (si possible)
# # #     print("\n🔍 Vérification aléatoire sur un document corrigé...")
# # #     sample = coll_dest.find_one({"emojis_originaux": {"$ne": []}})
# # #     if sample:
# # #         print(f"   • ID: {sample['_id']}")
# # #         print(f"   • Emojis trouvés: {sample.get('emojis_originaux')}")
# # #         print(f"   • Sentiments: {sample.get('emojis_sentiment')}")
# # #         print(f"   • Texte clean: {sample.get('normalized_arabert', '')[:50]}...")
# # #     else:
# # #         print("   (Aucun document avec emojis trouvé pour l'exemple)")

# # #     print("=" * 80)

# # # except Exception as e:
# # #     print(f"\n❌ Erreur critique : {e}")
# # #     import traceback
# # #     traceback.print_exc()

# # # finally:
# # #     if 'client' in locals():
# # #         client.close()
# # #         print("\n🔌 Connexion fermée.")

# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # """
# # supprimer_emojis.py – Supprime tous les emojis des colonnes Commentaire_Client, normalized_arabert, normalized_full
# # """

# # from pymongo import MongoClient
# # from configuration2 import MONGO_URI
# # import re
# # import sys
# # from datetime import datetime

# # # ═══════════════════════════════════════════════════════════════════════════
# # # FONCTIONS DE NETTOYAGE
# # # ═══════════════════════════════════════════════════════════════════════════

# # def remove_emojis(text):
# #     """
# #     Supprime tous les emojis et symboles spéciaux d'un texte
# #     """
# #     if not text or not isinstance(text, str):
# #         return text
    
# #     # Pattern pour détecter les emojis (plage Unicode des emojis)
# #     # Cela couvre la plupart des emojis standards
# #     emoji_pattern = re.compile(
# #         "["
# #         "\U0001F600-\U0001F64F"  # Émoticônes
# #         "\U0001F300-\U0001F5FF"  # Symboles et pictogrammes
# #         "\U0001F680-\U0001F6FF"  # Transport et symboles
# #         "\U0001F700-\U0001F77F"  # Symboles alchimiques
# #         "\U0001F780-\U0001F7FF"  # Formes géométriques
# #         "\U0001F800-\U0001F8FF"  # Flèches supplémentaires
# #         "\U0001F900-\U0001F9FF"  # Emojis supplémentaires
# #         "\U0001FA00-\U0001FA6F"  # Échecs, cartes, etc.
# #         "\U0001FA70-\U0001FAFF"  # Objets divers
# #         "\U00002702-\U000027B0"  # Dingbats
# #         "\U000024C2-\U0001F251" 
# #         "]+",
# #         flags=re.UNICODE
# #     )
    
# #     # Supprimer les emojis
# #     cleaned_text = emoji_pattern.sub('', text)
    
# #     # Nettoyer les espaces multiples
# #     cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    
# #     # Supprimer les espaces au début et à la fin
# #     cleaned_text = cleaned_text.strip()
    
# #     return cleaned_text

# # def clean_commentaire_client(text):
# #     """
# #     Nettoie le champ Commentaire_Client
# #     """
# #     return remove_emojis(text)

# # def clean_normalized_arabert(text):
# #     """
# #     Nettoie le champ normalized_arabert
# #     """
# #     return remove_emojis(text)

# # def clean_normalized_full(text):
# #     """
# #     Nettoie le champ normalized_full
# #     """
# #     return remove_emojis(text)

# # # ═══════════════════════════════════════════════════════════════════════════
# # # FONCTION PRINCIPALE
# # # ═══════════════════════════════════════════════════════════════════════════

# # def supprimer_emojis_des_colonnes():
# #     """
# #     Supprime tous les emojis des colonnes spécifiées
# #     """
    
# #     print("=" * 80)
# #     print("🗑️  SUPPRESSION DES EMOJIS DANS LES 3 COLONNES")
# #     print("=" * 80)
# #     print("Colonnes concernées:")
# #     print("   - Commentaire_Client")
# #     print("   - normalized_arabert")
# #     print("   - normalized_full")
# #     print("=" * 80)
    
# #     try:
# #         # Connexion à MongoDB Atlas
# #         print("\n🔌 Connexion à MongoDB Atlas...")
# #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
# #         client.admin.command('ping')
# #         print("✅ Connexion réussie")
        
# #         db = client["telecom_algerie_new"]
# #         collection = db["commentaires_normalises"]
        
# #         # Vérifier la collection
# #         total_docs = collection.count_documents({})
# #         print(f"\n📊 Total de documents dans la collection: {total_docs}")
        
# #         # Compter les documents qui ont des emojis
# #         print("\n🔍 Recherche des documents avec emojis...")
        
# #         # Échantillon pour vérifier
# #         docs_with_emojis = []
# #         for doc in collection.find().limit(100):
# #             text = doc.get("Commentaire_Client", "")
# #             if any(ord(c) > 0xFFFF for c in text if c):  # Détection simple d'emojis
# #                 docs_with_emojis.append(doc)
        
# #         print(f"📊 Échantillon: {len(docs_with_emojis)} documents avec emojis sur 100 testés")
        
# #         if len(docs_with_emojis) == 0:
# #             print("✅ Aucun emoji trouvé dans les 100 premiers documents!")
# #             reponse = input("\nVoulez-vous quand même continuer la vérification complète? (oui/non): ")
# #             if reponse.lower() != 'oui':
# #                 client.close()
# #                 return
        
# #         # Demander confirmation
# #         print("\n⚠️  ATTENTION: Cette opération va supprimer définitivement tous les emojis")
# #         print("   dans les 3 colonnes de tous les documents.")
# #         reponse = input("\nVoulez-vous continuer? (oui/non): ")
        
# #         if reponse.lower() != 'oui':
# #             print("❌ Opération annulée")
# #             client.close()
# #             return
        
# #         # Traitement par lots
# #         print("\n🔄 Suppression des emojis en cours...")
# #         batch_size = 500
# #         count = 0
# #         modified_count = 0
        
# #         for doc in collection.find():
# #             modified = False
# #             update_fields = {}
            
# #             # Nettoyer Commentaire_Client
# #             if "Commentaire_Client" in doc and doc["Commentaire_Client"]:
# #                 original = doc["Commentaire_Client"]
# #                 cleaned = clean_commentaire_client(original)
# #                 if original != cleaned:
# #                     update_fields["Commentaire_Client"] = cleaned
# #                     modified = True
            
# #             # Nettoyer normalized_arabert
# #             if "normalized_arabert" in doc and doc["normalized_arabert"]:
# #                 original = doc["normalized_arabert"]
# #                 cleaned = clean_normalized_arabert(original)
# #                 if original != cleaned:
# #                     update_fields["normalized_arabert"] = cleaned
# #                     modified = True
            
# #             # Nettoyer normalized_full
# #             if "normalized_full" in doc and doc["normalized_full"]:
# #                 original = doc["normalized_full"]
# #                 cleaned = clean_normalized_full(original)
# #                 if original != cleaned:
# #                     update_fields["normalized_full"] = cleaned
# #                     modified = True
            
# #             # Appliquer les modifications si nécessaire
# #             if modified:
# #                 update_fields["emojis_supprimes"] = True
# #                 update_fields["date_nettoyage_emojis"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
# #                 collection.update_one(
# #                     {"_id": doc["_id"]},
# #                     {"$set": update_fields}
# #                 )
# #                 modified_count += 1
            
# #             count += 1
            
# #             # Afficher la progression
# #             if count % 1000 == 0:
# #                 print(f"   ✅ {count}/{total_docs} documents traités ({modified_count} modifiés)")
        
# #         print(f"\n✅ Traitement terminé!")
# #         print(f"   - Total documents traités: {count}")
# #         print(f"   - Documents modifiés: {modified_count}")
        
# #         # Vérification finale
# #         print("\n🔍 VÉRIFICATION FINALE:")
        
# #         # Prendre un échantillon de documents modifiés
# #         sample = collection.find_one({"emojis_supprimes": True})
# #         if sample:
# #             print(f"\n📝 EXEMPLE DE DOCUMENT NETTOYÉ:")
# #             print(f"   - ID: {sample['_id']}")
# #             print(f"   - Commentaire_Client: {sample.get('Commentaire_Client', 'N/A')[:100]}")
# #             print(f"   - normalized_arabert: {sample.get('normalized_arabert', 'N/A')[:100]}")
# #             print(f"   - normalized_full: {sample.get('normalized_full', 'N/A')[:100]}")
        
# #         # Vérifier s'il reste des emojis
# #         print("\n🔍 Vérification des emojis restants...")
# #         remaining_emojis = 0
# #         for doc in collection.find().limit(1000):
# #             for field in ["Commentaire_Client", "normalized_arabert", "normalized_full"]:
# #                 text = doc.get(field, "")
# #                 if text and any(ord(c) > 0xFFFF for c in text):
# #                     remaining_emojis += 1
# #                     break
        
# #         if remaining_emojis == 0:
# #             print("   ✅ Aucun emoji détecté dans l'échantillon de 1000 documents")
# #         else:
# #             print(f"   ⚠️ {remaining_emojis} documents avec emojis détectés sur 1000")
        
# #     except Exception as e:
# #         print(f"❌ Erreur: {e}")
# #         import traceback
# #         traceback.print_exc()
# #     finally:
# #         client.close()
# #         print("\n🔌 Connexion fermée")

# # # ═══════════════════════════════════════════════════════════════════════════
# # # FONCTION DE TEST (pour un seul document)
# # # ═══════════════════════════════════════════════════════════════════════════

# # def tester_nettoyage():
# #     """
# #     Teste le nettoyage sur un seul document
# #     """
# #     print("=" * 80)
# #     print("🧪 TEST DE NETTOYAGE SUR UN SEUL DOCUMENT")
# #     print("=" * 80)
    
# #     try:
# #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
# #         db = client["telecom_algerie_new"]
# #         collection = db["commentaires_normalises"]
        
# #         # Trouver un document avec des emojis
# #         test_doc = collection.find_one({
# #             "Commentaire_Client": {"$regex": "🫩", "$options": "i"}
# #         })
        
# #         if not test_doc:
# #             print("❌ Aucun document avec emoji 🫩 trouvé")
# #             client.close()
# #             return
        
# #         print("\n📝 DOCUMENT AVANT NETTOYAGE:")
# #         print(f"   ID: {test_doc['_id']}")
# #         print(f"   Commentaire_Client: {test_doc.get('Commentaire_Client', 'N/A')}")
# #         print(f"   normalized_arabert: {test_doc.get('normalized_arabert', 'N/A')}")
# #         print(f"   normalized_full: {test_doc.get('normalized_full', 'N/A')}")
        
# #         # Nettoyer
# #         cleaned_comment = clean_commentaire_client(test_doc.get("Commentaire_Client", ""))
# #         cleaned_arabert = clean_normalized_arabert(test_doc.get("normalized_arabert", ""))
# #         cleaned_full = clean_normalized_full(test_doc.get("normalized_full", ""))
        
# #         print("\n📝 DOCUMENT APRÈS NETTOYAGE:")
# #         print(f"   Commentaire_Client: {cleaned_comment}")
# #         print(f"   normalized_arabert: {cleaned_arabert}")
# #         print(f"   normalized_full: {cleaned_full}")
        
# #         reponse = input("\n✅ Appliquer ce nettoyage à ce document? (oui/non): ")
        
# #         if reponse.lower() == "oui":
# #             collection.update_one(
# #                 {"_id": test_doc["_id"]},
# #                 {
# #                     "$set": {
# #                         "Commentaire_Client": cleaned_comment,
# #                         "normalized_arabert": cleaned_arabert,
# #                         "normalized_full": cleaned_full,
# #                         "emojis_supprimes": True,
# #                         "date_nettoyage_emojis": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# #                     }
# #                 }
# #             )
# #             print("✅ Nettoyage appliqué!")
        
# #         client.close()
        
# #     except Exception as e:
# #         print(f"❌ Erreur: {e}")

# # # ═══════════════════════════════════════════════════════════════════════════
# # # MAIN
# # # ═══════════════════════════════════════════════════════════════════════════

# # if __name__ == "__main__":
# #     print("\n" + "="*80)
# #     print("📌 MENU")
# #     print("="*80)
# #     print("1. Tester le nettoyage sur un seul document")
# #     print("2. Nettoyer tous les documents (supprimer tous les emojis)")
# #     print("="*80)
    
# #     choix = input("\nVotre choix (1 ou 2): ")
    
# #     if choix == "1":
# #         tester_nettoyage()
# #     elif choix == "2":
# #         supprimer_emojis_des_colonnes()
# #     else:
# #         print("❌ Choix invalide")
# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# corriger_emojis_local.py – Corrige les champs emojis dans MongoDB local
# """

# from pymongo import MongoClient
# import re
# from datetime import datetime

# # ============================================================
# # CONFIGURATION (MongoDB local)
# # ============================================================
# MONGO_HOST = 'localhost'
# MONGO_PORT = 27018
# DB_NAME = 'telecom_algerie'          # ← À adapter si différent
# COLLECTION_NAME = 'commentaires_normalises'

# # Regex pour trouver les emojis (large)
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

# # Dictionnaire sentiment (vous pouvez l'enrichir)
# EMOJI_TO_SENTIMENT = {
#     "😪": "نعاس",
#     "😢": "tristesse",
#     "😡": "colère",
#     "😠": "mécontentement",
#     "😍": "amour",
#     "👍": "approbation",
#     "👎": "désapprobation",
#     # Ajoutez les emojis que vous rencontrez
# }

# def corriger_emojis():
#     print("=" * 80)
#     print("🔧 CORRECTION DES EMOJIS (MongoDB local)")
#     print("=" * 80)

#     try:
#         client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=5000)
#         client.admin.command('ping')
#         print("✅ Connecté à MongoDB local")
        
#         db = client[DB_NAME]
#         coll = db[COLLECTION_NAME]

#         # 1. Convertir les chaînes "[]" en tableaux vides
#         print("\n🔄 Étape 1 : Conversion des chaînes '[]' en tableaux vides...")
#         result1 = coll.update_many(
#             {"emojis_originaux": "[]"},
#             {"$set": {"emojis_originaux": []}}
#         )
#         print(f"   → {result1.modified_count} documents corrigés pour emojis_originaux")
        
#         result2 = coll.update_many(
#             {"emojis_sentiment": "[]"},
#             {"$set": {"emojis_sentiment": []}}
#         )
#         print(f"   → {result2.modified_count} documents corrigés pour emojis_sentiment")

#         # 2. Trouver les documents où emojis_originaux est vide (ou absent)
#         #    mais qui contiennent des emojis dans Commentaire_Client
#         print("\n🔄 Étape 2 : Extraction des emojis depuis Commentaire_Client...")
        
#         # On cherche les documents avec au moins un emoji dans le texte ET (emojis_originaux vide ou absent)
#         # Pour éviter de rescanner tous les docs, on utilise une regex côté Python.
#         # Alternative : on parcourt tous les documents (si petite collection) ou on utilise un filtre approximatif.
        
#         cursor = coll.find({
#             "$or": [
#                 {"emojis_originaux": []},
#                 {"emojis_originaux": {"$exists": False}}
#             ]
#         })
        
#         count_total = 0
#         count_modified = 0
#         for doc in cursor:
#             text = doc.get("Commentaire_Client", "")
#             if not text:
#                 continue
#             emojis = EMOJI_REGEX.findall(text)
#             if emojis:
#                 sentiments = [EMOJI_TO_SENTIMENT.get(e, "emoji") for e in emojis]
#                 coll.update_one(
#                     {"_id": doc["_id"]},
#                     {"$set": {
#                         "emojis_originaux": emojis,
#                         "emojis_sentiment": sentiments,
#                         "emojis_corrige_local": True,
#                         "date_correction_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                     }}
#                 )
#                 count_modified += 1
#                 if count_modified % 100 == 0:
#                     print(f"   → {count_modified} documents mis à jour...")
#             count_total += 1
#             if count_total % 1000 == 0:
#                 print(f"   → {count_total} documents parcourus...")
        
#         print(f"\n✅ Extraction terminée : {count_modified} documents enrichis sur {count_total} parcourus")

#         # 3. Vérification
#         print("\n🔍 VÉRIFICATION :")
#         sample = coll.find_one({"emojis_originaux": {"$ne": []}})
#         if sample:
#             print(f"   Exemple : {sample['_id']}")
#             print(f"   Commentaire : {sample.get('Commentaire_Client')}")
#             print(f"   emojis_originaux : {sample.get('emojis_originaux')}")
#             print(f"   emojis_sentiment : {sample.get('emojis_sentiment')}")
#         else:
#             print("   Aucun document avec emojis extraits trouvé.")

#     except Exception as e:
#         print(f"❌ Erreur : {e}")
#         import traceback
#         traceback.print_exc()
#     finally:
#         if 'client' in locals():
#             client.close()
#             print("\n🔌 Connexion fermée")

# if __name__ == "__main__":
#     corriger_emojis()

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# fusion_emojis.py – Correction + extraction + suppression des emojis (MongoDB local)
# Étapes :
#   1. Conversion des chaînes "[]" en tableaux vides
#   2. Extraction des emojis depuis Commentaire_Client vers emojis_originaux / emojis_sentiment
#   3. Suppression des emojis dans les 3 colonnes textuelles
# Tout se fait sur la même collection (par défaut commentaires_normalises).
# """

# from pymongo import MongoClient
# import re
# from datetime import datetime

# # ============================================================
# # CONFIGURATION (MongoDB local)
# # ============================================================
# MONGO_HOST = 'localhost'
# MONGO_PORT = 27018
# DB_NAME = 'telecom_algerie'
# COLLECTION_NAME = 'commentaires_normalises'   # Collection cible

# # Regex pour trouver tous les emojis (identique pour extraction et suppression)
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

# # Dictionnaire sentiment (enrichissable)
# EMOJI_TO_SENTIMENT = {
#     "😪": "نعاس",
#     "😢": "tristesse",
#     "😡": "colère",
#     "😠": "mécontentement",
#     "😍": "amour",
#     "👍": "approbation",
#     "👎": "désapprobation",
# }

# # ============================================================
# # ÉTAPE 1 : Conversion des chaînes "[]" en tableaux vides
# # ============================================================
# def corriger_chaines_vides(coll):
#     print("\n🔄 Étape 1 : Conversion des chaînes '[]' en tableaux vides...")
#     res1 = coll.update_many(
#         {"emojis_originaux": "[]"},
#         {"$set": {"emojis_originaux": []}}
#     )
#     print(f"   → {res1.modified_count} docs corrigés pour emojis_originaux")
#     res2 = coll.update_many(
#         {"emojis_sentiment": "[]"},
#         {"$set": {"emojis_sentiment": []}}
#     )
#     print(f"   → {res2.modified_count} docs corrigés pour emojis_sentiment")

# # ============================================================
# # ÉTAPE 2 : Extraction des emojis depuis Commentaire_Client
# # ============================================================
# def extraire_emojis(coll):
#     print("\n🔄 Étape 2 : Extraction des emojis depuis Commentaire_Client...")
#     cursor = coll.find({
#         "$or": [
#             {"emojis_originaux": []},
#             {"emojis_originaux": {"$exists": False}}
#         ]
#     })
#     count_total = 0
#     count_modified = 0
#     for doc in cursor:
#         text = doc.get("Commentaire_Client", "")
#         if not text:
#             continue
#         emojis = EMOJI_REGEX.findall(text)
#         if emojis:
#             sentiments = [EMOJI_TO_SENTIMENT.get(e, "emoji") for e in emojis]
#             coll.update_one(
#                 {"_id": doc["_id"]},
#                 {"$set": {
#                     "emojis_originaux": emojis,
#                     "emojis_sentiment": sentiments,
#                     "emojis_corrige_local": True,
#                     "date_correction_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 }}
#             )
#             count_modified += 1
#             if count_modified % 100 == 0:
#                 print(f"   → {count_modified} docs mis à jour...")
#         count_total += 1
#         if count_total % 1000 == 0:
#             print(f"   → {count_total} docs parcourus...")
#     print(f"\n✅ Extraction terminée : {count_modified} docs enrichis sur {count_total} parcourus")

# # ============================================================
# # ÉTAPE 3 : Suppression des emojis dans les 3 colonnes textuelles
# # ============================================================
# def supprimer_emojis_texte(coll):
#     print("\n🔄 Étape 3 : Suppression des emojis dans les colonnes texte...")
#     total_docs = coll.count_documents({})
#     print(f"📊 Total documents : {total_docs}")

#     print("\n⚠️  Cette opération va supprimer DÉFINITIVEMENT les emojis")
#     print("   dans les colonnes : Commentaire_Client, normalized_arabert, normalized_full")
#     print("   (les emojis sont déjà sauvegardés dans emojis_originaux/emojis_sentiment)")
#     reponse = input("\nVoulez-vous continuer ? (oui/non) : ")
#     if reponse.lower() != 'oui':
#         print("❌ Annulé.")
#         return

#     modified_count = 0
#     count = 0
#     for doc in coll.find():
#         modified = False
#         update_fields = {}
#         for field in ["Commentaire_Client", "normalized_arabert", "normalized_full"]:
#             original = doc.get(field, "")
#             if original and isinstance(original, str):
#                 cleaned = EMOJI_REGEX.sub('', original)
#                 cleaned = re.sub(r'\s+', ' ', cleaned).strip()
#                 if original != cleaned:
#                     update_fields[field] = cleaned
#                     modified = True
#         if modified:
#             update_fields["emojis_supprimes_du_texte"] = True
#             update_fields["date_suppression_emojis"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             coll.update_one({"_id": doc["_id"]}, {"$set": update_fields})
#             modified_count += 1
#         count += 1
#         if count % 1000 == 0:
#             print(f"   → {count}/{total_docs} docs traités ({modified_count} modifiés)")

#     print(f"\n✅ Suppression terminée : {modified_count} docs modifiés")

# # ============================================================
# # FONCTION PRINCIPALE
# # ============================================================
# def main():
#     print("=" * 80)
#     print("🔧 TRAITEMENT COMPLET DES EMOJIS (correction + extraction + suppression)")
#     print("=" * 80)

#     try:
#         client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=5000)
#         client.admin.command('ping')
#         print("✅ Connecté à MongoDB local")
#         db = client[DB_NAME]
#         coll = db[COLLECTION_NAME]

#         # 1. Corriger les chaînes "[]"
#         corriger_chaines_vides(coll)

#         # 2. Extraire les emojis depuis le texte
#         extraire_emojis(coll)

#         # 3. Supprimer les emojis du texte (avec confirmation)
#         supprimer_emojis_texte(coll)

#         # Exemple de vérification finale
#         sample = coll.find_one({"emojis_originaux": {"$ne": []}})
#         if sample:
#             print("\n📝 EXEMPLE FINAL :")
#             print(f"   Commentaire_Client : {sample.get('Commentaire_Client')}")
#             print(f"   normalized_arabert : {sample.get('normalized_arabert')}")
#             print(f"   normalized_full : {sample.get('normalized_full')}")
#             print(f"   emojis_originaux : {sample.get('emojis_originaux')}")
#             print(f"   emojis_sentiment : {sample.get('emojis_sentiment')}")

#     except Exception as e:
#         print(f"❌ Erreur : {e}")
#         import traceback
#         traceback.print_exc()
#     finally:
#         if 'client' in locals():
#             client.close()
#             print("\n🔌 Connexion fermée")

# if __name__ == "__main__":
#     main()


# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# corriger_emojis_multinode.py – Correction + extraction + suppression des emojis (Spark multi-nœuds)
# Lit depuis une collection source, applique :
#   1. Conversion des chaînes "[]" en tableaux vides
#   2. Extraction des emojis manquants depuis Commentaire_Client
#   3. Suppression des emojis dans les champs texte (Commentaire_Client, normalized_arabert, normalized_full)
# Écrit dans une collection destination.
# """

# from pyspark.sql import SparkSession
# from pymongo import MongoClient, InsertOne
# from pymongo.errors import BulkWriteError
# from datetime import datetime
# import os, time, math, json, re

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# DB_NAME           = "telecom_algerie"
# COLLECTION_SOURCE = "commentaires_sans_emojis"      # Source (après extraction)
# COLLECTION_DEST   = "commentaires_sans_emojis_final" # Destination
# NB_WORKERS        = 3
# SPARK_MASTER      = "spark://spark-master:7077"
# RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_correction_emojis.txt"

# # Regex et dictionnaire pour les emojis
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

# EMOJI_TO_SENTIMENT = {
#     "😪": "نعاس",
#     "😢": "tristesse",
#     "😡": "colère",
#     "😠": "mécontentement",
#     "😍": "amour",
#     "👍": "approbation",
#     "👎": "désapprobation",
# }

# # ============================================================
# # FONCTIONS DE TRAITEMENT
# # ============================================================
# def corriger_emojis_dans_document(doc):
#     """
#     Applique les trois étapes de correction sur un document.
#     Retourne le document modifié.
#     """
#     # 1. Conversion des chaînes "[]" en tableaux vides
#     if doc.get("emojis_originaux") == "[]":
#         doc["emojis_originaux"] = []
#     if doc.get("emojis_sentiment") == "[]":
#         doc["emojis_sentiment"] = []

#     # 2. Extraction des emojis manquants depuis Commentaire_Client
#     if (not doc.get("emojis_originaux") or doc["emojis_originaux"] == []) and "Commentaire_Client" in doc:
#         texte = doc.get("Commentaire_Client", "")
#         if texte and isinstance(texte, str):
#             emojis = EMOJI_REGEX.findall(texte)
#             if emojis:
#                 sentiments = [EMOJI_TO_SENTIMENT.get(e, "emoji") for e in emojis]
#                 doc["emojis_originaux"] = emojis
#                 doc["emojis_sentiment"] = sentiments
#                 doc["emojis_corrige_spark"] = True
#                 doc["date_correction_spark"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#     # 3. Suppression des emojis dans les colonnes textuelles
#     for field in ["Commentaire_Client", "normalized_arabert", "normalized_full"]:
#         if field in doc and isinstance(doc[field], str):
#             original = doc[field]
#             cleaned = EMOJI_REGEX.sub('', original)
#             cleaned = re.sub(r'\s+', ' ', cleaned).strip()
#             if original != cleaned:
#                 doc[field] = cleaned
#                 doc["emojis_supprimes_du_texte"] = True
#                 doc["date_suppression_spark"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#     return doc


# def traiter_partition(partition):
#     """
#     Worker : applique la correction à chaque document et écrit en blocs.
#     """
#     import sys
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient, InsertOne
#     from pymongo.errors import BulkWriteError

#     try:
#         client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
#         db = client[DB_NAME]
#         collection = db[COLLECTION_DEST]
#     except Exception as e:
#         yield {"_erreur": str(e), "statut": "connexion_failed"}
#         return

#     batch = []
#     docs_traites = 0
#     docs_modifies = 0

#     for doc in partition:
#         doc_corrige = corriger_emojis_dans_document(doc)
#         batch.append(InsertOne(doc_corrige))
#         docs_traites += 1
#         if doc_corrige.get("emojis_corrige_spark") or doc_corrige.get("emojis_supprimes_du_texte"):
#             docs_modifies += 1
#         if len(batch) >= 1000:
#             try:
#                 collection.bulk_write(batch, ordered=False)
#             except BulkWriteError:
#                 pass
#             batch = []

#     if batch:
#         try:
#             collection.bulk_write(batch, ordered=False)
#         except BulkWriteError:
#             pass

#     client.close()
#     yield {
#         "docs_traites": docs_traites,
#         "docs_modifies": docs_modifies,
#         "statut": "ok"
#     }


# def lire_partition_depuis_mongo(partition_info):
#     """
#     Lit une partition depuis la collection source.
#     """
#     import sys
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient

#     for item in partition_info:
#         client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
#         db = client[DB_NAME]
#         collection = db[COLLECTION_SOURCE]
#         curseur = collection.find({}, {}).skip(item["skip"]).limit(item["limit"])
#         for doc in curseur:
#             doc["_id"] = str(doc["_id"])
#             yield doc
#         client.close()


# # ============================================================
# # PIPELINE SPARK
# # ============================================================
# temps_debut = time.time()

# print("=" * 70)
# print("✨ CORRECTION DES EMOJIS — Multi-Node Spark")
# print(f"   Source      : {COLLECTION_SOURCE}")
# print(f"   Destination : {COLLECTION_DEST}")
# print("   Opérations :")
# print("   ✅ Conversion '[]' → []")
# print("   ✅ Extraction emojis manquants")
# print("   ✅ Suppression emojis du texte")
# print("=" * 70)

# # 1. Connexion MongoDB Driver
# print("\n📂 Connexion MongoDB (Driver)...")
# client_driver = MongoClient(MONGO_URI_DRIVER)
# db_driver = client_driver[DB_NAME]
# coll_source = db_driver[COLLECTION_SOURCE]
# total_docs = coll_source.count_documents({})
# print(f"✅ {total_docs} documents dans la source")

# # 2. Connexion Spark
# print("\n⚡ Connexion au cluster Spark...")
# temps_spark = time.time()
# spark = SparkSession.builder \
#     .appName("Correction_Emojis_MultiNode") \
#     .master(SPARK_MASTER) \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.sql.shuffle.partitions", "4") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("WARN")
# print(f"✅ Spark connecté en {time.time()-temps_spark:.2f}s")

# # 3. Lecture distribuée
# print("\n📥 LECTURE DISTRIBUÉE...")
# docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# plages = [
#     {"skip": i * docs_par_worker,
#      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
#     for i in range(NB_WORKERS)
# ]
# for idx, p in enumerate(plages):
#     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

# rdd_data = spark.sparkContext \
#     .parallelize(plages, NB_WORKERS) \
#     .mapPartitions(lire_partition_depuis_mongo)

# df_spark = spark.read.json(rdd_data.map(
#     lambda d: json.dumps(
#         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None), list)) else v
#          for k, v in d.items()}
#     )
# ))
# total_lignes = df_spark.count()
# print(f"✅ {total_lignes} documents chargés")

# # 4. Vider destination
# coll_dest = db_driver[COLLECTION_DEST]
# coll_dest.delete_many({})
# print("\n🧹 Collection destination vidée")

# # 5. Traitement + écriture distribuée
# print("\n💾 CORRECTION + ÉCRITURE DISTRIBUÉE...")
# temps_traitement = time.time()

# rdd_stats = df_spark.rdd \
#     .map(lambda row: row.asDict()) \
#     .mapPartitions(traiter_partition)

# stats = rdd_stats.collect()
# total_traites = sum(s.get("docs_traites", 0) for s in stats if s.get("statut") == "ok")
# total_modifies = sum(s.get("docs_modifies", 0) for s in stats if s.get("statut") == "ok")
# erreurs = [s for s in stats if "_erreur" in s]

# print(f"✅ Traitement terminé en {time.time()-temps_traitement:.2f}s")
# if erreurs:
#     for e in erreurs:
#         print(f"   ⚠️  {e.get('_erreur')}")

# # 6. Vérification finale
# print("\n🔎 VÉRIFICATION FINALE...")
# total_en_dest = coll_dest.count_documents({})
# succes = total_en_dest == total_traites

# print(f"   ┌──────────────────────────────────────────────────┐")
# print(f"   │ Documents source          : {total_lignes:<20} │")
# print(f"   │ Documents traités         : {total_traites:<20} │")
# print(f"   │ Documents modifiés        : {total_modifies:<20} │")
# print(f"   │ Documents destination     : {total_en_dest:<20} │")
# print(f"   │ Statut : {'✅ SUCCÈS TOTAL !':<38} │")
# print(f"   └──────────────────────────────────────────────────┘")

# # 7. Exemple
# print("\n📋 EXEMPLE DE DOCUMENT CORRIGÉ :")
# exemple = coll_dest.find_one({"emojis_originaux": {"$ne": []}})
# if exemple:
#     print(f"   Commentaire_Client : {exemple.get('Commentaire_Client', '')[:80]}...")
#     print(f"   emojis_originaux   : {exemple.get('emojis_originaux', [])}")
#     print(f"   emojis_sentiment   : {exemple.get('emojis_sentiment', [])}")

# # 8. Rapport
# temps_total = time.time() - temps_debut
# rapport = f"""
# {"="*70}
# RAPPORT — CORRECTION DES EMOJIS (Multi-Node Spark)
# {"="*70}
# Date        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Spark 4.1.1 | Multi-Node | {NB_WORKERS} Workers
# Collection  : {DB_NAME}.{COLLECTION_SOURCE} → {COLLECTION_DEST}

# TRAITEMENT :
#    • Conversion '[]' → [] dans emojis_originaux / emojis_sentiment
#    • Extraction emojis manquants depuis Commentaire_Client
#    • Suppression des emojis dans les champs texte

# RÉSULTATS :
#    • Total source      : {total_lignes}
#    • Total destination : {total_en_dest}
#    • Documents modifiés: {total_modifies}

# TEMPS :
#    • Total    : {temps_total:.2f}s
#    • Vitesse  : {total_lignes/temps_total:.0f} docs/s

# STOCKAGE :
#    • Source      : {DB_NAME}.{COLLECTION_SOURCE}
#    • Destination : {DB_NAME}.{COLLECTION_DEST}
#    • Statut      : {'✅ SUCCÈS' if succes else '⚠️ VÉRIFIER'}
# {"="*70}
# """

# os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
#     f.write(rapport)

# spark.stop()
# client_driver.close()

# print(f"\n✅ Rapport : {RAPPORT_PATH}")
# print("\n" + "="*70)
# print("📊 RÉSUMÉ FINAL")
# print("="*70)
# print(f"   📥 Documents source      : {total_lignes}")
# print(f"   📤 Documents destination : {total_en_dest}")
# print(f"   🔧 Documents modifiés    : {total_modifies}")
# print(f"   ⏱️  Temps total           : {temps_total:.2f}s")
# print(f"   🚀 Vitesse               : {total_lignes/temps_total:.0f} docs/s")
# print(f"   📁 Destination           : {DB_NAME}.{COLLECTION_DEST}")
# print("="*70)
# print("🎉 CORRECTION DES EMOJIS TERMINÉE EN MODE MULTI-NŒUDS !")


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
corriger_emojis_multinode.py – Correction + extraction + suppression des emojis (Spark multi-nœuds)
Lit depuis une collection source, applique les corrections et remplace la collection source par le résultat.
(Utilise une collection temporaire, puis renomme.)
"""

from pyspark.sql import SparkSession
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime
import os, time, math, json, re

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_normalises_tfidf"      # Source (à corriger)
COLLECTION_TEMP   = "_temp_emojis_correction"       # Collection temporaire
NB_WORKERS        = 3
SPARK_MASTER      = "spark://spark-master:7077"
RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_correction_emojis.txt"

# Regex et dictionnaire pour les emojis (inchangés)
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

EMOJI_TO_SENTIMENT = {
    "😪": "نعاس",
    "😢": "tristesse",
    "😡": "colère",
    "😠": "mécontentement",
    "😍": "amour",
    "👍": "approbation",
    "👎": "désapprobation",
}

# ============================================================
# FONCTIONS DE TRAITEMENT (inchangées)
# ============================================================
def corriger_emojis_dans_document(doc):
    if doc.get("emojis_originaux") == "[]":
        doc["emojis_originaux"] = []
    if doc.get("emojis_sentiment") == "[]":
        doc["emojis_sentiment"] = []

    if (not doc.get("emojis_originaux") or doc["emojis_originaux"] == []) and "Commentaire_Client" in doc:
        texte = doc.get("Commentaire_Client", "")
        if texte and isinstance(texte, str):
            emojis = EMOJI_REGEX.findall(texte)
            if emojis:
                sentiments = [EMOJI_TO_SENTIMENT.get(e, "emoji") for e in emojis]
                doc["emojis_originaux"] = emojis
                doc["emojis_sentiment"] = sentiments
                doc["emojis_corrige_spark"] = True
                doc["date_correction_spark"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for field in ["Commentaire_Client", "normalized_arabert", "normalized_full"]:
        if field in doc and isinstance(doc[field], str):
            original = doc[field]
            cleaned = EMOJI_REGEX.sub('', original)
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            if original != cleaned:
                doc[field] = cleaned
                doc["emojis_supprimes_du_texte"] = True
                doc["date_suppression_spark"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return doc

def traiter_partition(partition):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError

    try:
        client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_TEMP]   # ← écriture dans la collection temporaire
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed"}
        return

    batch = []
    docs_traites = 0
    docs_modifies = 0

    for doc in partition:
        doc_corrige = corriger_emojis_dans_document(doc)
        batch.append(InsertOne(doc_corrige))
        docs_traites += 1
        if doc_corrige.get("emojis_corrige_spark") or doc_corrige.get("emojis_supprimes_du_texte"):
            docs_modifies += 1
        if len(batch) >= 1000:
            try:
                collection.bulk_write(batch, ordered=False)
            except BulkWriteError:
                pass
            batch = []

    if batch:
        try:
            collection.bulk_write(batch, ordered=False)
        except BulkWriteError:
            pass

    client.close()
    yield {
        "docs_traites": docs_traites,
        "docs_modifies": docs_modifies,
        "statut": "ok"
    }

def lire_partition_depuis_mongo(partition_info):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_SOURCE]   # lecture depuis la source
        curseur = collection.find({}, {}).skip(item["skip"]).limit(item["limit"])
        for doc in curseur:
            doc["_id"] = str(doc["_id"])
            yield doc
        client.close()

# ============================================================
# PIPELINE SPARK + REMPLACEMENT DE LA COLLECTION SOURCE
# ============================================================
temps_debut = time.time()

print("=" * 70)
print("✨ CORRECTION DES EMOJIS — Multi-Node Spark (remplacement de la source)")
print(f"   Source      : {DB_NAME}.{COLLECTION_SOURCE}")
print(f"   Temporaire  : {DB_NAME}.{COLLECTION_TEMP}")
print("   Opérations :")
print("   ✅ Conversion '[]' → []")
print("   ✅ Extraction emojis manquants")
print("   ✅ Suppression emojis du texte")
print("=" * 70)

# 1. Connexion MongoDB Driver
print("\n📂 Connexion MongoDB (Driver)...")
client_driver = MongoClient(MONGO_URI_DRIVER)
db_driver = client_driver[DB_NAME]
coll_source = db_driver[COLLECTION_SOURCE]
total_docs = coll_source.count_documents({})
print(f"✅ {total_docs} documents dans la source")

# 2. Connexion Spark
print("\n⚡ Connexion au cluster Spark...")
temps_spark = time.time()
spark = SparkSession.builder \
    .appName("Correction_Emojis_MultiNode") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(f"✅ Spark connecté en {time.time()-temps_spark:.2f}s")

# 3. Lecture distribuée
print("\n📥 LECTURE DISTRIBUÉE...")
docs_par_worker = math.ceil(total_docs / NB_WORKERS)
plages = [
    {"skip": i * docs_par_worker,
     "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
    for i in range(NB_WORKERS)
]
for idx, p in enumerate(plages):
    print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

rdd_data = spark.sparkContext \
    .parallelize(plages, NB_WORKERS) \
    .mapPartitions(lire_partition_depuis_mongo)

df_spark = spark.read.json(rdd_data.map(
    lambda d: json.dumps(
        {k: str(v) if not isinstance(v, (str, int, float, bool, type(None), list)) else v
         for k, v in d.items()}
    )
))
total_lignes = df_spark.count()
print(f"✅ {total_lignes} documents chargés")

# 4. Vider la collection temporaire si elle existe
coll_temp = db_driver[COLLECTION_TEMP]
coll_temp.delete_many({})
print("\n🧹 Collection temporaire vidée")

# 5. Traitement + écriture dans la collection temporaire
print("\n💾 CORRECTION + ÉCRITURE (dans temporaire)...")
temps_traitement = time.time()

rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(traiter_partition)

stats = rdd_stats.collect()
total_traites = sum(s.get("docs_traites", 0) for s in stats if s.get("statut") == "ok")
total_modifies = sum(s.get("docs_modifies", 0) for s in stats if s.get("statut") == "ok")
erreurs = [s for s in stats if "_erreur" in s]

print(f"✅ Traitement terminé en {time.time()-temps_traitement:.2f}s")
if erreurs:
    for e in erreurs:
        print(f"   ⚠️  {e.get('_erreur')}")

# 6. Vérification dans la temporaire
total_temp = coll_temp.count_documents({})
print(f"📊 Documents dans la temporaire : {total_temp}")

# 7. Remplacer la collection source par la temporaire
print("\n🔄 Remplacement de la collection source...")
db_driver[COLLECTION_SOURCE].drop()
print(f"   Ancienne collection '{COLLECTION_SOURCE}' supprimée.")
coll_temp.rename(COLLECTION_SOURCE)
print(f"   Collection temporaire renommée en '{COLLECTION_SOURCE}'.")

# 8. Vérification finale
total_final = db_driver[COLLECTION_SOURCE].count_documents({})
succes = total_final == total_traites

print(f"\n🔎 VÉRIFICATION FINALE...")
print(f"   ┌──────────────────────────────────────────────────┐")
print(f"   │ Documents source (originaux) : {total_lignes:<20} │")
print(f"   │ Documents traités            : {total_traites:<20} │")
print(f"   │ Documents modifiés           : {total_modifies:<20} │")
print(f"   │ Documents finaux             : {total_final:<20} │")
print(f"   │ Statut : {'✅ SUCCÈS TOTAL !':<38} │")
print(f"   └──────────────────────────────────────────────────┘")

# 9. Exemple
print("\n📋 EXEMPLE DE DOCUMENT CORRIGÉ :")
exemple = db_driver[COLLECTION_SOURCE].find_one({"emojis_originaux": {"$ne": []}})
if exemple:
    print(f"   Commentaire_Client : {exemple.get('Commentaire_Client', '')[:80]}...")
    print(f"   emojis_originaux   : {exemple.get('emojis_originaux', [])}")
    print(f"   emojis_sentiment   : {exemple.get('emojis_sentiment', [])}")

# 10. Rapport
temps_total = time.time() - temps_debut
rapport = f"""
{"="*70}
RAPPORT — CORRECTION DES EMOJIS (Multi-Node Spark) — REMPLACEMENT SOURCE
{"="*70}
Date        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Spark 4.1.1 | Multi-Node | {NB_WORKERS} Workers
Collection  : {DB_NAME}.{COLLECTION_SOURCE} (remplacée)

TRAITEMENT :
   • Conversion '[]' → [] dans emojis_originaux / emojis_sentiment
   • Extraction emojis manquants depuis Commentaire_Client
   • Suppression des emojis dans les champs texte

RÉSULTATS :
   • Total source originaux : {total_lignes}
   • Total finaux           : {total_final}
   • Documents modifiés     : {total_modifies}

TEMPS :
   • Total    : {temps_total:.2f}s
   • Vitesse  : {total_lignes/temps_total:.0f} docs/s

STOCKAGE :
   • Collection finale : {DB_NAME}.{COLLECTION_SOURCE}
   • Statut           : {'✅ SUCCÈS' if succes else '⚠️ VÉRIFIER'}
{"="*70}
"""

os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
    f.write(rapport)

spark.stop()
client_driver.close()

print(f"\n✅ Rapport : {RAPPORT_PATH}")
print("\n" + "="*70)
print("📊 RÉSUMÉ FINAL")
print("="*70)
print(f"   📥 Documents source      : {total_lignes}")
print(f"   📤 Documents finaux      : {total_final}")
print(f"   🔧 Documents modifiés    : {total_modifies}")
print(f"   ⏱️  Temps total           : {temps_total:.2f}s")
print(f"   🚀 Vitesse               : {total_lignes/temps_total:.0f} docs/s")
print(f"   📁 Collection finale     : {DB_NAME}.{COLLECTION_SOURCE}")
print("="*70)
print("🎉 CORRECTION DES EMOJIS TERMINÉE — SOURCE REMPLACÉE !")