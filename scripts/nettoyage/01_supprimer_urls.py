# # # # scripts/nettoyage/01_supprimer_urls.py - VERSION AVEC CSV
# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import col, regexp_replace, when
# # # import pandas as pd
# # # import re

# # # def supprimer_urls(texte):
# # #     """Supprime les URLs d'un texte"""
# # #     if pd.isna(texte):
# # #         return ""
# # #     texte = str(texte)
# # #     pattern = r'http[s]?://\S+|www\.\S+'
# # #     return re.sub(pattern, '', texte)

# # # print("="*60)
# # # print("🔍 ÉTAPE 1 : DÉTECTION ET SUPPRESSION DES URLS")
# # # print("="*60)

# # # # 1. Créer Spark
# # # spark = SparkSession.builder \
# # #     .appName("Suppression_URLs") \
# # #     .master("local[*]") \
# # #     .getOrCreate()
# # # print("✅ Spark démarré")

# # # # 2. Charger les données depuis l'Excel directement
# # # print("\n📂 Chargement depuis Excel...")
# # # pandas_df = pd.read_excel("donnees/brutes/Social-Media-Analytics1.xlsx", header=1)
# # # print(f"✅ {len(pandas_df)} commentaires chargés")

# # # # 3. Identifier la colonne des commentaires
# # # colonne_commentaire = None
# # # for col_name in pandas_df.columns:
# # #     if 'commentaire' in str(col_name).lower():
# # #         colonne_commentaire = col_name
# # #         break

# # # if colonne_commentaire is None:
# # #     colonne_commentaire = pandas_df.columns[0]
# # #     print(f"⚠️ Colonne non trouvée, utilisation de: {colonne_commentaire}")
# # # else:
# # #     print(f"📋 Colonne analysée : {colonne_commentaire}")

# # # # 4. ANALYSE : Détecter les URLs
# # # print("\n🔎 ANALYSE : Recherche des URLs...")

# # # # Fonction pour détecter les URLs
# # # def detecter_urls_texte(texte):
# # #     if pd.isna(texte):
# # #         return 0, ""
# # #     texte = str(texte)
# # #     pattern = r'http[s]?://\S+|www\.\S+'
# # #     urls = re.findall(pattern, texte)
# # #     return len(urls), " | ".join(urls)

# # # # Appliquer la détection
# # # urls_info = pandas_df[colonne_commentaire].apply(detecter_urls_texte)
# # # pandas_df['nb_urls'] = urls_info.apply(lambda x: x[0])
# # # pandas_df['urls_trouvees'] = urls_info.apply(lambda x: x[1])

# # # # Compter les commentaires avec URLs
# # # nb_total = len(pandas_df)
# # # nb_avec_urls = (pandas_df['nb_urls'] > 0).sum()
# # # nb_sans_urls = nb_total - nb_avec_urls
# # # pourcentage = (nb_avec_urls / nb_total * 100) if nb_total > 0 else 0

# # # print(f"\n📊 STATISTIQUES DES URLS:")
# # # print(f"   - Total commentaires : {nb_total}")
# # # print(f"   - Commentaires avec URLs : {nb_avec_urls} ({pourcentage:.2f}%)")
# # # print(f"   - Commentaires sans URLs : {nb_sans_urls}")

# # # # 5. AFFICHER les commentaires avec URLs
# # # if nb_avec_urls > 0:
# # #     print(f"\n📝 COMMENTAIRES AVEC URLS TROUVÉS:")
# # #     df_urls = pandas_df[pandas_df['nb_urls'] > 0]
    
# # #     for idx, row in df_urls.head(10).iterrows():
# # #         print(f"\n   Ligne {idx + 2}:")
# # #         print(f"   Texte: {row[colonne_commentaire][:100]}...")
# # #         print(f"   URLs: {row['urls_trouvees']}")

# # # # 6. SUPPRESSION des URLs
# # # print("\n🧹 SUPPRESSION DES URLS...")
# # # pandas_df['commentaire_sans_urls'] = pandas_df[colonne_commentaire].apply(supprimer_urls)

# # # # 7. VÉRIFICATION
# # # print("🔎 VÉRIFICATION...")

# # # # Compter s'il reste des URLs
# # # pandas_df['verification_urls'] = pandas_df['commentaire_sans_urls'].apply(
# # #     lambda x: len(re.findall(r'http[s]?://\S+|www\.\S+', str(x)))
# # # )
# # # nb_reste = (pandas_df['verification_urls'] > 0).sum()

# # # if nb_reste == 0:
# # #     print("✅ SUCCÈS : Toutes les URLs ont été supprimées !")
# # # else:
# # #     print(f"⚠️ ATTENTION : Il reste {nb_reste} commentaires avec URLs")

# # # # 8. CRÉER LE FICHIER FINAL (MÊME STRUCTURE QUE L'ORIGINAL)
# # # print("\n💾 Création du fichier CSV final...")

# # # # Garder les mêmes colonnes que l'original + la version nettoyée
# # # colonnes_a_garder = list(pandas_df.columns)
# # # # Enlever les colonnes temporaires
# # # colonnes_temp = ['nb_urls', 'urls_trouvees', 'verification_urls']
# # # colonnes_finales = [c for c in colonnes_a_garder if c not in colonnes_temp]

# # # # Créer le DataFrame final avec la colonne originale remplacée par la version nettoyée
# # # df_final = pandas_df.copy()
# # # df_final[colonne_commentaire] = df_final['commentaire_sans_urls']
# # # df_final = df_final[colonnes_finales]

# # # # Sauvegarder en CSV
# # # csv_path = "donnees/resultats/donnees_sans_urls.csv"
# # # df_final.to_csv(csv_path, index=False, encoding='utf-8-sig')
# # # print(f"✅ Fichier CSV créé : {csv_path}")

# # # # 9. CRÉER UN RAPPORT
# # # print("\n📄 Création du rapport...")
# # # with open("donnees/resultats/rapport_urls.txt", "w", encoding="utf-8") as f:
# # #     f.write("="*60 + "\n")
# # #     f.write("RAPPORT DE DÉTECTION ET SUPPRESSION DES URLS\n")
# # #     f.write("="*60 + "\n\n")
# # #     f.write(f"Date : 2024-02-23\n")
# # #     f.write(f"Fichier source : Social-Media-Analytics.xlsx\n")
# # #     f.write(f"Total commentaires : {nb_total}\n")
# # #     f.write(f"Commentaires avec URLs : {nb_avec_urls}\n")
# # #     f.write(f"Pourcentage : {pourcentage:.2f}%\n")
# # #     f.write(f"URLs supprimées avec succès : {'OUI' if nb_reste==0 else 'NON'}\n")
# # #     f.write(f"\nFichier créé : donnees_sans_urls.csv\n")

# # # print("✅ Rapport sauvegardé")

# # # # 10. EXEMPLES AVANT/APRÈS
# # # print("\n📊 EXEMPLES AVANT/APRÈS SUPPRESSION:")
# # # if nb_avec_urls > 0:
# # #     exemples = pandas_df[pandas_df['nb_urls'] > 0].head(3)
# # #     for idx, row in exemples.iterrows():
# # #         print(f"\n   AVANT: {row[colonne_commentaire][:100]}...")
# # #         print(f"   APRÈS: {row['commentaire_sans_urls'][:100]}...")
# # # else:
# # #     print("   Aucun exemple avec URLs")

# # # print("\n" + "="*60)
# # # print("📊 RÉSUMÉ FINAL")
# # # print("="*60)
# # # print(f"✅ {nb_avec_urls} commentaires avec URLs ont été traités")
# # # print(f"✅ Fichier créé : donnees/resultats/donnees_sans_urls.csv")
# # # print(f"✅ Tu peux l'ouvrir avec Excel ou n'importe quel éditeur")
# # # print("="*60)

# # # print("\n🎉 ÉTAPE 1 TERMINÉE !")


# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-

# # # scripts/nettoyage/01_supprimer_urls.py - VERSION AVEC STOCKAGE MONGODB

# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, udf
# # from pyspark.sql.types import StringType, IntegerType
# # import pandas as pd
# # import re
# # import os
# # from pymongo import MongoClient
# # from datetime import datetime
# # from bson import ObjectId

# # def supprimer_urls(texte):
# #     """Supprime les URLs d'un texte"""
# #     if texte is None or not isinstance(texte, str):
# #         return texte
# #     # Pattern pour détecter les URLs
# #     pattern = r'http[s]?://\S+|www\.\S+'
# #     texte_propre = re.sub(pattern, '', texte)
# #     # Supprimer les espaces multiples
# #     texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
# #     return texte_propre if texte_propre else None

# # def detecter_urls(texte):
# #     """Détecte si un texte contient des URLs"""
# #     if texte is None or not isinstance(texte, str):
# #         return 0
# #     pattern = r'http[s]?://\S+|www\.\S+'
# #     return 1 if re.search(pattern, texte) else 0

# # def extraire_urls(texte):
# #     """Extrait toutes les URLs d'un texte"""
# #     if texte is None or not isinstance(texte, str):
# #         return []
# #     pattern = r'http[s]?://\S+|www\.\S+'
# #     return re.findall(pattern, texte)

# # print("="*70)
# # print("🔍 ÉTAPE 1 : DÉTECTION ET SUPPRESSION DES URLS")
# # print("="*70)

# # # 1. Créer Spark
# # print("\n⚡ Démarrage de Spark...")
# # spark = SparkSession.builder \
# #     .appName("Suppression_URLs") \
# #     .master("local[*]") \
# #     .config("spark.executor.memory", "4g") \
# #     .config("spark.driver.memory", "4g") \
# #     .getOrCreate()
# # print("✅ Spark démarré")

# # # 2. Connexion à MongoDB
# # print("\n📂 Connexion à MongoDB...")
# # try:
# #     client = MongoClient('localhost', 27018)
# #     db = client['telecom_algerie']
    
# #     # Collection source
# #     collection_source = db['commentaires_bruts']
    
# #     # Collection destination (nettoyée)
# #     collection_dest = db['commentaires_sans_urls']
    
# #     # Vider la collection de destination si elle existe
# #     collection_dest.delete_many({})
    
# #     print("✅ Connexion MongoDB réussie")
    
# # except Exception as e:
# #     print(f"❌ Erreur de connexion MongoDB: {e}")
# #     spark.stop()
# #     exit(1)

# # # 3. Charger les données
# # print("\n📥 Chargement des commentaires...")
# # data = list(collection_source.find({}))
# # print(f"📊 {len(data)} commentaires chargés")

# # if len(data) == 0:
# #     print("❌ Aucune donnée trouvée")
# #     spark.stop()
# #     exit(1)

# # # 4. ANALYSE : Détecter les URLs
# # print("\n🔎 ANALYSE : Recherche des URLs...")

# # total_avec_urls = 0
# # total_urls_trouvees = 0
# # exemples_urls = []

# # for doc in data[:10]:  # Seulement pour l'affichage des exemples
# #     commentaire = doc.get('Commentaire_Client', '')
# #     urls = extraire_urls(commentaire)
# #     if urls:
# #         exemples_urls.append({
# #             'texte': commentaire[:150],
# #             'urls': urls
# #         })

# # # Compter tous les URLs
# # for doc in data:
# #     commentaire = doc.get('Commentaire_Client', '')
# #     urls = extraire_urls(commentaire)
# #     if urls:
# #         total_avec_urls += 1
# #         total_urls_trouvees += len(urls)

# # print(f"\n📊 STATISTIQUES:")
# # print(f"   ┌────────────────────────────────────┐")
# # print(f"   │ Total commentaires    : {len(data):<15} │")
# # print(f"   │ Avec URLs             : {total_avec_urls:<15} │")
# # print(f"   │ URLs trouvées         : {total_urls_trouvees:<15} │")
# # print(f"   │ Pourcentage           : {(total_avec_urls/len(data)*100):<15.2f}% │")
# # print(f"   └────────────────────────────────────┘")

# # # Afficher des exemples
# # if exemples_urls:
# #     print("\n📝 EXEMPLES DE COMMENTAIRES AVEC URLS:")
# #     for i, ex in enumerate(exemples_urls[:5], 1):
# #         print(f"\n   Exemple {i}:")
# #         print(f"   📍 Texte: {ex['texte']}...")
# #         print(f"   🔗 URLs: {', '.join(ex['urls'])}")
# #         print("   " + "-" * 60)

# # # 5. NETTOYAGE : Supprimer les URLs
# # print("\n🧹 SUPPRESSION DES URLS EN COURS...")

# # docs_nettoyes = []
# # docs_avec_modifications = 0

# # for i, doc in enumerate(data):
# #     # Créer une copie du document
# #     doc_propre = doc.copy()
    
# #     # Nettoyer le commentaire client
# #     commentaire_original = doc.get('Commentaire_Client', '')
# #     commentaire_nettoye = supprimer_urls(commentaire_original)
    
# #     if commentaire_original != commentaire_nettoye:
# #         docs_avec_modifications += 1
    
# #     doc_propre['Commentaire_Client'] = commentaire_nettoye
    
# #     # Nettoyer le commentaire moderateur s'il existe
# #     if 'commentaire_moderateur' in doc:
# #         mod_original = doc.get('commentaire_moderateur', '')
# #         mod_nettoye = supprimer_urls(mod_original)
# #         doc_propre['commentaire_moderateur'] = mod_nettoye
    
# #     # Ajouter des métadonnées de nettoyage
# #     doc_propre['_nettoyage'] = {
# #         'date_nettoyage': datetime.now(),
# #         'etape': 'suppression_urls',
# #         'urls_supprimees': len(extraire_urls(commentaire_original)) > 0,
# #         'nb_urls_trouvees': len(extraire_urls(commentaire_original))
# #     }
    
# #     docs_nettoyes.append(doc_propre)
    
# #     # Afficher la progression
# #     if (i + 1) % 5000 == 0:
# #         print(f"   ✓ {i + 1}/{len(data)} documents traités")

# # print(f"\n✅ Traitement terminé: {len(docs_nettoyes)} documents")
# # print(f"   • Documents modifiés: {docs_avec_modifications}")

# # # 6. SAUVEGARDE DANS MONGODB
# # print("\n💾 SAUVEGARDE DANS MongoDB...")

# # try:
# #     # Insérer par lots de 1000 pour éviter les timeout
# #     batch_size = 1000
# #     for i in range(0, len(docs_nettoyes), batch_size):
# #         batch = docs_nettoyes[i:i+batch_size]
# #         collection_dest.insert_many(batch)
# #         print(f"   ✓ Lot {i//batch_size + 1}: {len(batch)} documents sauvegardés")
    
# #     print(f"\n✅ {len(docs_nettoyes)} documents sauvegardés dans 'commentaires_sans_urls'")
    
# # except Exception as e:
# #     print(f"❌ Erreur lors de la sauvegarde: {e}")

# # # 7. VÉRIFICATION
# # print("\n🔎 VÉRIFICATION DE LA SUPPRESSION...")

# # # Vérifier dans la nouvelle collection
# # echantillon = collection_dest.find().limit(5)
# # urls_restantes = 0
# # total_verif = collection_dest.count_documents({})

# # print(f"\n📊 Vérification sur {total_verif} documents:")

# # # Vérifier quelques documents
# # for doc in collection_dest.find().limit(20):
# #     commentaire = doc.get('Commentaire_Client', '')
# #     if commentaire and extraire_urls(commentaire):
# #         urls_restantes += 1
# #         print(f"   ⚠️ URL restante trouvée: {commentaire[:100]}...")

# # if urls_restantes == 0:
# #     print("   ✅ Aucune URL restante dans l'échantillon vérifié")
# # else:
# #     print(f"   ⚠️ {urls_restantes} URLs restantes trouvées")

# # # 8. CRÉER UN RAPPORT
# # print("\n📄 CRÉATION DU RAPPORT...")

# # rapport = f"""
# # {"="*70}
# # RAPPORT DE SUPPRESSION DES URLS
# # {"="*70}

# # Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

# # 📊 STATISTIQUES:
# #    • Total commentaires traités : {len(data)}
# #    • Commentaires avec URLs     : {total_avec_urls}
# #    • URLs trouvées              : {total_urls_trouvees}
# #    • Pourcentage avec URLs      : {total_avec_urls/len(data)*100:.2f}%
# #    • Documents modifiés         : {docs_avec_modifications}

# # 📁 STOCKAGE:
# #    • Collection source      : telecom_algerie.commentaires_bruts
# #    • Collection destination : telecom_algerie.commentaires_sans_urls
# #    • Documents sauvegardés  : {len(docs_nettoyes)}

# # 🔍 EXEMPLES D'URLS SUPPRIMÉES:
# # """

# # # Ajouter des exemples au rapport
# # for i, ex in enumerate(exemples_urls[:5]):
# #     rapport += f"\n   {i+1}. URLs: {', '.join(ex['urls'])}"
# #     rapport += f"\n      Texte: {ex['texte'][:100]}...\n"

# # # Sauvegarder le rapport
# # rapport_path = "donnees/resultats/rapport_urls.txt"
# # os.makedirs("donnees/resultats", exist_ok=True)
# # with open(rapport_path, "w", encoding="utf-8") as f:
# #     f.write(rapport)

# # print(f"✅ Rapport sauvegardé: {rapport_path}")

# # # 9. EXPORT OPTIONNEL EN CSV/EXCEL (si vraiment nécessaire)
# # print("\n📁 EXPORT OPTIONNEL EN CSV/EXCEL...")

# # reponse = input("\nVoulez-vous aussi exporter en CSV/Excel ? (o/n): ")
# # if reponse.lower() == 'o':
# #     try:
# #         # Récupérer quelques documents pour l'export
# #         docs_export = list(collection_dest.find().limit(1000))
        
# #         # Convertir ObjectId en string
# #         for doc in docs_export:
# #             doc['_id'] = str(doc['_id'])
# #             if '_nettoyage' in doc:
# #                 doc['_nettoyage'] = str(doc['_nettoyage'])
        
# #         # Créer DataFrame
# #         df_export = pd.DataFrame(docs_export)
        
# #         # Exporter
# #         csv_path = "donnees/resultats/commentaires_sans_urls.csv"
# #         excel_path = "donnees/resultats/commentaires_sans_urls.xlsx"
        
# #         df_export.to_csv(csv_path, index=False, encoding='utf-8-sig')
# #         df_export.to_excel(excel_path, index=False)
        
# #         print(f"✅ CSV exporté: {csv_path} (1000 premières lignes)")
# #         print(f"✅ Excel exporté: {excel_path} (1000 premières lignes)")
        
# #     except Exception as e:
# #         print(f"❌ Erreur lors de l'export: {e}")

# # # 10. RÉSUMÉ FINAL
# # print("\n" + "="*70)
# # print("📊 RÉSUMÉ FINAL")
# # print("="*70)
# # print(f"📥 Commentaires traités    : {len(data)}")
# # print(f"🔗 URLs détectées          : {total_urls_trouvees}")
# # print(f"📝 Commentaires avec URLs  : {total_avec_urls}")
# # print(f"✅ Documents modifiés      : {docs_avec_modifications}")
# # print(f"\n📁 Base de données MongoDB:")
# # print(f"   • Source : telecom_algerie.commentaires_bruts")
# # print(f"   • Destination : telecom_algerie.commentaires_sans_urls")
# # print("="*70)

# # print("\n🎉 ÉTAPE 1 TERMINÉE AVEC SUCCÈS !")
# # print(f"💡 Les commentaires nettoyés sont dans: telecom_algerie.commentaires_sans_urls")

# # # Fermer les connexions
# # spark.stop()
# # client.close()
# # print("\n🔌 Connexions fermées")

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# # scripts/nettoyage/01_supprimer_urls.py - VERSION CORRIGÉE AVEC PATTERNS AMÉLIORÉS

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf
# from pyspark.sql.types import StringType, IntegerType
# import pandas as pd
# import re
# import os
# from pymongo import MongoClient
# from datetime import datetime
# from bson import ObjectId

# def supprimer_urls(texte):
#     """Supprime les URLs d'un texte - Version améliorée"""
#     if texte is None or not isinstance(texte, str):
#         return texte
    
#     # PATTERNS AMÉLIORÉS pour détecter tous les types d'URLs
#     patterns = [
#         r'https?://\S+',           # URLs complètes (http://example.com)
#         r'www\.\S+',                # www.example.com
#         r'https?://(?:\s|$)',       # https:// seul suivi d'espace ou fin de ligne
#         r'https?://$',              # https:// en fin de chaîne
#         r'\bhttps?://\b',           # https:// comme mot isolé
#         r'http://(?:\s|$)',         # http:// seul
#         r'http://$'                 # http:// en fin de chaîne
#     ]
    
#     texte_propre = texte
#     for pattern in patterns:
#         texte_propre = re.sub(pattern, '', texte_propre, flags=re.IGNORECASE)
    
#     # Supprimer les espaces multiples
#     texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
#     return texte_propre if texte_propre else None

# def detecter_urls(texte):
#     """Détecte si un texte contient des URLs - Version améliorée"""
#     if texte is None or not isinstance(texte, str):
#         return 0
    
#     # Patterns de détection
#     patterns = [
#         r'https?://',
#         r'www\.',
#         r'https?://(?:\s|$)',
#         r'https?://$'
#     ]
    
#     for pattern in patterns:
#         if re.search(pattern, texte, re.IGNORECASE):
#             return 1
#     return 0

# def extraire_urls(texte):
#     """Extrait toutes les URLs d'un texte - Version améliorée"""
#     if texte is None or not isinstance(texte, str):
#         return []
    
#     patterns = [
#         r'https?://\S+',
#         r'www\.\S+',
#         r'https?://(?:\s|$)',
#         r'https?://$'
#     ]
    
#     urls = []
#     for pattern in patterns:
#         found = re.findall(pattern, texte, re.IGNORECASE)
#         urls.extend([u for u in found if u.strip()])  # Éviter les chaînes vides
    
#     return list(set(urls))  # Éliminer les doublons

# def compter_urls(texte):
#     """Compte le nombre d'URLs dans un texte"""
#     if texte is None or not isinstance(texte, str):
#         return 0
#     return len(extraire_urls(texte))

# print("="*70)
# print("🔍 ÉTAPE 1 : DÉTECTION ET SUPPRESSION DES URLS")
# print("="*70)

# # 1. Créer Spark
# print("\n⚡ Démarrage de Spark...")
# spark = SparkSession.builder \
#     .appName("Suppression_URLs") \
#     .master("local[*]") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.driver.memory", "4g") \
#     .getOrCreate()
# print("✅ Spark démarré")

# # 2. Connexion à MongoDB
# print("\n📂 Connexion à MongoDB...")
# try:
#     client = MongoClient('localhost', 27018)
#     db = client['telecom_algerie']
    
#     # Collection source
#     collection_source = db['commentaires_bruts']
    
#     # Collection destination (nettoyée)
#     collection_dest = db['commentaires_sans_urls_v2']
    
#     # Vider la collection de destination si elle existe
#     collection_dest.delete_many({})
    
#     print("✅ Connexion MongoDB réussie")
    
# except Exception as e:
#     print(f"❌ Erreur de connexion MongoDB: {e}")
#     spark.stop()
#     exit(1)

# # 3. Charger les données
# print("\n📥 Chargement des commentaires...")
# data = list(collection_source.find({}))
# print(f"📊 {len(data)} commentaires chargés")

# if len(data) == 0:
#     print("❌ Aucune donnée trouvée")
#     spark.stop()
#     exit(1)

# # 4. ANALYSE : Détecter les URLs (avec nouveaux patterns)
# print("\n🔎 ANALYSE : Recherche des URLs (version améliorée)...")

# total_avec_urls = 0
# total_urls_trouvees = 0
# exemples_urls = []
# cas_speciaux = []  # Pour capturer les cas comme "https://" seul

# for doc in data[:50]:  # Analyser plus d'exemples pour trouver les cas spéciaux
#     commentaire = doc.get('Commentaire_Client', '')
#     urls = extraire_urls(commentaire)
    
#     # Vérifier spécifiquement les cas "https://" seul
#     if re.search(r'https?://(?:\s|$)', commentaire, re.IGNORECASE):
#         cas_speciaux.append({
#             'texte': commentaire[:150],
#             'urls': urls
#         })
    
#     if urls:
#         exemples_urls.append({
#             'texte': commentaire[:150],
#             'urls': urls
#         })

# # Compter tous les URLs
# for doc in data:
#     commentaire = doc.get('Commentaire_Client', '')
#     urls = extraire_urls(commentaire)
#     if urls:
#         total_avec_urls += 1
#         total_urls_trouvees += len(urls)

# print(f"\n📊 STATISTIQUES:")
# print(f"   ┌────────────────────────────────────┐")
# print(f"   │ Total commentaires    : {len(data):<15} │")
# print(f"   │ Avec URLs             : {total_avec_urls:<15} │")
# print(f"   │ URLs trouvées         : {total_urls_trouvees:<15} │")
# print(f"   │ Pourcentage           : {(total_avec_urls/len(data)*100):<15.2f}% │")
# print(f"   └────────────────────────────────────┘")

# # Afficher les cas spéciaux (https:// seul)
# if cas_speciaux:
#     print(f"\n⚠️ CAS SPÉCIAUX DÉTECTÉS (https:// seul):")
#     for i, ex in enumerate(cas_speciaux[:3], 1):
#         print(f"\n   Cas {i}:")
#         print(f"   📍 Texte: {ex['texte']}")
#         print(f"   🔗 URLs: {', '.join(ex['urls'])}")
#         print("   " + "-" * 60)

# # Afficher des exemples généraux
# if exemples_urls:
#     print("\n📝 EXEMPLES DE COMMENTAIRES AVEC URLS:")
#     for i, ex in enumerate(exemples_urls[:5], 1):
#         print(f"\n   Exemple {i}:")
#         print(f"   📍 Texte: {ex['texte']}...")
#         print(f"   🔗 URLs: {', '.join(ex['urls'])}")
#         print("   " + "-" * 60)

# # 5. NETTOYAGE : Supprimer les URLs
# print("\n🧹 SUPPRESSION DES URLS EN COURS (version améliorée)...")

# docs_nettoyes = []
# docs_avec_modifications = 0
# urls_par_doc = []

# for i, doc in enumerate(data):
#     # Créer une copie du document
#     doc_propre = doc.copy()
    
#     # Nettoyer le commentaire client
#     commentaire_original = doc.get('Commentaire_Client', '')
#     commentaire_nettoye = supprimer_urls(commentaire_original)
    
#     # Compter les URLs avant/après
#     urls_avant = compter_urls(commentaire_original)
#     urls_apres = compter_urls(commentaire_nettoye)
    
#     if urls_avant > 0:
#         urls_par_doc.append({
#             'id': doc.get('_id'),
#             'avant': urls_avant,
#             'apres': urls_apres,
#             'texte': commentaire_original[:100]
#         })
    
#     if commentaire_original != commentaire_nettoye:
#         docs_avec_modifications += 1
    
#     doc_propre['Commentaire_Client'] = commentaire_nettoye
    
#     # Nettoyer le commentaire moderateur s'il existe
#     if 'commentaire_moderateur' in doc:
#         mod_original = doc.get('commentaire_moderateur', '')
#         mod_nettoye = supprimer_urls(mod_original)
#         doc_propre['commentaire_moderateur'] = mod_nettoye
    
#     # Ajouter des métadonnées de nettoyage détaillées
#     doc_propre['_nettoyage'] = {
#         'date_nettoyage': datetime.now(),
#         'etape': 'suppression_urls_v2',
#         'urls_avant': urls_avant,
#         'urls_apres': urls_apres,
#         'urls_supprimees': urls_avant > 0
#     }
    
#     docs_nettoyes.append(doc_propre)
    
#     # Afficher la progression
#     if (i + 1) % 5000 == 0:
#         print(f"   ✓ {i + 1}/{len(data)} documents traités")

# print(f"\n✅ Traitement terminé: {len(docs_nettoyes)} documents")
# print(f"   • Documents modifiés: {docs_avec_modifications}")

# # Afficher quelques statistiques sur les URLs par document
# if urls_par_doc:
#     print("\n📊 Détail des URLs par document (échantillon):")
#     for item in urls_par_doc[:5]:
#         print(f"   • Document {item['id']}: {item['avant']} URLs → {item['apres']} après")
#         print(f"     Texte: {item['texte']}...")

# # 6. SAUVEGARDE DANS MONGODB
# print("\n💾 SAUVEGARDE DANS MongoDB...")

# try:
#     # Insérer par lots de 1000
#     batch_size = 1000
#     for i in range(0, len(docs_nettoyes), batch_size):
#         batch = docs_nettoyes[i:i+batch_size]
#         collection_dest.insert_many(batch)
#         print(f"   ✓ Lot {i//batch_size + 1}: {len(batch)} documents sauvegardés")
    
#     print(f"\n✅ {len(docs_nettoyes)} documents sauvegardés dans 'commentaires_sans_urls_v2'")
    
# except Exception as e:
#     print(f"❌ Erreur lors de la sauvegarde: {e}")

# # 7. VÉRIFICATION APPROFONDIE
# print("\n🔎 VÉRIFICATION APPROFONDIE DE LA SUPPRESSION...")

# # Vérifier avec différents patterns
# patterns_verification = [
#     r'https?://\S+',
#     r'www\.\S+',
#     r'https?://(?:\s|$)',
#     r'https?://$'
# ]

# print("\n📊 Vérification pattern par pattern:")

# for pattern in patterns_verification:
#     count = collection_dest.count_documents({
#         "Commentaire_Client": {"$regex": pattern, "$options": "i"}
#     })
#     print(f"   • Pattern '{pattern[:20]}...': {count} documents")

# # Vérification globale
# urls_restantes = collection_dest.count_documents({
#     "Commentaire_Client": {"$regex": "https?://|www\.", "$options": "i"}
# })

# print(f"\n📊 RÉSULTAT DE LA VÉRIFICATION:")
# print(f"   • Total documents avec URLs restantes: {urls_restantes}")

# if urls_restantes == 0:
#     print("   ✅ SUCCÈS : Aucune URL restante détectée !")
# else:
#     print(f"   ⚠️ ATTENTION : {urls_restantes} documents ont encore des URLs")
    
#     # Afficher les documents problématiques
#     print("\n📝 DOCUMENTS AVEC URLS RESTANTES:")
#     docs_problemes = collection_dest.find({
#         "Commentaire_Client": {"$regex": "https?://|www\.", "$options": "i"}
#     }).limit(5)
    
#     for doc in docs_problemes:
#         print(f"\n   • ID: {doc['_id']}")
#         print(f"     Texte: {doc.get('Commentaire_Client', '')[:150]}...")

# # 8. CRÉER UN RAPPORT DÉTAILLÉ
# print("\n📄 CRÉATION DU RAPPORT...")

# rapport = f"""
# {"="*70}
# RAPPORT DE SUPPRESSION DES URLS - VERSION AMÉLIORÉE
# {"="*70}

# Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

# 📊 STATISTIQUES GLOBALES:
#    • Total commentaires traités : {len(data)}
#    • Commentaires avec URLs     : {total_avec_urls}
#    • URLs trouvées              : {total_urls_trouvees}
#    • Pourcentage avec URLs      : {total_avec_urls/len(data)*100:.2f}%
#    • Documents modifiés         : {docs_avec_modifications}

# 📁 STOCKAGE:
#    • Collection source      : telecom_algerie.commentaires_bruts
#    • Collection destination : telecom_algerie.commentaires_sans_urls
#    • Documents sauvegardés  : {len(docs_nettoyes)}

# 🔍 RÉSULTATS DE LA VÉRIFICATION:
#    • Documents avec URLs restantes : {urls_restantes}
#    • Statut : {"✅ SUCCÈS" if urls_restantes == 0 else "⚠️ ÉCHEC"}

# 📝 EXEMPLES D'URLS SUPPRIMÉES:
# """

# # Ajouter des exemples au rapport
# for i, ex in enumerate(exemples_urls[:5]):
#     rapport += f"\n   {i+1}. URLs: {', '.join(ex['urls'])}"
#     rapport += f"\n      Texte: {ex['texte'][:100]}...\n"

# # Sauvegarder le rapport
# os.makedirs("donnees/resultats", exist_ok=True)
# rapport_path = "donnees/resultats/rapport_urls.txt"
# with open(rapport_path, "w", encoding="utf-8") as f:
#     f.write(rapport)

# print(f"✅ Rapport sauvegardé: {rapport_path}")

# # 9. EXPORT OPTIONNEL
# print("\n📁 EXPORT OPTIONNEL EN CSV/EXCEL...")

# reponse = input("\nVoulez-vous aussi exporter en CSV/Excel ? (o/n): ")
# if reponse.lower() == 'o':
#     try:
#         # Récupérer quelques documents
#         docs_export = list(collection_dest.find().limit(1000))
        
#         # Convertir ObjectId en string
#         for doc in docs_export:
#             doc['_id'] = str(doc['_id'])
#             if '_nettoyage' in doc:
#                 doc['_nettoyage'] = str(doc['_nettoyage'])
        
#         # Créer DataFrame
#         df_export = pd.DataFrame(docs_export)
        
#         # Exporter
#         csv_path = "donnees/resultats/commentaires_sans_urls.csv"
#         excel_path = "donnees/resultats/commentaires_sans_urls.xlsx"
        
#         df_export.to_csv(csv_path, index=False, encoding='utf-8-sig')
#         df_export.to_excel(excel_path, index=False)
        
#         print(f"✅ CSV exporté: {csv_path}")
#         print(f"✅ Excel exporté: {excel_path}")
        
#     except Exception as e:
#         print(f"❌ Erreur lors de l'export: {e}")

# # 10. RÉSUMÉ FINAL
# print("\n" + "="*70)
# print("📊 RÉSUMÉ FINAL - VERSION AMÉLIORÉE")
# print("="*70)
# print(f"📥 Commentaires traités    : {len(data)}")
# print(f"🔗 URLs détectées          : {total_urls_trouvees}")
# print(f"📝 Commentaires avec URLs  : {total_avec_urls}")
# print(f"✅ Documents modifiés      : {docs_avec_modifications}")
# print(f"🔍 URLs restantes          : {urls_restantes}")
# print(f"\n📁 Base de données MongoDB:")
# print(f"   • Source : telecom_algerie.commentaires_bruts")
# print(f"   • Destination : telecom_algerie.commentaires_sans_urls")
# print("="*70)

# print("\n🎉 ÉTAPE 1 TERMINÉE AVEC SUCCÈS !")
# print(f"💡 Les commentaires nettoyés sont dans: telecom_algerie.commentaires_sans_urls")

# # Fermer les connexions
# spark.stop()
# client.close()
# print("\n🔌 Connexions fermées")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/nettoyage/01_supprimer_urls.py - VERSION AVEC MESURE DE TEMPS

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType
import pandas as pd
import re
import os
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import time  # 👈 IMPORT POUR MESURER LE TEMPS

def supprimer_urls(texte):
    """Supprime les URLs d'un texte - Version améliorée"""
    if texte is None or not isinstance(texte, str):
        return texte
    
    # PATTERNS AMÉLIORÉS pour détecter tous les types d'URLs
    patterns = [
        r'https?://\S+',           # URLs complètes (http://example.com)
        r'www\.\S+',                # www.example.com
        r'https?://(?:\s|$)',       # https:// seul suivi d'espace ou fin de ligne
        r'https?://$',              # https:// en fin de chaîne
        r'\bhttps?://\b',           # https:// comme mot isolé
        r'http://(?:\s|$)',         # http:// seul
        r'http://$'                 # http:// en fin de chaîne
    ]
    
    texte_propre = texte
    for pattern in patterns:
        texte_propre = re.sub(pattern, '', texte_propre, flags=re.IGNORECASE)
    
    # Supprimer les espaces multiples
    texte_propre = re.sub(r'\s+', ' ', texte_propre).strip()
    return texte_propre if texte_propre else None

def detecter_urls(texte):
    """Détecte si un texte contient des URLs - Version améliorée"""
    if texte is None or not isinstance(texte, str):
        return 0
    
    # Patterns de détection
    patterns = [
        r'https?://',
        r'www\.',
        r'https?://(?:\s|$)',
        r'https?://$'
    ]
    
    for pattern in patterns:
        if re.search(pattern, texte, re.IGNORECASE):
            return 1
    return 0

def extraire_urls(texte):
    """Extrait toutes les URLs d'un texte - Version améliorée"""
    if texte is None or not isinstance(texte, str):
        return []
    
    patterns = [
        r'https?://\S+',
        r'www\.\S+',
        r'https?://(?:\s|$)',
        r'https?://$'
    ]
    
    urls = []
    for pattern in patterns:
        found = re.findall(pattern, texte, re.IGNORECASE)
        urls.extend([u for u in found if u.strip()])  # Éviter les chaînes vides
    
    return list(set(urls))  # Éliminer les doublons

def compter_urls(texte):
    """Compte le nombre d'URLs dans un texte"""
    if texte is None or not isinstance(texte, str):
        return 0
    return len(extraire_urls(texte))

# 📊 DÉBUT DU CHRONOMÈTRAGE GLOBAL
temps_debut_global = time.time()

print("="*70)
print("🔍 ÉTAPE 1 : DÉTECTION ET SUPPRESSION DES URLS - SINGLE NODE")
print("="*70)

# 1. Créer Spark
print("\n⚡ Démarrage de Spark...")
temps_debut_spark = time.time()

spark = SparkSession.builder \
    .appName("Suppression_URLs") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

temps_fin_spark = time.time()
print(f"✅ Spark démarré en {temps_fin_spark - temps_debut_spark:.2f} secondes")

# 2. Connexion à MongoDB
print("\n📂 Connexion à MongoDB...")
temps_debut_mongo = time.time()

try:
    client = MongoClient('localhost', 27018)
    db = client['telecom_algerie']
    
    # Collection source
    collection_source = db['commentaires_bruts']
    
    # Collection destination (nettoyée)
    collection_dest = db['commentaires_sans_urls_singlenode']
    
    # Vider la collection de destination si elle existe
    collection_dest.delete_many({})
    
    temps_fin_mongo = time.time()
    print(f"✅ Connexion MongoDB réussie en {temps_fin_mongo - temps_debut_mongo:.2f} secondes")
    
except Exception as e:
    print(f"❌ Erreur de connexion MongoDB: {e}")
    spark.stop()
    exit(1)

# 3. Charger les données
print("\n📥 Chargement des commentaires...")
temps_debut_chargement = time.time()

data = list(collection_source.find({}))
print(f"📊 {len(data)} commentaires chargés")

temps_fin_chargement = time.time()
print(f"✅ Chargement terminé en {temps_fin_chargement - temps_debut_chargement:.2f} secondes")

if len(data) == 0:
    print("❌ Aucune donnée trouvée")
    spark.stop()
    exit(1)

# 4. ANALYSE : Détecter les URLs
print("\n🔎 ANALYSE : Recherche des URLs (version améliorée)...")
temps_debut_analyse = time.time()

total_avec_urls = 0
total_urls_trouvees = 0
exemples_urls = []
cas_speciaux = []  # Pour capturer les cas comme "https://" seul

for doc in data[:50]:  # Analyser plus d'exemples pour trouver les cas spéciaux
    commentaire = doc.get('Commentaire_Client', '')
    urls = extraire_urls(commentaire)
    
    # Vérifier spécifiquement les cas "https://" seul
    if re.search(r'https?://(?:\s|$)', commentaire, re.IGNORECASE):
        cas_speciaux.append({
            'texte': commentaire[:150],
            'urls': urls
        })
    
    if urls:
        exemples_urls.append({
            'texte': commentaire[:150],
            'urls': urls
        })

# Compter tous les URLs
for doc in data:
    commentaire = doc.get('Commentaire_Client', '')
    urls = extraire_urls(commentaire)
    if urls:
        total_avec_urls += 1
        total_urls_trouvees += len(urls)

temps_fin_analyse = time.time()
print(f"\n📊 STATISTIQUES (analyse en {temps_fin_analyse - temps_debut_analyse:.2f}s):")
print(f"   ┌────────────────────────────────────┐")
print(f"   │ Total commentaires    : {len(data):<15} │")
print(f"   │ Avec URLs             : {total_avec_urls:<15} │")
print(f"   │ URLs trouvées         : {total_urls_trouvees:<15} │")
print(f"   │ Pourcentage           : {(total_avec_urls/len(data)*100):<15.2f}% │")
print(f"   └────────────────────────────────────┘")

# Afficher les cas spéciaux (https:// seul)
if cas_speciaux:
    print(f"\n⚠️ CAS SPÉCIAUX DÉTECTÉS (https:// seul):")
    for i, ex in enumerate(cas_speciaux[:3], 1):
        print(f"\n   Cas {i}:")
        print(f"   📍 Texte: {ex['texte']}")
        print(f"   🔗 URLs: {', '.join(ex['urls'])}")
        print("   " + "-" * 60)

# Afficher des exemples généraux
if exemples_urls:
    print("\n📝 EXEMPLES DE COMMENTAIRES AVEC URLS:")
    for i, ex in enumerate(exemples_urls[:5], 1):
        print(f"\n   Exemple {i}:")
        print(f"   📍 Texte: {ex['texte']}...")
        print(f"   🔗 URLs: {', '.join(ex['urls'])}")
        print("   " + "-" * 60)

# 5. NETTOYAGE : Supprimer les URLs
print("\n🧹 SUPPRESSION DES URLS EN COURS...")
temps_debut_nettoyage = time.time()

docs_nettoyes = []
docs_avec_modifications = 0
urls_par_doc = []

for i, doc in enumerate(data):
    # Créer une copie du document
    doc_propre = doc.copy()
    
    # Nettoyer le commentaire client
    commentaire_original = doc.get('Commentaire_Client', '')
    commentaire_nettoye = supprimer_urls(commentaire_original)
    
    # Compter les URLs avant/après
    urls_avant = compter_urls(commentaire_original)
    urls_apres = compter_urls(commentaire_nettoye)
    
    if urls_avant > 0:
        urls_par_doc.append({
            'id': doc.get('_id'),
            'avant': urls_avant,
            'apres': urls_apres,
            'texte': commentaire_original[:100]
        })
    
    if commentaire_original != commentaire_nettoye:
        docs_avec_modifications += 1
    
    doc_propre['Commentaire_Client'] = commentaire_nettoye
    
    # Nettoyer le commentaire moderateur s'il existe
    if 'commentaire_moderateur' in doc:
        mod_original = doc.get('commentaire_moderateur', '')
        mod_nettoye = supprimer_urls(mod_original)
        doc_propre['commentaire_moderateur'] = mod_nettoye
    
    # Ajouter des métadonnées de nettoyage détaillées
    doc_propre['_nettoyage'] = {
        'date_nettoyage': datetime.now(),
        'etape': 'suppression_urls_v2',
        'urls_avant': urls_avant,
        'urls_apres': urls_apres,
        'urls_supprimees': urls_avant > 0
    }
    
    docs_nettoyes.append(doc_propre)
    
    # Afficher la progression
    if (i + 1) % 5000 == 0:
        print(f"   ✓ {i + 1}/{len(data)} documents traités")

temps_fin_nettoyage = time.time()
print(f"\n✅ Traitement terminé: {len(docs_nettoyes)} documents en {temps_fin_nettoyage - temps_debut_nettoyage:.2f} secondes")
print(f"   • Documents modifiés: {docs_avec_modifications}")

# Afficher quelques statistiques sur les URLs par document
if urls_par_doc:
    print("\n📊 Détail des URLs par document (échantillon):")
    for item in urls_par_doc[:5]:
        print(f"   • Document {item['id']}: {item['avant']} URLs → {item['apres']} après")
        print(f"     Texte: {item['texte']}...")

# 6. SAUVEGARDE DANS MONGODB
print("\n💾 SAUVEGARDE DANS MongoDB...")
temps_debut_sauvegarde = time.time()

try:
    # Insérer par lots de 1000
    batch_size = 1000
    for i in range(0, len(docs_nettoyes), batch_size):
        batch = docs_nettoyes[i:i+batch_size]
        collection_dest.insert_many(batch)
        print(f"   ✓ Lot {i//batch_size + 1}: {len(batch)} documents sauvegardés")
    
    temps_fin_sauvegarde = time.time()
    print(f"\n✅ {len(docs_nettoyes)} documents sauvegardés dans 'commentaires_sans_urls_singlenode'")
    print(f"   ⏱️  Temps de sauvegarde: {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f} secondes")
    
except Exception as e:
    print(f"❌ Erreur lors de la sauvegarde: {e}")

# 7. VÉRIFICATION APPROFONDIE
print("\n🔎 VÉRIFICATION APPROFONDIE DE LA SUPPRESSION...")
temps_debut_verification = time.time()

# Vérifier avec différents patterns
patterns_verification = [
    r'https?://\S+',
    r'www\.\S+',
    r'https?://(?:\s|$)',
    r'https?://$'
]

print("\n📊 Vérification pattern par pattern:")

for pattern in patterns_verification:
    count = collection_dest.count_documents({
        "Commentaire_Client": {"$regex": pattern, "$options": "i"}
    })
    print(f"   • Pattern '{pattern[:20]}...': {count} documents")

# Vérification globale
urls_restantes = collection_dest.count_documents({
    "Commentaire_Client": {"$regex": "https?://|www\.", "$options": "i"}
})

temps_fin_verification = time.time()
print(f"\n📊 RÉSULTAT DE LA VÉRIFICATION (en {temps_fin_verification - temps_debut_verification:.2f}s):")
print(f"   • Total documents avec URLs restantes: {urls_restantes}")

if urls_restantes == 0:
    print("   ✅ SUCCÈS : Aucune URL restante détectée !")
else:
    print(f"   ⚠️ ATTENTION : {urls_restantes} documents ont encore des URLs")
    
    # Afficher les documents problématiques
    print("\n📝 DOCUMENTS AVEC URLS RESTANTES:")
    docs_problemes = collection_dest.find({
        "Commentaire_Client": {"$regex": "https?://|www\.", "$options": "i"}
    }).limit(5)
    
    for doc in docs_problemes:
        print(f"\n   • ID: {doc['_id']}")
        print(f"     Texte: {doc.get('Commentaire_Client', '')[:150]}...")

# 🏁 FIN DU CHRONOMÈTRAGE GLOBAL
temps_fin_global = time.time()
temps_total = temps_fin_global - temps_debut_global

# 8. CRÉER UN RAPPORT DÉTAILLÉ AVEC TEMPS
print("\n📄 CRÉATION DU RAPPORT...")

rapport = f"""
{"="*70}
RAPPORT DE SUPPRESSION DES URLS - SINGLE NODE
{"="*70}

Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode : Single Node (Spark local)

⏱️  TEMPS D'EXÉCUTION:
   • Connexion Spark        : {temps_fin_spark - temps_debut_spark:.2f}s
   • Connexion MongoDB      : {temps_fin_mongo - temps_debut_mongo:.2f}s
   • Chargement données     : {temps_fin_chargement - temps_debut_chargement:.2f}s
   • Analyse des URLs       : {temps_fin_analyse - temps_debut_analyse:.2f}s
   • Nettoyage URLs         : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s
   • Sauvegarde MongoDB     : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s
   • Vérification           : {temps_fin_verification - temps_debut_verification:.2f}s
   • ──────────────────────────────────
   • TEMPS TOTAL            : {temps_total:.2f}s
   • Documents par seconde  : {len(data) / temps_total:.2f} doc/s

📊 STATISTIQUES GLOBALES:
   • Total commentaires traités : {len(data)}
   • Commentaires avec URLs     : {total_avec_urls}
   • URLs trouvées              : {total_urls_trouvees}
   • Pourcentage avec URLs      : {total_avec_urls/len(data)*100:.2f}%
   • Documents modifiés         : {docs_avec_modifications}

📁 STOCKAGE:
   • Collection source      : telecom_algerie.commentaires_bruts
   • Collection destination : telecom_algerie.commentaires_sans_urls_singlenode
   • Documents sauvegardés  : {len(docs_nettoyes)}

🔍 RÉSULTATS DE LA VÉRIFICATION:
   • Documents avec URLs restantes : {urls_restantes}
   • Statut : {"✅ SUCCÈS" if urls_restantes == 0 else "⚠️ ÉCHEC"}

📝 EXEMPLES D'URLS SUPPRIMÉES:
"""

# Ajouter des exemples au rapport
for i, ex in enumerate(exemples_urls[:5]):
    rapport += f"\n   {i+1}. URLs: {', '.join(ex['urls'])}"
    rapport += f"\n      Texte: {ex['texte'][:100]}...\n"

# Sauvegarder le rapport
os.makedirs("donnees/resultats", exist_ok=True)
rapport_path = "donnees/resultats/rapport_urls_singlenode.txt"
with open(rapport_path, "w", encoding="utf-8") as f:
    f.write(rapport)

print(f"✅ Rapport sauvegardé: {rapport_path}")

# 9. EXPORT OPTIONNEL
print("\n📁 EXPORT OPTIONNEL EN CSV/EXCEL...")

reponse = input("\nVoulez-vous aussi exporter en CSV/Excel ? (o/n): ")
if reponse.lower() == 'o':
    try:
        # Récupérer quelques documents
        docs_export = list(collection_dest.find().limit(1000))
        
        # Convertir ObjectId en string
        for doc in docs_export:
            doc['_id'] = str(doc['_id'])
            if '_nettoyage' in doc:
                doc['_nettoyage'] = str(doc['_nettoyage'])
        
        # Créer DataFrame
        df_export = pd.DataFrame(docs_export)
        
        # Exporter
        csv_path = "donnees/resultats/commentaires_sans_urls_singlenode.csv"
        excel_path = "donnees/resultats/commentaires_sans_urls_singlenode.xlsx"
        
        df_export.to_csv(csv_path, index=False, encoding='utf-8-sig')
        df_export.to_excel(excel_path, index=False)
        
        print(f"✅ CSV exporté: {csv_path}")
        print(f"✅ Excel exporté: {excel_path}")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'export: {e}")

# 10. RÉSUMÉ FINAL AVEC TEMPS
print("\n" + "="*70)
print("📊 RÉSUMÉ FINAL - SINGLE NODE")
print("="*70)
print(f"📥 Commentaires traités    : {len(data)}")
print(f"🔗 URLs détectées          : {total_urls_trouvees}")
print(f"📝 Commentaires avec URLs  : {total_avec_urls}")
print(f"✅ Documents modifiés      : {docs_avec_modifications}")
print(f"🔍 URLs restantes          : {urls_restantes}")
print(f"\n⏱️  TEMPS D'EXÉCUTION:")
print(f"   • Chargement : {temps_fin_chargement - temps_debut_chargement:.2f}s")
print(f"   • Nettoyage  : {temps_fin_nettoyage - temps_debut_nettoyage:.2f}s")
print(f"   • Sauvegarde : {temps_fin_sauvegarde - temps_debut_sauvegarde:.2f}s")
print(f"   • TOTAL      : {temps_total:.2f}s")
print(f"   • Vitesse    : {len(data) / temps_total:.2f} docs/s")
print(f"\n📁 Base de données MongoDB:")
print(f"   • Source : telecom_algerie.commentaires_bruts")
print(f"   • Destination : telecom_algerie.commentaires_sans_urls_singlenode")
print("="*70)

print("\n🎉 ÉTAPE 1 TERMINÉE AVEC SUCCÈS !")
print(f"💡 Les commentaires nettoyés sont dans: telecom_algerie.commentaires_sans_urls_singlenode")

# Fermer les connexions
spark.stop()
client.close()
print("\n🔌 Connexions fermées")