# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # """
# # SCRIPT: Importer les fichiers Excel dans MongoDB
# # À exécuter UNE SEULE fois au début
# # """

# # import pandas as pd
# # from pymongo import MongoClient
# # from datetime import datetime
# # import os
# # import glob

# # print("="*60)
# # print("📥 IMPORT DES FICHIERS EXCEL VERS MONGODB")
# # print("="*60)

# # # 1. Connexion à MongoDB
# # print("\n🔌 Connexion à MongoDB...")
# # try:
# #     client = MongoClient('localhost', 27018, serverSelectionTimeoutMS=5000)
# #     client.admin.command('ping')
# #     print("✅ Connecté à MongoDB")
# # except Exception as e:
# #     print(f"❌ Erreur: {e}")
# #     exit(1)

# # # 2. Créer la base et la collection
# # db = client['telecom_algerie']
# # collection = db['commentaires_bruts2']

# # # 3. Chercher les fichiers Excel
# # print("\n🔍 Recherche des fichiers Excel...")
# # fichiers = glob.glob("/home/mouna/projet_telecom/donnees/brutes/*.xlsx")

# # if not fichiers:
# #     print("❌ Aucun fichier trouvé dans donnees/brutes/")
# #     print("📁 Vérifie le dossier")
# #     exit(1)

# # print(f"✅ {len(fichiers)} fichier(s) trouvé(s):")
# # for f in fichiers:
# #     print(f"   - {os.path.basename(f)}")


# # # 4. Importer chaque fichier
# # total = 0
# # for fichier in fichiers:
# #     print(f"\n📄 Traitement de: {os.path.basename(fichier)}")
    
# #     # Lire l'Excel
# #     df = pd.read_excel(fichier, header=1)
# #     print(f"   📊 {len(df)} lignes lues")
    
# #     # Convertir en documents
# #     documents = []
# #     for idx, row in df.iterrows():
# #         doc = {
# #             'Commentaire_Client': str(row.get('Commentaire Client', '')),
# #             'commentaire_moderateur': str(row.get('Commentaire Modérateur', '')), 
# #             'date': str(row.get('Date', '')),
# #             'source': str(row.get('Réseau Social', '')),
# #             'moderateur': str(row.get('Modérateur', '')),
# #             'metadata': {
# #                 'fichier': os.path.basename(fichier),
# #                 'ligne': idx + 2,
# #                 'date_import': datetime.now()
# #             },
# #             'statut': 'brut'
# #         }
# #         documents.append(doc)
    
# #     # Insérer dans MongoDB
# #     resultat = collection.insert_many(documents)
# #     print(f"   ✅ {len(resultat.inserted_ids)} documents insérés")
# #     total += len(resultat.inserted_ids)

# # # 5. Résumé
# # print("\n" + "="*60)
# # print("📊 RÉSUMÉ FINAL")
# # print("="*60)
# # print(f"✅ Total importé: {total} commentaires")
# # print(f"📁 Dans MongoDB: telecom_algerie.commentaires_bruts")
# # print("="*60)

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# SCRIPT: Importer les fichiers Excel dans MongoDB
# À exécuter UNE SEULE fois au début
# """

# import pandas as pd
# from pymongo import MongoClient
# from datetime import datetime
# import os
# import glob

# print("="*60)
# print("📥 IMPORT DES FICHIERS EXCEL VERS MONGODB")
# print("="*60)

# # 1. Connexion à MongoDB
# print("\n🔌 Connexion à MongoDB...")
# try:
#     client = MongoClient('localhost', 27018, serverSelectionTimeoutMS=5000)
#     client.admin.command('ping')
#     print("✅ Connecté à MongoDB")
# except Exception as e:
#     print(f"❌ Erreur: {e}")
#     exit(1)

# # 2. Créer la base et la collection
# db = client['telecom_algerie']
# collection = db['commentaires_bruts2']

# # 3. Chercher les fichiers Excel
# print("\n🔍 Recherche des fichiers Excel...")
# fichiers = glob.glob("/home/mouna/projet_telecom/donnees/brutes/*.xlsx")

# if not fichiers:
#     print("❌ Aucun fichier trouvé dans donnees/brutes/")
#     print("📁 Vérifie le dossier")
#     exit(1)

# print(f"✅ {len(fichiers)} fichier(s) trouvé(s):")
# for f in fichiers:
#     print(f"   - {os.path.basename(f)}")


# # 4. Importer chaque fichier
# total = 0
# for fichier in fichiers:
#     print(f"\n📄 Traitement de: {os.path.basename(fichier)}")
    
#     # Lire l'Excel
#     df = pd.read_excel(fichier, header=1)
#     print(f"   📊 {len(df)} lignes lues")
    
#     # Convertir en documents
#     documents = []
#     for idx, row in df.iterrows():
#         # Récupérer la date et la convertir en datetime
#         date_val = row.get('Date')
#         if pd.notna(date_val):
#             # Si c'est déjà un datetime, le garder ; sinon tenter de parser
#             if isinstance(date_val, datetime):
#                 date_obj = date_val
#             else:
#                 try:
#                     date_obj = pd.to_datetime(date_val).to_pydatetime()
#                 except Exception:
#                     # Si la conversion échoue, on garde None ou on peut logger
#                     print(f"      ⚠️ Ligne {idx+2}: date non convertible '{date_val}'")
#                     date_obj = None
#         else:
#             date_obj = None
        
#         doc = {
#             'Commentaire_Client': str(row.get('Commentaire Client', '')),
#             'commentaire_moderateur': str(row.get('Commentaire Modérateur', '')),
#             'date': date_obj,                     # ✅ datetime (ou None)
#             'source': str(row.get('Réseau Social', '')),
#             'moderateur': str(row.get('Modérateur', '')),
#             'traite': False,
#             'metadata': {
#                 'fichier': os.path.basename(fichier),
#                 'ligne': idx + 2,
#                 'date_import': datetime.now()
#             },
#             'statut': 'brut'
#         }
#         documents.append(doc)
    
#     # Insérer dans MongoDB
#     if documents:
#         resultat = collection.insert_many(documents)
#         print(f"   ✅ {len(resultat.inserted_ids)} documents insérés")
#         total += len(resultat.inserted_ids)

# # 5. Résumé
# print("\n" + "="*60)
# print("📊 RÉSUMÉ FINAL")
# print("="*60)
# print(f"✅ Total importé: {total} commentaires")
# print(f"📁 Dans MongoDB: telecom_algerie.commentaires_bruts")
# print("="*60)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT: Importer les fichiers Excel dans MongoDB
À exécuter UNE SEULE fois au début
"""

import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os
import glob
from bson.objectid import ObjectId  # pour générer des ObjectId

print("="*60)
print("📥 IMPORT DES FICHIERS EXCEL VERS MONGODB")
print("="*60)

# 1. Connexion à MongoDB
print("\n🔌 Connexion à MongoDB...")
try:
    client = MongoClient('localhost', 27018, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("✅ Connecté à MongoDB")
except Exception as e:
    print(f"❌ Erreur: {e}")
    exit(1)

# 2. Créer la base et la collection
db = client['telecom_algerie']
collection = db['commentaires_bruts2']

# 3. Chercher les fichiers Excel
print("\n🔍 Recherche des fichiers Excel...")
fichiers = glob.glob("/home/mouna/projet_telecom/donnees/brutes/*.xlsx")

if not fichiers:
    print("❌ Aucun fichier trouvé dans donnees/brutes/")
    print("📁 Vérifie le dossier")
    exit(1)

print(f"✅ {len(fichiers)} fichier(s) trouvé(s):")
for f in fichiers:
    print(f"   - {os.path.basename(f)}")


# 4. Importer chaque fichier
total = 0
for fichier in fichiers:
    print(f"\n📄 Traitement de: {os.path.basename(fichier)}")
    
    # Lire l'Excel
    df = pd.read_excel(fichier, header=1)
    print(f"   📊 {len(df)} lignes lues")
    
    # Convertir en documents
    documents = []
    for idx, row in df.iterrows():
        # Générer un ObjectId et le convertir en string
        doc_id = str(ObjectId())   # ex: "69d7be7ca1a6a97691eaf0d6"
        
        doc = {
            '_id': doc_id,  # ← l'_id est une string de 24 caractères hexadécimaux
            'Commentaire_Client': str(row.get('Commentaire Client', '')),
            'commentaire_moderateur': str(row.get('Commentaire Modérateur', '')), 
            'date': str(row.get('Date', '')),
            'source': str(row.get('Réseau Social', '')),
            'moderateur': str(row.get('Modérateur', '')),
            'metadata': {
                'fichier': os.path.basename(fichier),
                'ligne': idx + 2,
                'date_import': datetime.now()
            },
            'statut': 'brut'
        }
        documents.append(doc)
    
    # Insérer dans MongoDB
    resultat = collection.insert_many(documents)
    print(f"   ✅ {len(resultat.inserted_ids)} documents insérés")
    total += len(resultat.inserted_ids)

# 5. Résumé
print("\n" + "="*60)
print("📊 RÉSUMÉ FINAL")
print("="*60)
print(f"✅ Total importé: {total} commentaires")
print(f"📁 Dans MongoDB: telecom_algerie.commentaires_bruts2")
print("="*60)