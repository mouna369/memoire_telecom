# #!/usr/bin/env python3
# from pymongo import MongoClient
# from datetime import datetime

# print("="*60)
# print("🔍 VÉRIFICATION COMPLÈTE DE MONGODB")
# print("="*60)

# # Connexion
# client = MongoClient('localhost', 27017)
# db = client['telecom_algerie']
# collection = db['commentaires_bruts']

# # 1. Total général
# total = collection.count_documents({})
# print(f"\n📊 TOTAL: {total} commentaires")

# # 2. Répartition par fichier
# print("\n📁 RÉPARTITION PAR FICHIER SOURCE:")
# pipeline_fichiers = [
#     {"$group": {"_id": "$metadata.fichier", "count": {"$sum": 1}}}
# ]
# for doc in collection.aggregate(pipeline_fichiers):
#     print(f"   {doc['_id']}: {doc['count']}")

# # 3. Répartition par source (réseau social)
# print("\n📱 RÉPARTITION PAR SOURCE:")
# pipeline_source = [
#     {"$group": {"_id": "$source", "count": {"$sum": 1}}},
#     {"$sort": {"count": -1}}
# ]
# for doc in collection.aggregate(pipeline_source):
#     print(f"   {doc['_id']}: {doc['count']}")

# # 4. Afficher 3 exemples
# print("\n📝 3 PREMIERS COMMENTAIRES:")
# for doc in collection.find().limit(3):
#     print(f"\n   📄 Fichier: {doc['metadata']['fichier']}")
#     print(f"   💬 Texte: {doc['texte_original'][:100]}...")
#     print(f"   📅 Date: {doc['date']}")
#     print(f"   📱 Source: {doc['source']}")

# # 5. Statistiques rapides
# print("\n📊 STATISTIQUES:")
# print(f"   🔹 Commentaires avec date: {collection.count_documents({'date': {'$ne': ''}})}")
# print(f"   🔹 Commentaires sans date: {collection.count_documents({'date': ''})}")
# print(f"   🔹 Commentaires Facebook: {collection.count_documents({'source': 'Facebook'})}")
# print(f"   🔹 Commentaires Twitter: {collection.count_documents({'source': 'Twitter'})}")

# print("\n" + "="*60)
# print("✅ VÉRIFICATION TERMINÉE")
# print("="*60)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import glob
import os
import subprocess

print("="*60)
print("📊 ANALYSE DES RÉSULTATS DU NETTOYAGE")
print("="*60)

# Chemin vers les résultats
base_path = "/home/mouna/projet_telecom/data/results/commentaires_sans_urls.parquet"

# 1. Vérifier que le dossier existe
if not os.path.exists(base_path):
    print(f"❌ Dossier non trouvé: {base_path}")
    print("📁 Vérifiez le chemin:")
    os.system("ls -la /home/mouna/projet_telecom/data/results/")
    exit(1)

print(f"📁 Dossier trouvé: {base_path}")

# 2. Voir TOUT le contenu avec sudo
print("\n🔍 Contenu COMPLET du dossier (avec sudo):")
subprocess.run(f"sudo ls -la {base_path}/", shell=True)

# 3. Chercher tous les dossiers task_
print("\n🔍 Recherche des dossiers task_...")
task_dirs = subprocess.getoutput(f"sudo find {base_path} -type d -name 'task_*'").split('\n')
task_dirs = [d for d in task_dirs if d]  # Enlever les lignes vides

print(f"✅ {len(task_dirs)} dossiers task_ trouvés")

# 4. Chercher les fichiers Parquet dans ces dossiers
print("\n📂 Recherche des fichiers Parquet...")
all_files = []

for task_dir in task_dirs:
    files = subprocess.getoutput(f"sudo find {task_dir} -name '*.parquet'").split('\n')
    for f in files:
        if f and not f.endswith('.crc'):  # Ignorer les fichiers .crc
            all_files.append(f)
            print(f"   ✓ {os.path.basename(task_dir)}/{os.path.basename(f)}")

print(f"\n✅ {len(all_files)} fichiers Parquet trouvés")

# 5. Copier les fichiers dans un dossier temporaire avec les bonnes permissions
if all_files:
    temp_dir = "/tmp/resultats_parquet"
    os.makedirs(temp_dir, exist_ok=True)
    os.system(f"sudo chmod 777 {temp_dir}")
    
    print(f"\n📋 Copie des fichiers vers {temp_dir}...")
    for i, file in enumerate(all_files):
        dest = f"{temp_dir}/part-{i:05d}.parquet"
        os.system(f"sudo cp {file} {dest}")
        os.system(f"sudo chmod 644 {dest}")
        print(f"   {i+1}/{len(all_files)} copié")
    
    # 6. Lire les fichiers avec pandas
    print("\n📖 Lecture des fichiers...")
    all_dfs = []
    
    for i in range(len(all_files)):
        try:
            df = pd.read_parquet(f"{temp_dir}/part-{i:05d}.parquet")
            all_dfs.append(df)
            print(f"   ✓ Partie {i+1}: {len(df)} lignes")
        except Exception as e:
            print(f"   ❌ Erreur partie {i+1}: {e}")
    
    # 7. Concaténer
    if all_dfs:
        print("\n🔗 Fusion des données...")
        df_final = pd.concat(all_dfs, ignore_index=True)
        
        print(f"\n✅ TOTAL: {len(df_final)} lignes")
        
        # 8. Aperçu
        print("\n👀 Aperçu des 5 premières lignes:")
        if 'Commentaire_Client' in df_final.columns and 'commentaire_clean' in df_final.columns:
            print(df_final[['Commentaire_Client', 'commentaire_clean']].head())
        else:
            print(df_final.head())
        
        # 9. Sauvegarder
        output_file = "/home/mouna/projet_telecom/data/results/commentaires_sans_urlsl.csv"
        df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
        os.system(f"sudo chown mouna:mouna {output_file}")
        print(f"\n💾 Fichier CSV sauvegardé: {output_file}")
        
        # 10. Nettoyage
        os.system(f"rm -rf {temp_dir}")
        
    else:
        print("❌ Aucune donnée lue!")
else:
    print("❌ Aucun fichier Parquet trouvé!")

print("\n" + "="*60)
print("🎉 ANALYSE TERMINÉE")
print("="*60)