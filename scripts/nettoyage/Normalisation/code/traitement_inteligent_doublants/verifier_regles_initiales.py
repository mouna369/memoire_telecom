from pymongo import MongoClient
import pandas as pd
from collections import defaultdict
from datetime import datetime
import json

# ============================================================
# CONNEXION À MONGODB ATLAS
# ============================================================

MONGO_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

print("🔌 Connexion à MongoDB Atlas...")
client = MongoClient(MONGO_URI)

# Sélectionner la base et la collection
db = client["telecom_algerie_new"]
collection = db["dataset_unifie_sans_doublons"]

# Tester la connexion
try:
    client.admin.command('ping')
    print("✅ Connexion réussie !")
except Exception as e:
    print(f"❌ Erreur de connexion : {e}")
    exit()

# Compter les documents
nb_documents = collection.count_documents({})
print(f"📊 Collection 'dataset_unifie_copie' : {nb_documents} documents")

# ============================================================
# CHARGER LES DONNÉES DANS UN DATAFRAME
# ============================================================

print("\n📥 Chargement des données...")
cursor = collection.find({})
df = pd.DataFrame(list(cursor))

print(f"✅ {len(df)} commentaires chargés")
print(f"Colonnes disponibles : {list(df.columns)}")

# ============================================================
# PRÉTRAITEMENT DES COLONNES
# ============================================================

# Identifier la colonne de commentaire
colonne_commentaire = None
for col in ['Commentaire_Client_Original', 'commentaire', 'texte', 'content', 'Commentaire_Client']:
    if col in df.columns:
        colonne_commentaire = col
        break

if colonne_commentaire is None:
    print("❌ Aucune colonne de commentaire trouvée !")
    print(f"Colonnes disponibles : {list(df.columns)}")
    exit()

print(f"📝 Colonne commentaire utilisée : '{colonne_commentaire}'")

# Convertir la date si présente
colonne_date = None
for col in ['date', 'Date', 'created_at', 'createdAt']:
    if col in df.columns:
        colonne_date = col
        break

if colonne_date:
    df[colonne_date] = pd.to_datetime(df[colonne_date], errors='coerce')
    df['date_seule'] = df[colonne_date].dt.date
    print(f"📅 Colonne date utilisée : '{colonne_date}'")

# ============================================================
# FONCTIONS DE NORMALISATION
# ============================================================

def normaliser_texte(texte):
    """Normalisation légère pour comparer les textes"""
    if not isinstance(texte, str):
        return ""
    texte = texte.lower()
    texte = texte.strip()
    return texte

# Appliquer la normalisation
df['texte_norm'] = df[colonne_commentaire].apply(normaliser_texte)
df['longueur'] = df[colonne_commentaire].astype(str).str.len()

# ============================================================
# INITIALISATION DU FICHIER EXCEL
# ============================================================

fichier_excel = "verification_regles_resultats.xlsx"
writer = pd.ExcelWriter(fichier_excel, engine='openpyxl')

print("\n" + "="*60)
print("EXTRACTION DES RÉSULTATS PAR RÈGLE")
print("="*60)

# ============================================================
# 1. VÉRIFICATION R1 : Doublons stricts
# ============================================================

print("\n📌 R1 : Recherche des doublons stricts...")

# Déterminer les colonnes pour R1
cols_r1 = ['texte_norm']
if colonne_date:
    cols_r1.append('date_seule')
if 'source' in df.columns:
    cols_r1.append('source')
if 'moderateur' in df.columns:
    cols_r1.append('moderateur')

if len(cols_r1) >= 2:
    # Trouver les groupes de doublons stricts
    groupes_r1 = df.groupby(cols_r1).size().reset_index(name='count')
    violations_r1 = groupes_r1[groupes_r1['count'] > 1]
    
    if len(violations_r1) > 0:
        # Extraire les détails des doublons R1
        resultats_r1 = []
        for _, row in violations_r1.iterrows():
            # Filtrer les lignes correspondantes
            mask = True
            for col in cols_r1:
                mask = mask & (df[col] == row[col])
            lignes_doublons = df[mask]
            
            for idx, ligne in lignes_doublons.iterrows():
                resultats_r1.append({
                    'regle': 'R1',
                    'description': 'Doublon strict (même texte, même date, même source, même modérateur)',
                    'commentaire': ligne[colonne_commentaire],
                    'date': ligne['date_seule'] if colonne_date else None,
                    'source': ligne.get('source', None),
                    'moderateur': ligne.get('moderateur', None),
                    'label': ligne.get('label', None),
                    'longueur': ligne['longueur'],
                    'nb_occurrences': row['count']
                })
        
        df_r1 = pd.DataFrame(resultats_r1)
        df_r1.to_excel(writer, sheet_name='R1_Doublons_Stricts', index=False)
        print(f"   ✅ {len(violations_r1)} groupes trouvés → {len(resultats_r1)} lignes extraites")
    else:
        df_r1 = pd.DataFrame({'message': ['Aucun doublon strict détecté (R1 OK)']})
        df_r1.to_excel(writer, sheet_name='R1_Doublons_Stricts', index=False)
        print("   ✅ Aucun doublon strict détecté")
else:
    print("   ⚠️ Colonnes manquantes pour R1")

# ============================================================
# 2. VÉRIFICATION R2 : Même texte, même jour, sources différentes
# ============================================================

print("\n📌 R2 : Recherche des textes sur plusieurs sources le même jour...")

if colonne_date and 'source' in df.columns:
    # Trouver les textes avec sources multiples le même jour
    groupes_r2 = df.groupby(['texte_norm', 'date_seule'])['source'].nunique().reset_index(name='nb_sources')
    cas_r2 = groupes_r2[groupes_r2['nb_sources'] > 1]
    
    if len(cas_r2) > 0:
        resultats_r2 = []
        for _, row in cas_r2.iterrows():
            # Extraire toutes les occurrences
            mask = (df['texte_norm'] == row['texte_norm']) & (df['date_seule'] == row['date_seule'])
            lignes = df[mask]
            
            for idx, ligne in lignes.iterrows():
                resultats_r2.append({
                    'regle': 'R2',
                    'description': 'Même texte, même jour, sources différentes',
                    'commentaire': ligne[colonne_commentaire],
                    'date': row['date_seule'],
                    'source': ligne.get('source', None),
                    'moderateur': ligne.get('moderateur', None),
                    'label': ligne.get('label', None),
                    'longueur': ligne['longueur'],
                    'nb_sources': row['nb_sources']
                })
        
        df_r2 = pd.DataFrame(resultats_r2)
        df_r2.to_excel(writer, sheet_name='R2_Sources_Differentes', index=False)
        print(f"   ✅ {len(cas_r2)} groupes trouvés → {len(resultats_r2)} lignes extraites")
    else:
        df_r2 = pd.DataFrame({'message': ['Aucun texte sur plusieurs sources le même jour (R2 OK)']})
        df_r2.to_excel(writer, sheet_name='R2_Sources_Differentes', index=False)
        print("   ✅ Aucun cas détecté")
else:
    print("   ⚠️ Colonnes manquantes pour R2")

# ============================================================
# 3. VÉRIFICATION R3 : Même texte, jours différents
# ============================================================

print("\n📌 R3 : Recherche des textes sur plusieurs jours...")

if colonne_date:
    # Trouver les textes qui apparaissent plusieurs jours
    groupes_r3 = df.groupby('texte_norm')['date_seule'].nunique().reset_index(name='nb_jours')
    cas_r3 = groupes_r3[groupes_r3['nb_jours'] > 1]
    
    if len(cas_r3) > 0:
        resultats_r3 = []
        for _, row in cas_r3.iterrows():
            # Extraire toutes les occurrences
            mask = (df['texte_norm'] == row['texte_norm'])
            lignes = df[mask]
            
            # Récupérer toutes les dates
            dates = lignes['date_seule'].unique()
            
            for idx, ligne in lignes.iterrows():
                resultats_r3.append({
                    'regle': 'R3',
                    'description': 'Même texte, jours différents',
                    'commentaire': ligne[colonne_commentaire],
                    'date': ligne['date_seule'],
                    'toutes_dates': str(list(dates)),
                    'source': ligne.get('source', None),
                    'moderateur': ligne.get('moderateur', None),
                    'label': ligne.get('label', None),
                    'longueur': ligne['longueur'],
                    'nb_jours': row['nb_jours']
                })
        
        df_r3 = pd.DataFrame(resultats_r3)
        df_r3.to_excel(writer, sheet_name='R3_Jours_Differents', index=False)
        print(f"   ✅ {len(cas_r3)} groupes trouvés → {len(resultats_r3)} lignes extraites")
    else:
        df_r3 = pd.DataFrame({'message': ['Aucun texte sur plusieurs jours (R3 OK)']})
        df_r3.to_excel(writer, sheet_name='R3_Jours_Differents', index=False)
        print("   ✅ Aucun cas détecté")
else:
    print("   ⚠️ Colonne date manquante pour R3")

# ============================================================
# 4. VÉRIFICATION R4 : Même texte, même jour, modérateurs différents
# ============================================================

print("\n📌 R4 : Recherche des textes avec modérateurs différents le même jour...")

if colonne_date and 'moderateur' in df.columns:
    # Trouver les textes avec modérateurs multiples le même jour
    groupes_r4 = df.groupby(['texte_norm', 'date_seule'])['moderateur'].nunique().reset_index(name='nb_moderateurs')
    cas_r4 = groupes_r4[groupes_r4['nb_moderateurs'] > 1]
    
    if len(cas_r4) > 0:
        resultats_r4 = []
        for _, row in cas_r4.iterrows():
            # Extraire toutes les occurrences
            mask = (df['texte_norm'] == row['texte_norm']) & (df['date_seule'] == row['date_seule'])
            lignes = df[mask]
            
            for idx, ligne in lignes.iterrows():
                resultats_r4.append({
                    'regle': 'R4',
                    'description': 'Même texte, même jour, modérateurs différents',
                    'commentaire': ligne[colonne_commentaire],
                    'date': row['date_seule'],
                    'source': ligne.get('source', None),
                    'moderateur': ligne.get('moderateur', None),
                    'label': ligne.get('label', None),
                    'longueur': ligne['longueur'],
                    'nb_moderateurs': row['nb_moderateurs']
                })
        
        df_r4 = pd.DataFrame(resultats_r4)
        df_r4.to_excel(writer, sheet_name='R4_Moderateurs_Differents', index=False)
        print(f"   ✅ {len(cas_r4)} groupes trouvés → {len(resultats_r4)} lignes extraites")
    else:
        df_r4 = pd.DataFrame({'message': ['Aucun texte avec modérateurs différents (R4 OK)']})
        df_r4.to_excel(writer, sheet_name='R4_Moderateurs_Differents', index=False)
        print("   ✅ Aucun cas détecté")
else:
    print("   ⚠️ Colonnes manquantes pour R4")

# ============================================================
# 5. VÉRIFICATION R5 : Texte tronqué vs complet
# ============================================================

print("\n📌 R5 : Recherche des versions tronquées...")

# Regrouper par texte normalisé
groupes_r5 = df.groupby('texte_norm')['longueur'].max().reset_index(name='longueur_max')

problemes_r5 = []
for texte_norm in df['texte_norm'].unique():
    groupe = df[df['texte_norm'] == texte_norm]
    longueur_max = groupe['longueur'].max()
    versions_courtes = groupe[groupe['longueur'] < longueur_max]
    
    if len(versions_courtes) > 0:
        # Prendre la version complète
        version_complete = groupe[groupe['longueur'] == longueur_max].iloc[0]
        
        for _, ligne in versions_courtes.iterrows():
            problemes_r5.append({
                'regle': 'R5',
                'description': 'Version tronquée vs complète',
                'commentaire_tronque': ligne[colonne_commentaire],
                'commentaire_complet': version_complete[colonne_commentaire],
                'longueur_tronque': ligne['longueur'],
                'longueur_complet': longueur_max,
                'date_tronque': ligne['date_seule'] if colonne_date else None,
                'date_complet': version_complete['date_seule'] if colonne_date else None,
                'source_tronque': ligne.get('source', None),
                'source_complet': version_complete.get('source', None),
                'label_tronque': ligne.get('label', None),
                'label_complet': version_complete.get('label', None)
            })

if len(problemes_r5) > 0:
    df_r5 = pd.DataFrame(problemes_r5)
    df_r5.to_excel(writer, sheet_name='R5_Textes_Tronques', index=False)
    print(f"   ✅ {len(problemes_r5)} versions tronquées trouvées")
else:
    df_r5 = pd.DataFrame({'message': ['Aucune version tronquée détectée (R5 OK)']})
    df_r5.to_excel(writer, sheet_name='R5_Textes_Tronques', index=False)
    print("   ✅ Aucune version tronquée détectée")

# ============================================================
# 6. RÉSUMÉ GLOBAL
# ============================================================

print("\n📌 Création du résumé global...")

resume = {
    'Collection': ['dataset_unifie_copie'],
    'Base de données': ['telecom_algerie_new'],
    'Total commentaires': [len(df)],
    'Commentaires uniques': [df['texte_norm'].nunique()],
    'Taux de compression (%)': [round((1 - df['texte_norm'].nunique()/len(df))*100, 2)],
    'R1 - Doublons stricts': [len(violations_r1) if 'violations_r1' in dir() else 0],
    'R2 - Sources multiples': [len(cas_r2) if 'cas_r2' in dir() else 0],
    'R3 - Jours différents': [len(cas_r3) if 'cas_r3' in dir() else 0],
    'R4 - Modérateurs différents': [len(cas_r4) if 'cas_r4' in dir() else 0],
    'R5 - Versions tronquées': [len(problemes_r5)]
}

df_resume = pd.DataFrame(resume)
df_resume.to_excel(writer, sheet_name='0_Resume_Global', index=False)
print("   ✅ Résumé global créé")

# ============================================================
# 7. LISTE DE TOUS LES DOUBLONS (GLOBAL)
# ============================================================

print("\n📌 Création de la liste globale des doublons...")

# Trouver tous les commentaires qui sont des doublons (apparaissent plus d'une fois)
doublons_mask = df.duplicated(subset=['texte_norm'], keep=False)
df_tous_doublons = df[doublons_mask].copy()
df_tous_doublons = df_tous_doublons.sort_values(['texte_norm', colonne_date if colonne_date else 'longueur'])

# Ajouter le nombre d'occurrences
df_tous_doublons['nb_occurrences'] = df_tous_doublons.groupby('texte_norm')['texte_norm'].transform('count')

# Sélectionner les colonnes importantes
cols_export = ['texte_norm', colonne_commentaire, 'longueur', 'nb_occurrences']
if colonne_date:
    cols_export.append('date_seule')
if 'source' in df.columns:
    cols_export.append('source')
if 'moderateur' in df.columns:
    cols_export.append('moderateur')
if 'label' in df.columns:
    cols_export.append('label')

df_tous_doublons = df_tous_doublons[[c for c in cols_export if c in df_tous_doublons.columns]]
df_tous_doublons.to_excel(writer, sheet_name='Tous_Les_Doublons', index=False)

print(f"   ✅ {len(df_tous_doublons)} commentaires doublons extraits")

# ============================================================
# SAUVEGARDER LE FICHIER EXCEL
# ============================================================

writer.close()
print(f"\n" + "="*60)
print(f"✅ Fichier Excel sauvegardé : {fichier_excel}")
print("="*60)

print(f"""
📋 Feuilles disponibles dans le fichier :
   - 0_Resume_Global : Résumé des statistiques
   - R1_Doublons_Stricts : Doublons stricts (R1)
   - R2_Sources_Differentes : Textes sur plusieurs sources (R2)
   - R3_Jours_Differents : Textes sur plusieurs jours (R3)
   - R4_Moderateurs_Differents : Textes avec modérateurs différents (R4)
   - R5_Textes_Tronques : Versions tronquées vs complètes (R5)
   - Tous_Les_Doublons : Liste globale de tous les doublons
""")

# ============================================================
# FERMER LA CONNEXION
# ============================================================

client.close()
print("🔒 Connexion MongoDB fermée")

# from pymongo import MongoClient
# import pandas as pd
# import re
# from difflib import SequenceMatcher

# # ============================================================
# # CONNEXION À MONGODB ATLAS
# # ============================================================

# MONGO_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# print("🔌 Connexion à MongoDB Atlas...")
# client = MongoClient(MONGO_URI)

# db = client["telecom_algerie_new"]
# collection = db["dataset_unifie_copie"]

# print("✅ Connexion réussie !")

# # ============================================================
# # CHARGEMENT DES DONNÉES
# # ============================================================

# print("\n📥 Chargement des données...")
# cursor = collection.find({})
# df = pd.DataFrame(list(cursor))

# print(f"✅ {len(df)} commentaires chargés")

# # ============================================================
# # IDENTIFIER LA COLONNE DE COMMENTAIRE
# # ============================================================

# colonne_commentaire = None
# for col in ['Commentaire_Client_Original', 'commentaire', 'text', 'content', 'Commentaire_Client']:
#     if col in df.columns:
#         colonne_commentaire = col
#         break

# if colonne_commentaire is None:
#     print("❌ Aucune colonne de commentaire trouvée !")
#     exit()

# print(f"📝 Colonne commentaire : '{colonne_commentaire}'")

# # ============================================================
# # FONCTIONS DE NORMALISATION
# # ============================================================

# def normaliser_texte(texte):
#     """Normalisation pour comparer les textes"""
#     if not isinstance(texte, str):
#         return ""
#     texte = texte.lower()
#     texte = re.sub(r'[.!?،;:()"\']', '', texte)
#     texte = re.sub(r'\s+', ' ', texte).strip()
#     return texte

# def similarite_textes(t1, t2):
#     """Calcule la similarité entre deux textes (0 à 1)"""
#     if not t1 or not t2:
#         return 0
#     return SequenceMatcher(None, t1, t2).ratio()

# # Appliquer la normalisation
# df['texte_norm'] = df[colonne_commentaire].apply(normaliser_texte)
# df['longueur'] = df[colonne_commentaire].astype(str).str.len()

# # ============================================================
# # VÉRIFICATION R5 : TEXTE TRONQUÉ VS COMPLET
# # ============================================================

# print("\n" + "="*70)
# print("VÉRIFICATION R5 : TEXTE TRONQUÉ VS COMPLET")
# print("="*70)

# # Regrouper par texte normalisé
# groupes = df.groupby('texte_norm')

# resultats_r5 = []
# statistiques = {
#     'total_groupes': 0,
#     'groupes_avec_troncatures': 0,
#     'total_versions_tronquees': 0,
#     'total_versions_completes': 0
# }

# for texte_norm, groupe in groupes:
#     statistiques['total_groupes'] += 1
    
#     if len(groupe) == 1:
#         continue
    
#     # Trouver la version la plus longue (complète)
#     longueur_max = groupe['longueur'].max()
#     version_complete = groupe[groupe['longueur'] == longueur_max].iloc[0]
    
#     # Trouver les versions plus courtes (tronquées)
#     versions_courtes = groupe[groupe['longueur'] < longueur_max]
    
#     if len(versions_courtes) > 0:
#         statistiques['groupes_avec_troncatures'] += 1
#         statistiques['total_versions_completes'] += 1
#         statistiques['total_versions_tronquees'] += len(versions_courtes)
        
#         for _, version_courte in versions_courtes.iterrows():
#             # Calculer la similarité
#             sim = similarite_textes(
#                 version_courte[colonne_commentaire],
#                 version_complete[colonne_commentaire]
#             )
            
#             # Vérifier si la version courte est un préfixe/suffixe
#             texte_court = version_courte[colonne_commentaire]
#             texte_long = version_complete[colonne_commentaire]
            
#             est_prefixe = texte_long.startswith(texte_court) if texte_court else False
#             est_suffixe = texte_long.endswith(texte_court) if texte_court else False
#             est_contenu = texte_court in texte_long if texte_court else False
            
#             resultats_r5.append({
#                 'groupe_id': texte_norm[:50],
#                 'type': 'VERSION_COMPLETE',
#                 'commentaire': version_complete[colonne_commentaire],
#                 'longueur': version_complete['longueur'],
#                 'date': version_complete.get('date', None),
#                 'source': version_complete.get('source', None),
#                 'moderateur': version_complete.get('moderateur', None),
#                 'label': version_complete.get('label', None),
#                 'est_prefixe': None,
#                 'est_suffixe': None,
#                 'est_contenu': None,
#                 'similarite': None
#             })
            
#             resultats_r5.append({
#                 'groupe_id': texte_norm[:50],
#                 'type': 'VERSION_TRONQUEE',
#                 'commentaire': version_courte[colonne_commentaire],
#                 'longueur': version_courte['longueur'],
#                 'date': version_courte.get('date', None),
#                 'source': version_courte.get('source', None),
#                 'moderateur': version_courte.get('moderateur', None),
#                 'label': version_courte.get('label', None),
#                 'est_prefixe': est_prefixe,
#                 'est_suffixe': est_suffixe,
#                 'est_contenu': est_contenu,
#                 'similarite': round(sim, 3)
#             })

# # ============================================================
# # AFFICHAGE DES STATISTIQUES
# # ============================================================

# print(f"\n📊 STATISTIQUES R5 :")
# print(f"   - Groupes de commentaires similaires : {statistiques['total_groupes']}")
# print(f"   - Groupes avec troncatures : {statistiques['groupes_avec_troncatures']}")
# print(f"   - Versions complètes : {statistiques['total_versions_completes']}")
# print(f"   - Versions tronquées : {statistiques['total_versions_tronquees']}")

# if statistiques['groupes_avec_troncatures'] > 0:
#     print(f"\n⚠️ {statistiques['groupes_avec_troncatures']} groupes ont des versions tronquées !")
# else:
#     print("\n✅ R5 OK : Aucune version tronquée détectée")

# # ============================================================
# # AFFICHAGE DES EXEMPLES
# # ============================================================

# if len(resultats_r5) > 0:
#     print("\n" + "="*70)
#     print("EXEMPLES DE TRONCATURES DÉTECTÉES")
#     print("="*70)
    
#     # Grouper par groupe pour afficher complet + tronqué
#     groupes_affichage = {}
#     for r in resultats_r5:
#         if r['groupe_id'] not in groupes_affichage:
#             groupes_affichage[r['groupe_id']] = []
#         groupes_affichage[r['groupe_id']].append(r)
    
#     nb_exemples = 0
#     for groupe_id, items in groupes_affichage.items():
#         if nb_exemples >= 5:
#             break
        
#         complete = next((i for i in items if i['type'] == 'VERSION_COMPLETE'), None)
#         tronquees = [i for i in items if i['type'] == 'VERSION_TRONQUEE']
        
#         if complete and tronquees:
#             print(f"\n📝 Groupe : '{groupe_id}...'")
#             print(f"\n   ✅ Version COMPLÈTE ({complete['longueur']} caractères) :")
#             print(f"      {complete['commentaire'][:100]}...")
            
#             for t in tronquees[:2]:
#                 print(f"\n   ❌ Version TRONQUÉE ({t['longueur']} caractères) :")
#                 print(f"      {t['commentaire'][:100]}...")
#                 print(f"      → Prefixe : {t['est_prefixe']} | Contenu : {t['est_contenu']} | Similarité : {t['similarite']}")
            
#             nb_exemples += 1

# # ============================================================
# # ANALYSE DES INCOHÉRENCES DE LABELS
# # ============================================================

# print("\n" + "="*70)
# print("ANALYSE DES INCOHÉRENCES DE LABELS ENTRE VERSIONS")
# print("="*70)

# incoherences = []
# groupes_par_texte = df.groupby('texte_norm')

# for texte_norm, groupe in groupes_par_texte:
#     if len(groupe) > 1 and 'label' in df.columns:
#         labels = groupe['label'].unique()
#         if len(labels) > 1:
#             # Trouver la version complète et les tronquées
#             longueur_max = groupe['longueur'].max()
#             version_complete = groupe[groupe['longueur'] == longueur_max].iloc[0]
#             versions_courtes = groupe[groupe['longueur'] < longueur_max]
            
#             for _, vc in versions_courtes.iterrows():
#                 if vc.get('label') != version_complete.get('label'):
#                     incoherences.append({
#                         'texte': texte_norm[:50],
#                         'label_complet': version_complete.get('label'),
#                         'label_tronque': vc.get('label'),
#                         'commentaire_complet': version_complete[colonne_commentaire][:80],
#                         'commentaire_tronque': vc[colonne_commentaire][:80]
#                     })

# if len(incoherences) > 0:
#     print(f"\n⚠️ {len(incoherences)} incohérences de labels détectées :")
#     for inc in incoherences[:5]:
#         print(f"\n   Texte : '{inc['texte']}...'")
#         print(f"   Label COMPLET : {inc['label_complet']}")
#         print(f"   Label TRONQUÉ : {inc['label_tronque']}")
#         print(f"   Complet : {inc['commentaire_complet']}...")
#         print(f"   Tronqué : {inc['commentaire_tronque']}...")
# else:
#     print("\n✅ Aucune incohérence de labels entre versions complètes et tronquées")

# # ============================================================
# # SAUVEGARDE DES RÉSULTATS
# # ============================================================

# if len(resultats_r5) > 0:
#     # Sauvegarder tous les résultats
#     df_resultats = pd.DataFrame(resultats_r5)
#     df_resultats.to_excel("verification_R5_complete2.xlsx", index=False)
#     print(f"\n✅ Résultats complets sauvegardés dans 'verification_R5_complete2.xlsx'")
    
#     # Sauvegarder le résumé
#     df_resume = pd.DataFrame([statistiques])
#     df_resume.to_excel("verification_R5_resume2.xlsx", index=False)
#     print(f"✅ Résumé sauvegardé dans 'verification_R5_resume2.xlsx'")
    
#     # Sauvegarder les incohérences
#     if len(incoherences) > 0:
#         df_incoherences = pd.DataFrame(incoherences)
#         df_incoherences.to_excel("verification_R5_incoherences2.xlsx", index=False)
#         print(f"✅ Incohérences sauvegardées dans 'verification_R5_incoherences2.xlsx'")
# else:
#     print("\n✅ Aucune version tronquée trouvée - R5 est correctement appliquée")

# # ============================================================
# # RECOMMANDATIONS
# # ============================================================

# print("\n" + "="*70)
# print("RECOMMANDATIONS")
# print("="*70)

# if statistiques['groupes_avec_troncatures'] > 0:
#     print("""
# ⚠️ Des versions tronquées ont été détectées. Voici ce que vous devez faire :

#    1. Pour chaque groupe, gardez UNIQUEMENT la version la PLUS LONGUE (complète)
#    2. Supprimez les versions tronquées
#    3. Si les labels diffèrent, prenez le label de la version complète
#    4. Après correction, re-vérifiez avec ce script

# 📝 Action recommandée :
#    - Ouvrez 'verification_R5_complete.xlsx'
#    - Pour chaque groupe, gardez la ligne 'VERSION_COMPLETE'
#    - Supprimez les lignes 'VERSION_TRONQUEE'
# """)
# else:
#     print("""
# ✅ R5 est correctement appliquée :
#    - Aucune version tronquée détectée
#    - Vous pouvez continuer avec la normalisation
# """)

# # ============================================================
# # FERMETURE
# # ============================================================

# client.close()
# print("\n🔒 Connexion MongoDB fermée")