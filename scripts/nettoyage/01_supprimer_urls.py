# scripts/nettoyage/01_supprimer_urls.py - VERSION AVEC CSV
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when
import pandas as pd
import re

def supprimer_urls(texte):
    """Supprime les URLs d'un texte"""
    if pd.isna(texte):
        return ""
    texte = str(texte)
    pattern = r'http[s]?://\S+|www\.\S+'
    return re.sub(pattern, '', texte)

print("="*60)
print("🔍 ÉTAPE 1 : DÉTECTION ET SUPPRESSION DES URLS")
print("="*60)

# 1. Créer Spark
spark = SparkSession.builder \
    .appName("Suppression_URLs") \
    .master("local[*]") \
    .getOrCreate()
print("✅ Spark démarré")

# 2. Charger les données depuis l'Excel directement
print("\n📂 Chargement depuis Excel...")
pandas_df = pd.read_excel("donnees/brutes/Social-Media-Analytics.xlsx", header=1)
print(f"✅ {len(pandas_df)} commentaires chargés")

# 3. Identifier la colonne des commentaires
colonne_commentaire = None
for col_name in pandas_df.columns:
    if 'commentaire' in str(col_name).lower():
        colonne_commentaire = col_name
        break

if colonne_commentaire is None:
    colonne_commentaire = pandas_df.columns[0]
    print(f"⚠️ Colonne non trouvée, utilisation de: {colonne_commentaire}")
else:
    print(f"📋 Colonne analysée : {colonne_commentaire}")

# 4. ANALYSE : Détecter les URLs
print("\n🔎 ANALYSE : Recherche des URLs...")

# Fonction pour détecter les URLs
def detecter_urls_texte(texte):
    if pd.isna(texte):
        return 0, ""
    texte = str(texte)
    pattern = r'http[s]?://\S+|www\.\S+'
    urls = re.findall(pattern, texte)
    return len(urls), " | ".join(urls)

# Appliquer la détection
urls_info = pandas_df[colonne_commentaire].apply(detecter_urls_texte)
pandas_df['nb_urls'] = urls_info.apply(lambda x: x[0])
pandas_df['urls_trouvees'] = urls_info.apply(lambda x: x[1])

# Compter les commentaires avec URLs
nb_total = len(pandas_df)
nb_avec_urls = (pandas_df['nb_urls'] > 0).sum()
nb_sans_urls = nb_total - nb_avec_urls
pourcentage = (nb_avec_urls / nb_total * 100) if nb_total > 0 else 0

print(f"\n📊 STATISTIQUES DES URLS:")
print(f"   - Total commentaires : {nb_total}")
print(f"   - Commentaires avec URLs : {nb_avec_urls} ({pourcentage:.2f}%)")
print(f"   - Commentaires sans URLs : {nb_sans_urls}")

# 5. AFFICHER les commentaires avec URLs
if nb_avec_urls > 0:
    print(f"\n📝 COMMENTAIRES AVEC URLS TROUVÉS:")
    df_urls = pandas_df[pandas_df['nb_urls'] > 0]
    
    for idx, row in df_urls.head(10).iterrows():
        print(f"\n   Ligne {idx + 2}:")
        print(f"   Texte: {row[colonne_commentaire][:100]}...")
        print(f"   URLs: {row['urls_trouvees']}")

# 6. SUPPRESSION des URLs
print("\n🧹 SUPPRESSION DES URLS...")
pandas_df['commentaire_sans_urls'] = pandas_df[colonne_commentaire].apply(supprimer_urls)

# 7. VÉRIFICATION
print("🔎 VÉRIFICATION...")

# Compter s'il reste des URLs
pandas_df['verification_urls'] = pandas_df['commentaire_sans_urls'].apply(
    lambda x: len(re.findall(r'http[s]?://\S+|www\.\S+', str(x)))
)
nb_reste = (pandas_df['verification_urls'] > 0).sum()

if nb_reste == 0:
    print("✅ SUCCÈS : Toutes les URLs ont été supprimées !")
else:
    print(f"⚠️ ATTENTION : Il reste {nb_reste} commentaires avec URLs")

# 8. CRÉER LE FICHIER FINAL (MÊME STRUCTURE QUE L'ORIGINAL)
print("\n💾 Création du fichier CSV final...")

# Garder les mêmes colonnes que l'original + la version nettoyée
colonnes_a_garder = list(pandas_df.columns)
# Enlever les colonnes temporaires
colonnes_temp = ['nb_urls', 'urls_trouvees', 'verification_urls']
colonnes_finales = [c for c in colonnes_a_garder if c not in colonnes_temp]

# Créer le DataFrame final avec la colonne originale remplacée par la version nettoyée
df_final = pandas_df.copy()
df_final[colonne_commentaire] = df_final['commentaire_sans_urls']
df_final = df_final[colonnes_finales]

# Sauvegarder en CSV
csv_path = "donnees/resultats/donnees_sans_urls.csv"
df_final.to_csv(csv_path, index=False, encoding='utf-8-sig')
print(f"✅ Fichier CSV créé : {csv_path}")

# 9. CRÉER UN RAPPORT
print("\n📄 Création du rapport...")
with open("donnees/resultats/rapport_urls.txt", "w", encoding="utf-8") as f:
    f.write("="*60 + "\n")
    f.write("RAPPORT DE DÉTECTION ET SUPPRESSION DES URLS\n")
    f.write("="*60 + "\n\n")
    f.write(f"Date : 2024-02-23\n")
    f.write(f"Fichier source : Social-Media-Analytics.xlsx\n")
    f.write(f"Total commentaires : {nb_total}\n")
    f.write(f"Commentaires avec URLs : {nb_avec_urls}\n")
    f.write(f"Pourcentage : {pourcentage:.2f}%\n")
    f.write(f"URLs supprimées avec succès : {'OUI' if nb_reste==0 else 'NON'}\n")
    f.write(f"\nFichier créé : donnees_sans_urls.csv\n")

print("✅ Rapport sauvegardé")

# 10. EXEMPLES AVANT/APRÈS
print("\n📊 EXEMPLES AVANT/APRÈS SUPPRESSION:")
if nb_avec_urls > 0:
    exemples = pandas_df[pandas_df['nb_urls'] > 0].head(3)
    for idx, row in exemples.iterrows():
        print(f"\n   AVANT: {row[colonne_commentaire][:100]}...")
        print(f"   APRÈS: {row['commentaire_sans_urls'][:100]}...")
else:
    print("   Aucun exemple avec URLs")

print("\n" + "="*60)
print("📊 RÉSUMÉ FINAL")
print("="*60)
print(f"✅ {nb_avec_urls} commentaires avec URLs ont été traités")
print(f"✅ Fichier créé : donnees/resultats/donnees_sans_urls.csv")
print(f"✅ Tu peux l'ouvrir avec Excel ou n'importe quel éditeur")
print("="*60)

print("\n🎉 ÉTAPE 1 TERMINÉE !")