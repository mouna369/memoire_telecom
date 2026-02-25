# scripts/nettoyage/02_supprimer_mentions.py
import pandas as pd
import re
import os

print("="*60)
print("🔍 ÉTAPE 2 : DÉTECTION ET SUPPRESSION DES MENTIONS @")
print("="*60)

# 1. Charger le fichier CSV généré à l'étape 1
print("\n📂 Chargement du fichier sans URLs...")
input_file = "donnees/resultats/donnees_sans_urls.csv"

if not os.path.exists(input_file):
    print(f"❌ Fichier non trouvé : {input_file}")
    print("   Exécute d'abord 01_supprimer_urls.py")
    exit(1)

df = pd.read_csv(input_file, encoding='utf-8-sig')
print(f"✅ {len(df)} commentaires chargés")

# 2. Identifier la colonne des commentaires
colonne_commentaire = None
for col_name in df.columns:
    if 'commentaire' in str(col_name).lower():
        colonne_commentaire = col_name
        break

if colonne_commentaire is None:
    colonne_commentaire = df.columns[0]
    print(f"⚠️ Colonne non trouvée, utilisation de: {colonne_commentaire}")
else:
    print(f"📋 Colonne analysée : {colonne_commentaire}")

# 3. Fonction pour détecter les mentions
def detecter_mentions(texte):
    """Détecte les mentions @ dans un texte"""
    if pd.isna(texte):
        return 0, ""
    texte = str(texte)
    pattern = r'@\w+'
    mentions = re.findall(pattern, texte)
    return len(mentions), ", ".join(mentions)

# 4. ANALYSE : Détecter les mentions
print("\n🔎 ANALYSE : Recherche des mentions @...")

mentions_info = df[colonne_commentaire].apply(detecter_mentions)
df['nb_mentions'] = mentions_info.apply(lambda x: x[0])
df['mentions_trouvees'] = mentions_info.apply(lambda x: x[1])

# Compter les commentaires avec mentions
nb_total = len(df)
nb_avec_mentions = (df['nb_mentions'] > 0).sum()
nb_sans_mentions = nb_total - nb_avec_mentions
pourcentage = (nb_avec_mentions / nb_total * 100) if nb_total > 0 else 0

print(f"\n📊 STATISTIQUES DES MENTIONS @:")
print(f"   - Total commentaires : {nb_total}")
print(f"   - Commentaires avec mentions : {nb_avec_mentions} ({pourcentage:.2f}%)")
print(f"   - Commentaires sans mentions : {nb_sans_mentions}")

# 5. AFFICHER les commentaires avec mentions
if nb_avec_mentions > 0:
    print(f"\n📝 COMMENTAIRES AVEC MENTIONS TROUVÉES:")
    df_mentions = df[df['nb_mentions'] > 0]
    
    for idx, row in df_mentions.head(10).iterrows():
        print(f"\n   Ligne {idx + 2}:")
        print(f"   Texte: {row[colonne_commentaire][:100]}...")
        print(f"   Mentions: {row['mentions_trouvees']}")

# 6. Fonction pour supprimer les mentions
def supprimer_mentions(texte):
    """Supprime les mentions @ d'un texte"""
    if pd.isna(texte):
        return ""
    texte = str(texte)
    pattern = r'@\w+'
    return re.sub(pattern, '', texte)

# 7. SUPPRESSION des mentions
print("\n🧹 SUPPRESSION DES MENTIONS...")
df['commentaire_sans_mentions'] = df[colonne_commentaire].apply(supprimer_mentions)

# 8. VÉRIFICATION
print("🔎 VÉRIFICATION...")

df['verification_mentions'] = df['commentaire_sans_mentions'].apply(
    lambda x: len(re.findall(r'@\w+', str(x)))
)
nb_reste = (df['verification_mentions'] > 0).sum()

if nb_reste == 0:
    print("✅ SUCCÈS : Toutes les mentions ont été supprimées !")
else:
    print(f"⚠️ ATTENTION : Il reste {nb_reste} commentaires avec mentions")

# 9. CRÉER LE FICHIER FINAL
print("\n💾 Création du fichier CSV final...")

# Remplacer la colonne originale par la version nettoyée
df[colonne_commentaire] = df['commentaire_sans_mentions']

# Garder seulement les colonnes originales
colonnes_a_garder = [c for c in df.columns if not c.startswith(('nb_', 'mentions_', 'verification_', 'commentaire_sans_'))]
df_final = df[colonnes_a_garder]

# Sauvegarder en CSV
output_file = "donnees/resultats/donnees_sans_mentions.csv"
df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"✅ Fichier CSV créé : {output_file}")

# 10. EXEMPLES AVANT/APRÈS
print("\n📊 EXEMPLES AVANT/APRÈS SUPPRESSION:")
if nb_avec_mentions > 0:
    exemples = df[df['nb_mentions'] > 0].head(3)
    for idx, row in exemples.iterrows():
        print(f"\n   AVANT: {row[colonne_commentaire][:100]}...")
        print(f"   APRÈS: {row['commentaire_sans_mentions'][:100]}...")
else:
    print("   Aucun exemple avec mentions")

# 11. RAPPORT
print("\n📄 Création du rapport...")
with open("donnees/resultats/rapport_mentions.txt", "w", encoding="utf-8") as f:
    f.write("="*60 + "\n")
    f.write("RAPPORT DE DÉTECTION ET SUPPRESSION DES MENTIONS @\n")
    f.write("="*60 + "\n\n")
    f.write(f"Fichier source : donnees_sans_urls.csv\n")
    f.write(f"Total commentaires : {nb_total}\n")
    f.write(f"Commentaires avec mentions : {nb_avec_mentions}\n")
    f.write(f"Pourcentage : {pourcentage:.2f}%\n")
    f.write(f"Mentions supprimées avec succès : {'OUI' if nb_reste==0 else 'NON'}\n")
    f.write(f"\nFichier créé : donnees_sans_mentions.csv\n")

print("\n" + "="*60)
print("📊 RÉSUMÉ FINAL")
print("="*60)
print(f"✅ {nb_avec_mentions} commentaires avec mentions ont été traités")
print(f"✅ Fichier créé : donnees/resultats/donnees_sans_mentions.csv")
print(f"✅ Prochaine étape : utilises ce fichier pour la suite")
print("="*60)

print("\n🎉 ÉTAPE 2 TERMINÉE !")