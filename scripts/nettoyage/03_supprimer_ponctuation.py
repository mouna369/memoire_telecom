# scripts/nettoyage/03_supprimer_ponctuation.py
import pandas as pd
import re
import os

print("="*60)
print("🔍 ÉTAPE 3 : DÉTECTION ET SUPPRESSION DE LA PONCTUATION")
print("="*60)

# 1. Charger le fichier CSV généré à l'étape 2
print("\n📂 Chargement du fichier sans mentions...")
input_file = "donnees/resultats/donnees_sans_mentions.csv"

if not os.path.exists(input_file):
    print(f"❌ Fichier non trouvé : {input_file}")
    print("   Exécute d'abord 02_supprimer_mentions.py")
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

# 3. Fonction pour détecter la ponctuation
def detecter_ponctuation(texte):
    """Détecte la ponctuation dans un texte"""
    if pd.isna(texte):
        return 0, ""
    texte = str(texte)
    # Ponctuation à détecter : ! ? . , ; : " ' ( ) [ ] { } - _ + = * / \ | < > 
    pattern = r'[!?.,;:\"\'()\[\]{}\-_+=\*/\\|<>]'
    ponctuation = re.findall(pattern, texte)
    # Compter le nombre total de caractères de ponctuation
    nb_ponctuation = len(ponctuation)
    # Afficher les 10 premiers pour l'aperçu
    ponctuation_uniques = list(set(ponctuation))[:10]
    return nb_ponctuation, " ".join(ponctuation_uniques)

# 4. ANALYSE : Détecter la ponctuation
print("\n🔎 ANALYSE : Recherche de la ponctuation...")

ponctuation_info = df[colonne_commentaire].apply(detecter_ponctuation)
df['nb_ponctuation'] = ponctuation_info.apply(lambda x: x[0])
df['types_ponctuation'] = ponctuation_info.apply(lambda x: x[1])

# Compter les commentaires avec ponctuation
nb_total = len(df)
nb_avec_ponctuation = (df['nb_ponctuation'] > 0).sum()
nb_sans_ponctuation = nb_total - nb_avec_ponctuation
pourcentage = (nb_avec_ponctuation / nb_total * 100) if nb_total > 0 else 0

print(f"\n📊 STATISTIQUES DE LA PONCTUATION:")
print(f"   - Total commentaires : {nb_total}")
print(f"   - Commentaires avec ponctuation : {nb_avec_ponctuation} ({pourcentage:.2f}%)")
print(f"   - Commentaires sans ponctuation : {nb_sans_ponctuation}")

# Afficher le total de caractères de ponctuation
total_ponctuation = df['nb_ponctuation'].sum()
print(f"   - Total caractères de ponctuation : {total_ponctuation}")

# 5. AFFICHER les commentaires avec beaucoup de ponctuation
if nb_avec_ponctuation > 0:
    print(f"\n📝 EXEMPLES DE COMMENTAIRES AVEC PONCTUATION:")
    df_ponctuation = df[df['nb_ponctuation'] > 0].sort_values('nb_ponctuation', ascending=False)
    
    for idx, row in df_ponctuation.head(5).iterrows():
        print(f"\n   Ligne {idx + 2}:")
        print(f"   Texte: {row[colonne_commentaire][:100]}...")
        print(f"   Nombre de ponctuation: {row['nb_ponctuation']}")
        print(f"   Types: {row['types_ponctuation']}")

# 6. Fonction pour supprimer la ponctuation (en préservant l'arabe)
def supprimer_ponctuation(texte):
    """
    Supprime la ponctuation d'un texte
    Garde les lettres arabes, françaises et les espaces
    """
    if pd.isna(texte):
        return ""
    texte = str(texte)
    # Garder : lettres arabes (\u0600-\u06FF), lettres françaises (a-zA-Z), espaces (\s)
    # Supprimer tout le reste
    texte = re.sub(r'[^\w\s\u0600-\u06FFa-zA-Z]', ' ', texte)
    return texte

# 7. SUPPRESSION de la ponctuation
print("\n🧹 SUPPRESSION DE LA PONCTUATION...")
df['commentaire_sans_ponctuation'] = df[colonne_commentaire].apply(supprimer_ponctuation)

# 8. VÉRIFICATION
print("🔎 VÉRIFICATION...")

# Vérifier s'il reste de la ponctuation
df['verification_ponctuation'] = df['commentaire_sans_ponctuation'].apply(
    lambda x: len(re.findall(r'[!?.,;:\"\'()\[\]{}\-_+=\*/\\|<>]', str(x)))
)
nb_reste = (df['verification_ponctuation'] > 0).sum()

if nb_reste == 0:
    print("✅ SUCCÈS : Toute la ponctuation a été supprimée !")
else:
    print(f"⚠️ ATTENTION : Il reste de la ponctuation dans {nb_reste} commentaires")

# 9. CRÉER LE FICHIER FINAL
print("\n💾 Création du fichier CSV final...")

# Remplacer la colonne originale par la version nettoyée
df[colonne_commentaire] = df['commentaire_sans_ponctuation']

# Garder seulement les colonnes originales
colonnes_a_garder = [c for c in df.columns 
                     if not c.startswith(('nb_', 'types_', 'verification_', 'commentaire_sans_'))]
df_final = df[colonnes_a_garder]

# Sauvegarder en CSV
output_file = "donnees/resultats/donnees_sans_ponctuation.csv"
df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"✅ Fichier CSV créé : {output_file}")

# 10. EXEMPLES AVANT/APRÈS
print("\n📊 EXEMPLES AVANT/APRÈS SUPPRESSION:")
if nb_avec_ponctuation > 0:
    exemples = df[df['nb_ponctuation'] > 0].head(3)
    for idx, row in exemples.iterrows():
        print(f"\n   AVANT: {row[colonne_commentaire][:100]}...")
        print(f"   APRÈS: {row['commentaire_sans_ponctuation'][:100]}...")
else:
    print("   Aucun exemple avec ponctuation")

# 11. RAPPORT
print("\n📄 Création du rapport...")
with open("donnees/resultats/rapport_ponctuation.txt", "w", encoding="utf-8") as f:
    f.write("="*60 + "\n")
    f.write("RAPPORT DE DÉTECTION ET SUPPRESSION DE LA PONCTUATION\n")
    f.write("="*60 + "\n\n")
    f.write(f"Fichier source : donnees_sans_mentions.csv\n")
    f.write(f"Total commentaires : {nb_total}\n")
    f.write(f"Commentaires avec ponctuation : {nb_avec_ponctuation}\n")
    f.write(f"Pourcentage : {pourcentage:.2f}%\n")
    f.write(f"Total caractères de ponctuation supprimés : {total_ponctuation}\n")
    f.write(f"Ponctuation supprimée avec succès : {'OUI' if nb_reste==0 else 'NON'}\n")
    f.write(f"\nFichier créé : donnees_sans_ponctuation.csv\n")

print("\n" + "="*60)
print("📊 RÉSUMÉ FINAL")
print("="*60)
print(f"✅ {nb_avec_ponctuation} commentaires avec ponctuation ont été traités")
print(f"✅ {total_ponctuation} caractères de ponctuation supprimés")
print(f"✅ Fichier créé : donnees/resultats/donnees_sans_ponctuation.csv")
print(f"✅ Prochaine étape : suppression des chiffres")
print("="*60)

print("\n🎉 ÉTAPE 3 TERMINÉE !")