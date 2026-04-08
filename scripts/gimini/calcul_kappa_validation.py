"""
╔══════════════════════════════════════════════════════════════════╗
║  FUSION + CALCUL COHEN'S KAPPA                                   ║
║  Pour les deux fichiers Excel de validation manuelle             ║
║                                                                  ║
║  Usage : python3 calcul_kappa_validation.py                      ║
╚══════════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import numpy as np
from sklearn.metrics import (
    cohen_kappa_score,
    classification_report,
    confusion_matrix,
    accuracy_score
)
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import glob
import os
import sys
from datetime import datetime

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 0 : Trouver automatiquement les fichiers Excel
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("\n" + "="*60)
print("  CALCUL COHEN'S KAPPA — VALIDATION MANUELLE")
print("="*60)

# Chercher les fichiers validation_*.xlsx dans le dossier courant
fichiers = glob.glob("validation_*.xlsx")

if len(fichiers) < 2:
    print("\n❌ Il faut au moins 2 fichiers validation_*.xlsx dans ce dossier")
    print("   Exemple : validation_sara_20260404_1541.xlsx")
    print("             validation_amina_20260404_1541.xlsx")
    sys.exit(1)

print(f"\n  Fichiers trouvés :")
for i, f in enumerate(fichiers):
    print(f"   [{i}] {f}")

# Sélectionner les deux fichiers
if len(fichiers) == 2:
    fichier_1 = fichiers[0]
    fichier_2 = fichiers[1]
else:
    idx1 = int(input("\n  Numéro du fichier annotateur 1 : "))
    idx2 = int(input("  Numéro du fichier annotateur 2 : "))
    fichier_1 = fichiers[idx1]
    fichier_2 = fichiers[idx2]

print(f"\n  Fichier 1 : {fichier_1}")
print(f"  Fichier 2 : {fichier_2}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 1 : Charger les deux fichiers Excel
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("\n  Chargement des fichiers...")

df1 = pd.read_excel(fichier_1, sheet_name="Validation")
df2 = pd.read_excel(fichier_2, sheet_name="Validation")

print(f"   Annotateur 1 : {len(df1)} tweets")
print(f"   Annotateur 2 : {len(df2)} tweets")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 2 : Nettoyer et préparer les données
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def nettoyer(df, nom):
    df = df.copy()

    # Renommer les colonnes importantes
    df.columns = [str(c).strip().upper() for c in df.columns]

    # Vérifier que les colonnes existent
    colonnes_requises = ["MONGO_ID", "LABEL_GEMINI", "MON_LABEL"]
    for col in colonnes_requises:
        if col not in df.columns:
            print(f"\n❌ Colonne '{col}' introuvable dans {nom}")
            print(f"   Colonnes disponibles : {list(df.columns)}")
            sys.exit(1)

    # Nettoyer les labels
    df["LABEL_GEMINI"] = df["LABEL_GEMINI"].str.strip().str.lower()
    df["MON_LABEL"]    = df["MON_LABEL"].str.strip().str.lower()

    # Supprimer les lignes non annotées (MON_LABEL vide)
    avant = len(df)
    df = df[df["MON_LABEL"].notna() & (df["MON_LABEL"] != "")]
    apres = len(df)
    if avant != apres:
        print(f"   ⚠️  {avant - apres} tweets sans annotation ignorés dans {nom}")

    # Supprimer les tweets marqués SUPPRIMER
    df = df[df["MON_LABEL"] != "supprimer"]

    return df

df1 = nettoyer(df1, fichier_1)
df2 = nettoyer(df2, fichier_2)

# Fusionner les deux fichiers
df_total = pd.concat([df1, df2], ignore_index=True)
print(f"\n  Total tweets annotés : {len(df_total)}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 3 : Garder seulement les labels valides pour le Kappa
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
labels_valides = ["negatif", "neutre", "positif"]

df_kappa = df_total[
    df_total["LABEL_GEMINI"].isin(labels_valides) &
    df_total["MON_LABEL"].isin(labels_valides)
].copy()

df_ambigu = df_total[df_total["MON_LABEL"] == "ambigu"]

print(f"  Tweets valides pour Kappa : {len(df_kappa)}")
print(f"  Tweets ambigus (exclus)   : {len(df_ambigu)}")

if len(df_kappa) == 0:
    print("\n❌ Aucun tweet valide pour calculer le Kappa")
    print("   Vérifiez que la colonne MON_LABEL est bien remplie")
    sys.exit(1)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 4 : Cohen's Kappa
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("\n" + "="*60)
print("  RESULTATS COHEN'S KAPPA")
print("="*60)

y_gemini = df_kappa["LABEL_GEMINI"].tolist()
y_human  = df_kappa["MON_LABEL"].tolist()

kappa    = cohen_kappa_score(y_human, y_gemini)
accuracy = accuracy_score(y_human, y_gemini)
erreurs  = len(df_kappa[df_kappa["LABEL_GEMINI"] != df_kappa["MON_LABEL"]])

print(f"\n  Cohen's Kappa  : {kappa:.4f}")
print(f"  Accuracy       : {accuracy*100:.2f}%")
print(f"  Taux d'erreur  : {(1-accuracy)*100:.2f}%")
print(f"  Erreurs totales: {erreurs} / {len(df_kappa)}")

# Interprétation
if kappa < 0.20:
    niveau = "FAIBLE ❌"
    conseil = "Revoir le prompt Gemini et re-annoter les cas problématiques"
elif kappa < 0.40:
    niveau = "PASSABLE ⚠️"
    conseil = "Corriger les erreurs et améliorer le prompt Gemini"
elif kappa < 0.60:
    niveau = "MODERE ✅"
    conseil = "Acceptable pour un mémoire — corriger les cas ambigus"
elif kappa < 0.80:
    niveau = "BON ✅✅"
    conseil = "Bonne fiabilité — corriger les erreurs détectées"
else:
    niveau = "EXCELLENT ✅✅✅"
    conseil = "Très haute fiabilité — annotation validée"

print(f"\n  Interprétation : {niveau}")
print(f"  Conseil        : {conseil}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 5 : Rapport détaillé par classe
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("\n" + "="*60)
print("  RAPPORT PAR CLASSE (Precision / Recall / F1)")
print("="*60)

report = classification_report(
    y_human, y_gemini,
    labels=labels_valides,
    target_names=["negatif", "neutre", "positif"]
)
print(report)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 6 : Matrice de confusion (image PNG)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
cm = confusion_matrix(y_human, y_gemini, labels=labels_valides)

fig, ax = plt.subplots(figsize=(8, 6))
sns.heatmap(
    cm, annot=True, fmt='d', cmap='Blues',
    xticklabels=labels_valides,
    yticklabels=labels_valides,
    ax=ax, linewidths=0.5, linecolor='gray'
)
ax.set_title(
    f"Matrice de Confusion — Gemini vs Annotation Manuelle\n"
    f"Kappa = {kappa:.3f}  |  Accuracy = {accuracy*100:.1f}%",
    fontsize=13, pad=15
)
ax.set_ylabel("Vérité terrain (Annotation manuelle)", fontsize=11)
ax.set_xlabel("Prédiction Gemini", fontsize=11)
plt.tight_layout()

ts = datetime.now().strftime("%Y%m%d_%H%M")
nom_matrice = f"matrice_confusion_{ts}.png"
plt.savefig(nom_matrice, dpi=150, bbox_inches='tight')
print(f"\n  Matrice de confusion sauvegardée : {nom_matrice}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 7 : Exporter les erreurs pour correction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
df_erreurs = df_kappa[df_kappa["LABEL_GEMINI"] != df_kappa["MON_LABEL"]].copy()
df_erreurs = df_erreurs[["MONGO_ID", "TEXTE_TWEET", "LABEL_GEMINI", "MON_LABEL"]].copy()
df_erreurs.columns = ["mongo_id", "texte", "gemini_label", "human_label"]

nom_erreurs = f"erreurs_a_corriger_{ts}.csv"
df_erreurs.to_csv(nom_erreurs, index=False, encoding="utf-8-sig")
print(f"  Erreurs exportées                : {nom_erreurs} ({len(df_erreurs)} tweets)")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 8 : Analyse des types d'erreurs
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("\n" + "="*60)
print("  TYPES D'ERREURS LES PLUS FREQUENTS")
print("="*60)

erreur_types = df_erreurs.groupby(
    ["gemini_label", "human_label"]
).size().reset_index(name="count").sort_values("count", ascending=False)

print()
for _, row in erreur_types.iterrows():
    print(f"  Gemini dit '{row['gemini_label']:8s}' → Manuel dit '{row['human_label']:8s}' : {row['count']} fois")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ETAPE 9 : Résumé final pour le mémoire
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print("\n" + "="*60)
print("  RESUME POUR VOTRE MEMOIRE")
print("="*60)
print(f"""
  Dataset total Gemini          : 25 415 commentaires
  Échantillon validé manuellement: {len(df_total)}  tweets
  ├── Tweets ambigus (exclus)   : {len(df_ambigu)}
  └── Tweets évalués (Kappa)    : {len(df_kappa)}

  Cohen's Kappa                 : {kappa:.4f}  → {niveau}
  Accuracy Gemini               : {accuracy*100:.2f}%
  Taux d'erreur                 : {(1-accuracy)*100:.2f}%
  Nombre d'erreurs à corriger   : {erreurs}

  Fichiers générés :
  ├── {nom_matrice}
  └── {nom_erreurs}
""")

print("="*60)
print("  DONE ✅")
print("="*60)
