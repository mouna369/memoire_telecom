"""
SCRIPT 2 — Comparaison des annotations entre MOI et CAMARADE
Utilise le texte original comme clé de correspondance.
"""

import pandas as pd
import numpy as np
from sklearn.metrics import cohen_kappa_score, confusion_matrix

# ═══════════════════════════════════════════════════════════
# CONFIGURATION — à adapter
# ═══════════════════════════════════════════════════════════
FICHIER_MOI      = "review_CAMARADE_mzl.xlsx"
FICHIER_CAMARADE = "review_CAMARADE.xlsx"
SHEET_NAME       = "📝 A corriger"
COLONNE_TEXTE    = "Texte original"          # colonne utilisée pour la correspondance
COLONNE_LABEL    = "⭐ label_corrige — REMPLIR ICI"
OUTPUT_REPORT    = "rapport_comparaison_texte.xlsx"

# ═══════════════════════════════════════════════════════════

def load_and_clean(file_path):
    """Charge le fichier Excel et nettoie les colonnes texte et label."""
    df = pd.read_excel(file_path, sheet_name=SHEET_NAME, header=None)
    
    # Les en-têtes sont à la ligne 4 (index 3) car les 3 premières lignes sont titres/légende
    # Mais pour être robuste, on cherche la ligne qui contient "Texte original"
    header_row_idx = None
    for i in range(10):
        row = df.iloc[i].astype(str).tolist()
        if any("Texte original" in cell for cell in row):
            header_row_idx = i
            break
    
    if header_row_idx is None:
        raise ValueError("Impossible de trouver la ligne d'en-tête contenant 'Texte original'")
    
    # Définir les en-têtes
    df.columns = df.iloc[header_row_idx]
    # Garder les données après l'en-tête
    df = df.iloc[header_row_idx+1:].copy()
    df.reset_index(drop=True, inplace=True)
    
    # Nettoyer le texte original (normaliser pour la correspondance)
    if COLONNE_TEXTE not in df.columns:
        raise ValueError(f"Colonne '{COLONNE_TEXTE}' introuvable")
    
    # Convertir le texte en string et nettoyer les espaces
    df[COLONNE_TEXTE] = df[COLONNE_TEXTE].astype(str).str.strip()
    
    # Nettoyer la colonne label
    if COLONNE_LABEL not in df.columns:
        raise ValueError(f"Colonne '{COLONNE_LABEL}' introuvable")
    
    df[COLONNE_LABEL] = df[COLONNE_LABEL].astype(str).str.strip().str.lower()
    df[COLONNE_LABEL] = df[COLONNE_LABEL].replace(['nan', 'none', ''], None)
    
    return df[[COLONNE_TEXTE, COLONNE_LABEL]]

def main():
    print("="*60)
    print("  COMPARAISON DES ANNOTATIONS MOI vs CAMARADE (par texte original)")
    print("="*60)
    
    print(f"\n📂 Chargement de {FICHIER_MOI}...")
    df_moi = load_and_clean(FICHIER_MOI)
    print(f"   → {len(df_moi)} lignes")
    
    print(f"📂 Chargement de {FICHIER_CAMARADE}...")
    df_camarade = load_and_clean(FICHIER_CAMARADE)
    print(f"   → {len(df_camarade)} lignes")
    
    # Fusion sur le texte original (inner join)
    merged = pd.merge(df_moi, df_camarade, on=COLONNE_TEXTE, suffixes=('_moi', '_camarade'))
    print(f"\n🔗 Fusion sur le texte : {len(merged)} commentaires communs")
    
    # Supprimer les lignes où un label est manquant
    merged_annotated = merged.dropna(subset=[f"{COLONNE_LABEL}_moi", f"{COLONNE_LABEL}_camarade"])
    print(f"📝 Annotations valides des deux côtés : {len(merged_annotated)}")
    
    if len(merged_annotated) == 0:
        print("❌ Aucune annotation commune à comparer.")
        return
    
    y_moi = merged_annotated[f"{COLONNE_LABEL}_moi"].tolist()
    y_cam = merged_annotated[f"{COLONNE_LABEL}_camarade"].tolist()
    
    # Calculs
    accord = sum(1 for a, b in zip(y_moi, y_cam) if a == b)
    taux_accord = accord / len(y_moi) * 100
    
    try:
        kappa = cohen_kappa_score(y_moi, y_cam)
    except Exception as e:
        kappa = None
        print(f"⚠️ Erreur calcul kappa : {e}")
    
    labels_unique = sorted(set(y_moi + y_cam))
    cm = confusion_matrix(y_moi, y_cam, labels=labels_unique)
    
    print("\n" + "="*60)
    print("📊 RÉSULTATS DE L'ACCORD INTER-ANNOTATEUR")
    print("="*60)
    print(f"✅ Taux d'accord : {taux_accord:.2f}% ({accord}/{len(y_moi)})")
    if kappa is not None:
        print(f"📈 Cohen's Kappa : {kappa:.4f}")
        if kappa < 0:
            interp = "Très faible (désaccord)"
        elif kappa < 0.2:
            interp = "Très faible"
        elif kappa < 0.4:
            interp = "Faible"
        elif kappa < 0.6:
            interp = "Modérée"
        elif kappa < 0.8:
            interp = "Forte"
        else:
            interp = "Presque parfaite"
        print(f"   → Interprétation : {interp}")
    
    print("\n📋 Matrice de confusion (lignes = toi, colonnes = camarade) :")
    print("   " + " ".join(f"{l:>10}" for l in labels_unique))
    for i, label in enumerate(labels_unique):
        print(f"{label:10} " + " ".join(f"{cm[i][j]:>10}" for j in range(len(labels_unique))))
    
    # Exporter les désaccords
    desaccords = merged_annotated[merged_annotated[f"{COLONNE_LABEL}_moi"] != merged_annotated[f"{COLONNE_LABEL}_camarade"]]
    if not desaccords.empty:
        print(f"\n⚠️ {len(desaccords)} désaccords détectés.")
        with pd.ExcelWriter(OUTPUT_REPORT, engine='openpyxl') as writer:
            desaccords.to_excel(writer, sheet_name="Désaccords", index=False)
            summary = pd.DataFrame({
                "Métrique": ["Taux d'accord", "Cohen's Kappa", "Nombre de paires", "Accords", "Désaccords"],
                "Valeur": [f"{taux_accord:.2f}%", f"{kappa:.4f}" if kappa else "N/A", len(y_moi), accord, len(desaccords)]
            })
            summary.to_excel(writer, sheet_name="Résumé", index=False)
        print(f"💾 Rapport sauvegardé dans '{OUTPUT_REPORT}'")
    else:
        print("\n🎉 Aucun désaccord ! Parfait.")

if __name__ == "__main__":
    main()