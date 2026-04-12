#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metrics.py — Évaluation du modèle DziriBERT fine-tuné
Génère un rapport complet : accuracy, F1, matrice de confusion, rapport par classe
"""

import json
import sys
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score,
    f1_score,
)

BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR))

from model.predict import PredicteurIntention
from preprocessing.darija_mapper import normaliser

TEST_PATH   = BASE_DIR / "data" / "test_set.json"
OUTPUT_DIR  = BASE_DIR / "analyse" / "rapports"
OUTPUT_DIR.mkdir(exist_ok=True)


def evaluer_modele():
    print("=" * 60)
    print("  Évaluation du modèle DziriBERT")
    print("=" * 60)

    # Chargement du jeu de test
    with open(TEST_PATH, encoding="utf-8") as f:
        test_data = json.load(f)

    predictor = PredicteurIntention()
    y_true, y_pred, confiances, textes = [], [], [], []

    for item in test_data:
        texte = item["texte"]
        vrai_label = item["intention"]
        result = predictor.predire(texte)
        y_true.append(vrai_label)
        y_pred.append(result["intention"])
        confiances.append(result["confiance"])
        textes.append(texte)

    # ─── Rapport texte ───────────────────────────────────────
    print("\n=== Rapport de classification ===")
    rapport = classification_report(y_true, y_pred, zero_division=0)
    print(rapport)

    acc = accuracy_score(y_true, y_pred)
    f1_w = f1_score(y_true, y_pred, average="weighted", zero_division=0)
    f1_m = f1_score(y_true, y_pred, average="macro", zero_division=0)
    conf_moy = np.mean(confiances)

    print(f"  Accuracy        : {acc:.4f}")
    print(f"  F1 weighted     : {f1_w:.4f}")
    print(f"  F1 macro        : {f1_m:.4f}")
    print(f"  Confiance moy.  : {conf_moy:.4f}")

    # ─── Sauvegarde rapport JSON ──────────────────────────────
    rapport_dict = classification_report(y_true, y_pred, zero_division=0, output_dict=True)
    rapport_dict["accuracy_global"]     = acc
    rapport_dict["f1_weighted"]         = f1_w
    rapport_dict["f1_macro"]            = f1_m
    rapport_dict["confiance_moyenne"]   = conf_moy

    with open(OUTPUT_DIR / "rapport.json", "w", encoding="utf-8") as f:
        json.dump(rapport_dict, f, ensure_ascii=False, indent=2)
    print(f"\n  Rapport JSON sauvegardé : {OUTPUT_DIR / 'rapport.json'}")

    # ─── Matrice de confusion ─────────────────────────────────
    labels = sorted(set(y_true + y_pred))
    cm = confusion_matrix(y_true, y_pred, labels=labels)

    plt.figure(figsize=(10, 8))
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        xticklabels=labels,
        yticklabels=labels,
    )
    plt.title("Matrice de confusion — DziriBERT", fontsize=14)
    plt.ylabel("Vrai label")
    plt.xlabel("Label prédit")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "confusion_matrix.png", dpi=150)
    plt.close()
    print(f"  Matrice sauvegardée : {OUTPUT_DIR / 'confusion_matrix.png'}")

    # ─── Graphique F1 par intention ───────────────────────────
    f1_par_classe = {
        k: v["f1-score"]
        for k, v in rapport_dict.items()
        if isinstance(v, dict) and "f1-score" in v
    }
    intentions = list(f1_par_classe.keys())
    f1_vals    = list(f1_par_classe.values())

    colors = ["#22c55e" if v >= 0.8 else "#f59e0b" if v >= 0.6 else "#ef4444"
              for v in f1_vals]

    plt.figure(figsize=(10, 5))
    bars = plt.barh(intentions, f1_vals, color=colors)
    plt.axvline(x=0.8, color="gray", linestyle="--", alpha=0.5, label="Seuil 80%")
    plt.xlabel("F1-score")
    plt.title("F1-score par intention — DziriBERT", fontsize=14)
    for bar, val in zip(bars, f1_vals):
        plt.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2,
                 f"{val:.2f}", va="center", fontsize=10)
    plt.xlim(0, 1.15)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "f1_par_intention.png", dpi=150)
    plt.close()
    print(f"  F1 par intention : {OUTPUT_DIR / 'f1_par_intention.png'}")

    # ─── Erreurs détaillées ───────────────────────────────────
    erreurs = [
        {"texte": t, "vrai": v, "predit": p, "confiance": c}
        for t, v, p, c in zip(textes, y_true, y_pred, confiances)
        if v != p
    ]
    if erreurs:
        print(f"\n  Erreurs ({len(erreurs)}) :")
        for e in erreurs:
            print(f"    '{e['texte']}'")
            print(f"      Vrai: {e['vrai']} | Prédit: {e['predit']} ({e['confiance']:.0%})")

    with open(OUTPUT_DIR / "erreurs.json", "w", encoding="utf-8") as f:
        json.dump(erreurs, f, ensure_ascii=False, indent=2)

    return rapport_dict


if __name__ == "__main__":
    evaluer_modele()
