#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
predict.py
Prédiction d'intention avec DziriBERT fine-tuné.
Supporte : darija latinisée, arabe dialectal, français.
"""

import json
import torch
import numpy as np
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import sys
sys.path.append(str(Path(__file__).parent.parent))
from preprocessing.darija_mapper import normaliser, detecter_langue

BASE_DIR       = Path(__file__).parent.parent
MODEL_DIR      = BASE_DIR / "model" / "dziribert_finetuned"
LABEL_MAP_PATH = BASE_DIR / "model" / "label_map.json"
INTENTS_PATH   = BASE_DIR / "data" / "intentions.json"

SEUIL_CONFIANCE = 0.40   # En-dessous → "incompris"


class PredicteurIntention:
    """
    Chargement unique du modèle DziriBERT fine-tuné.
    Réutiliser l'instance pour toutes les prédictions (optimisation mémoire).
    """

    def __init__(self):
        print(f"  Chargement DziriBERT depuis : {MODEL_DIR}")
        self.tokenizer = AutoTokenizer.from_pretrained(str(MODEL_DIR))
        self.model     = AutoModelForSequenceClassification.from_pretrained(str(MODEL_DIR))
        self.model.eval()

        # Déplacement sur GPU si disponible
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)

        # Chargement du label map : {index: tag}
        with open(LABEL_MAP_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        # Les clés sont des strings (JSON) → convertir en int
        self.label_map = {int(k): v for k, v in raw.items()}

        # Chargement des réponses
        with open(INTENTS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        self.reponses = {i["tag"]: i["responses"] for i in data["intentions"]}

        print(f"  Modèle chargé ({len(self.label_map)} intentions) — device: {self.device}")

    def predire(self, texte: str) -> dict:
        """
        Retourne un dict avec :
          - intention     : tag prédit
          - confiance     : score softmax [0-1]
          - top3          : liste des 3 meilleures intentions avec scores
          - texte_original   : texte brut
          - texte_normalise  : texte après preprocessing
          - langue_detectee  : 'fr', 'ar', 'darija_latin'
        """
        texte_original = texte
        langue         = detecter_langue(texte)
        texte_norm     = normaliser(texte)

        if not texte_norm:
            return self._incompris(texte_original, langue)

        # Tokenisation
        inputs = self.tokenizer(
            texte_norm,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=128,
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        # Inférence
        with torch.no_grad():
            logits = self.model(**inputs).logits

        probs = torch.softmax(logits, dim=1)[0].cpu().numpy()
        top3_idx = np.argsort(probs)[::-1][:3]

        best_idx    = int(top3_idx[0])
        best_prob   = float(probs[best_idx])
        best_intent = self.label_map.get(best_idx, "incompris")

        if best_prob < SEUIL_CONFIANCE:
            best_intent = "incompris"

        top3 = [
            {"intention": self.label_map.get(int(i), "?"), "score": round(float(probs[i]), 4)}
            for i in top3_idx
        ]

        return {
            "intention":        best_intent,
            "confiance":        round(best_prob, 4),
            "top3":             top3,
            "texte_original":   texte_original,
            "texte_normalise":  texte_norm,
            "langue_detectee":  langue,
        }

    def _incompris(self, texte: str, langue: str) -> dict:
        return {
            "intention":       "incompris",
            "confiance":       0.0,
            "top3":            [],
            "texte_original":  texte,
            "texte_normalise": "",
            "langue_detectee": langue,
        }


if __name__ == "__main__":
    predictor = PredicteurIntention()
    tests = [
        "salam, wach rak ?",
        "ma kaynach connexion 4G",
        "je veux recharger mon forfait",
        "3ndi chikaya",
        "واش راك صحبي",
        "merci beaucoup",
    ]
    print("\n=== Test prédictions ===")
    for t in tests:
        r = predictor.predire(t)
        print(f"  [{r['langue_detectee']:12}] '{t}'")
        print(f"    → {r['intention']} ({r['confiance']:.0%}) | normalisé: '{r['texte_normalise']}'")
        print()
