#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
train.py
Fine-tuning de DziriBERT / DarijaBERT pour la classification d'intentions.

Modèles supportés :
  - alger-ia/dziribert       (arabe algérien + français)
  - alger-ia/darijabert      (darija marocaine/algérienne)
  - moussaKam/DarijaBERT     (variante darija)

Le modèle retenu est DziriBERT car il est spécifiquement pré-entraîné
sur des textes algériens bilingues arabe/français.
"""

import json
import torch
import numpy as np
from pathlib import Path
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
    EarlyStoppingCallback,
)
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score
import sys
sys.path.append(str(Path(__file__).parent.parent))
from preprocessing.darija_mapper import normaliser

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent.parent
DATA_PATH       = BASE_DIR / "data" / "intentions.json"
OUTPUT_DIR      = BASE_DIR / "model" / "dziribert_finetuned"
LABEL_MAP_PATH  = BASE_DIR / "model" / "label_map.json"

# DziriBERT : modèle officiel pré-entraîné sur corpus algérien
MODEL_NAME = "alger-ia/dziribert"
# Alternatives :
# MODEL_NAME = "alger-ia/darijabert"
# MODEL_NAME = "moussaKam/DarijaBERT"

MAX_LEN         = 128
BATCH_SIZE      = 16
EPOCHS          = 15
LEARNING_RATE   = 3e-5
WEIGHT_DECAY    = 0.01
WARMUP_RATIO    = 0.1
TEST_SIZE       = 0.2


# ─────────────────────────────────────────────────────────────
# DATASET
# ─────────────────────────────────────────────────────────────
class IntentDataset(Dataset):
    def __init__(self, encodings: dict, labels: list):
        self.encodings = encodings
        self.labels    = labels

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        item = {k: torch.tensor(v[idx]) for k, v in self.encodings.items()}
        item["labels"] = torch.tensor(self.labels[idx], dtype=torch.long)
        return item


# ─────────────────────────────────────────────────────────────
# MÉTRIQUES
# ─────────────────────────────────────────────────────────────
def compute_metrics(eval_pred):
    logits, labels = eval_pred
    preds = np.argmax(logits, axis=1)
    return {
        "accuracy": accuracy_score(labels, preds),
        "f1_macro": f1_score(labels, preds, average="macro"),
        "f1_weighted": f1_score(labels, preds, average="weighted"),
    }


# ─────────────────────────────────────────────────────────────
# CHARGEMENT DES DONNÉES
# ─────────────────────────────────────────────────────────────
def charger_donnees():
    with open(DATA_PATH, encoding="utf-8") as f:
        data = json.load(f)

    textes, labels, label_map = [], [], {}
    idx = 0

    for intent in data["intentions"]:
        tag = intent["tag"]
        if tag not in label_map:
            label_map[tag] = idx
            idx += 1
        for pattern in intent["patterns"]:
            texte_norm = normaliser(pattern)
            textes.append(texte_norm)
            labels.append(label_map[tag])

    print(f"  Corpus chargé : {len(textes)} exemples, {len(label_map)} intentions")
    return textes, labels, label_map


# ─────────────────────────────────────────────────────────────
# ENTRAÎNEMENT
# ─────────────────────────────────────────────────────────────
def entrainer_modele():
    print("=" * 60)
    print(f"  Fine-tuning DziriBERT : {MODEL_NAME}")
    print("=" * 60)

    textes, labels, label_map = charger_donnees()

    # Split train / eval
    X_train, X_eval, y_train, y_eval = train_test_split(
        textes, labels, test_size=TEST_SIZE, random_state=42, stratify=labels
    )
    print(f"  Train: {len(X_train)} | Eval: {len(X_eval)}")

    # Tokenizer
    print(f"  Chargement tokenizer : {MODEL_NAME}")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    enc_train = tokenizer(X_train, truncation=True, padding=True, max_length=MAX_LEN)
    enc_eval  = tokenizer(X_eval,  truncation=True, padding=True, max_length=MAX_LEN)

    dataset_train = IntentDataset(enc_train, y_train)
    dataset_eval  = IntentDataset(enc_eval,  y_eval)

    # Modèle
    print(f"  Chargement modèle : {MODEL_NAME}")
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME,
        num_labels=len(label_map),
        ignore_mismatched_sizes=True,
    )

    # Arguments d'entraînement
    args = TrainingArguments(
        output_dir=str(OUTPUT_DIR),
        num_train_epochs=EPOCHS,
        per_device_train_batch_size=BATCH_SIZE,
        per_device_eval_batch_size=BATCH_SIZE,
        learning_rate=LEARNING_RATE,
        weight_decay=WEIGHT_DECAY,
        warmup_ratio=WARMUP_RATIO,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1_weighted",
        greater_is_better=True,
        logging_dir=str(BASE_DIR / "logs"),
        logging_steps=10,
        fp16=torch.cuda.is_available(),
        report_to="none",
    )

    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=dataset_train,
        eval_dataset=dataset_eval,
        compute_metrics=compute_metrics,
        callbacks=[EarlyStoppingCallback(early_stopping_patience=3)],
    )

    print("  Démarrage entraînement...")
    trainer.train()

    # Sauvegarde
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(str(OUTPUT_DIR))
    tokenizer.save_pretrained(str(OUTPUT_DIR))

    # Sauvegarde label map (index → tag)
    label_map_inv = {v: k for k, v in label_map.items()}
    with open(LABEL_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(label_map_inv, f, ensure_ascii=False, indent=2)

    print(f"\n  Modèle sauvegardé dans : {OUTPUT_DIR}")
    print(f"  Label map : {LABEL_MAP_PATH}")

    # Rapport final
    metrics = trainer.evaluate()
    print("\n=== Métriques finales ===")
    for k, v in metrics.items():
        print(f"  {k}: {v:.4f}" if isinstance(v, float) else f"  {k}: {v}")


if __name__ == "__main__":
    entrainer_modele()
