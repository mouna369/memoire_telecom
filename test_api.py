# # test_modele_local_correct.py

# import torch
# import torch.nn.functional as F
# from transformers import AutoTokenizer, AutoConfig, BertModel
# import os
# import json
# import numpy as np

# # ============================================================
# # 1. CHEMINS
# # ============================================================

# MODEL_PATH = "/home/mouna/projet_telecom/Modele/bert_meanpool_final"
# TOKENIZER_PATH = os.path.join(MODEL_PATH, "tokenizer")
# CHECKPOINT_PATH = os.path.join(MODEL_PATH, "checkpoints", "checkpoint-1740")
# WEIGHTS_PATH = os.path.join(CHECKPOINT_PATH, "model.safetensors")

# print("="*60)
# print("🚀 CHARGEMENT DU MODÈLE (avec la bonne architecture)")
# print("="*60)

# # ============================================================
# # 2. CHARGER LA CONFIGURATION
# # ============================================================

# # Dimensions du modèle (d'après votre code d'entraînement)
# HIDDEN_SIZE = 768
# FLAG_DIM = 10      # nombre de flags
# TFIDF_DIM = 150    # dimension TF-IDF
# TOTAL_FEATURES = HIDDEN_SIZE + FLAG_DIM + TFIDF_DIM  # 928
# NUM_LABELS = 3

# print(f"📊 Dimensions du modèle:")
# print(f"   Hidden size (BERT): {HIDDEN_SIZE}")
# print(f"   Flag dimension: {FLAG_DIM}")
# print(f"   TF-IDF dimension: {TFIDF_DIM}")
# print(f"   Total features: {TOTAL_FEATURES}")
# print(f"   Nombre de classes: {NUM_LABELS}")

# # ============================================================
# # 3. DÉFINIR L'ARCHITECTURE DU MODÈLE (identique à l'entraînement)
# # ============================================================

# class BertMeanPoolClassifier(torch.nn.Module):
#     """
#     Modèle identique à celui utilisé pendant l'entraînement
#     """
#     def __init__(self, model_name, flag_dim, tfidf_dim, num_labels=3, dropout=0.3):
#         super().__init__()
#         self.bert = BertModel.from_pretrained(model_name)
#         self.dropout = torch.nn.Dropout(dropout)
#         self.classifier = torch.nn.Linear(HIDDEN_SIZE + flag_dim + tfidf_dim, num_labels)
        
#     def mean_pool(self, last_hidden_state, attention_mask):
#         mask = attention_mask.unsqueeze(-1).float()
#         summed = (last_hidden_state * mask).sum(dim=1)
#         counts = mask.sum(dim=1).clamp(min=1e-9)
#         return summed / counts
    
#     def forward(self, input_ids, attention_mask, flags=None, tfidf=None):
#         outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
#         pooled = self.mean_pool(outputs.last_hidden_state, attention_mask)
#         pooled = self.dropout(pooled)
        
#         if flags is not None and tfidf is not None:
#             combined = torch.cat([pooled, flags, tfidf], dim=1)
#         else:
#             # Si pas de flags/TFIDF, on utilise des zéros
#             batch_size = pooled.shape[0]
#             device = pooled.device
#             flags_zero = torch.zeros(batch_size, FLAG_DIM).to(device)
#             tfidf_zero = torch.zeros(batch_size, TFIDF_DIM).to(device)
#             combined = torch.cat([pooled, flags_zero, tfidf_zero], dim=1)
        
#         logits = self.classifier(combined)
#         return logits

# # ============================================================
# # 4. CHARGER LE TOKENIZER
# # ============================================================

# print("\n📥 Chargement du tokenizer...")
# tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_PATH)
# print(f"✅ Tokenizer chargé, vocab_size: {tokenizer.vocab_size}")

# # ============================================================
# # 5. CRÉER LE MODÈLE
# # ============================================================

# print("\n📥 Création du modèle...")
# model = BertMeanPoolClassifier(
#     model_name="alger-ia/dziribert",
#     flag_dim=FLAG_DIM,
#     tfidf_dim=TFIDF_DIM,
#     num_labels=NUM_LABELS,
#     dropout=0.3
# )

# # ============================================================
# # 6. CHARGER LES POIDS
# # ============================================================

# print("\n📥 Chargement des poids...")
# from safetensors.torch import load_file

# state_dict = load_file(WEIGHTS_PATH)

# # Nettoyer les clés (enlever le préfixe 'model.' si présent)
# cleaned_state_dict = {}
# for key, value in state_dict.items():
#     if key.startswith('model.'):
#         new_key = key[6:]
#     else:
#         new_key = key
#     cleaned_state_dict[new_key] = value

# # Charger les poids
# missing, unexpected = model.load_state_dict(cleaned_state_dict, strict=False)

# if missing:
#     print(f"⚠️ Clés manquantes: {missing[:5]}...")
# if unexpected:
#     print(f"⚠️ Clés inattendues: {unexpected[:5]}...")

# print("✅ Poids chargés")

# # ============================================================
# # 7. MODE ÉVALUATION
# # ============================================================

# device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# model.to(device)
# model.eval()

# LABELS = ["NEGATIF", "NEUTRE", "POSITIF"]
# print(f"🏷️  Labels: {LABELS}")
# print(f"✅ Modèle sur {device}")

# # ============================================================
# # 8. FONCTION DE PRÉDICTION
# # ============================================================

# def predict(text):
#     inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=128)
#     inputs = {k: v.to(device) for k, v in inputs.items()}
    
#     with torch.no_grad():
#         logits = model(inputs['input_ids'], inputs['attention_mask'])
#         probs = F.softmax(logits, dim=-1)
#         pred = torch.argmax(probs, dim=-1).item()
    
#     return {
#         "label": LABELS[pred],
#         "confidence": float(probs[0][pred]),
#         "probabilities": {
#             "NEGATIF": float(probs[0][0]),
#             "NEUTRE": float(probs[0][1]),
#             "POSITIF": float(probs[0][2])
#         }
#     }

# # ============================================================
# # 9. TEST
# # ============================================================

# print("\n🧪 TESTS DU MODÈLE")
# print("-"*40)

# # Ajoutez ces tests
# test_texts = [
#     # Positifs
#     "Merci beaucoup pour votre aide",
#     "Service excellent, bravo !",
#     "Connexion rapide et stable",
    
#     # Négatifs
#     "Je n'arrive pas à me connecter",
#     "Problème depuis 3 jours",
#     "Service client inexistant",
    
#     # Neutres
#     "Pouvez-vous me contacter ?",
#     "Quels sont vos horaires ?",
#     "Comment changer mon forfait ?",
# ]

# for text in test_texts:
#     print(f"\n📝 Texte: {text}")
#     result = predict(text)
#     print(f"   🎯 Prédiction: {result['label']} ({result['confidence']:.2%})")
#     print(f"   📊 NEG={result['probabilities']['NEGATIF']:.2%}, NEU={result['probabilities']['NEUTRE']:.2%}, POS={result['probabilities']['POSITIF']:.2%}")

# print("\n" + "="*60)
# print("✅ Tests terminés !")

# test_kaggle_api.py

import requests

API_URL = "https://imprint-nerd-wok.ngrok-free.dev/predict"

def tester(texte):
    print(f"\n📝 Texte: {texte}")
    try:
        response = requests.post(
            API_URL,
            json={"commentaire": texte},
            timeout=30
        )
        
        if response.status_code == 200:
            resultat = response.json()
            print(f"   🎯 Prédiction: {resultat['label']}")
            print(f"   📊 Confiance: {resultat['confidence']:.2%}")
            print(f"   📈 Probabilités: NEG={resultat['probabilities']['NEGATIF']:.2%}, NEU={resultat['probabilities']['NEUTRE']:.2%}, POS={resultat['probabilities']['POSITIF']:.2%}")
        else:
            print(f"   ❌ Erreur: {response.status_code}")
            print(f"   Réponse: {response.text}")
            
    except Exception as e:
        print(f"   ❌ Exception: {e}")

if __name__ == "__main__":
    print("="*60)
    print("🧪 TEST DE L'API KAGGLE")
    print(f"   URL: {API_URL}")
    print("="*60)
    
    tester("Très bon service, je recommande !")
    tester("Service nul, connexion coupée")
    tester("rependre moi algerei")