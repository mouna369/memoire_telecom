# import pandas as pd
# from pymongo import MongoClient
# from google import genai
# from google.genai import types
# import json
# import time
# import os
# from dotenv import load_dotenv  # ← Pour gérer les clés secrètes

# # Charger les variables d'environnement depuis un fichier .env
# load_dotenv()

# # --- CONFIGURATION SÉCURISÉE ---
# GOOGLE_API_KEY = os.getenv("AIzaSyDkfYTxvYmr287LeO-AO73r0hlEkoNmmVk")  # ← Clé dans .env, pas en dur !
# if not GOOGLE_API_KEY:
#     raise ValueError("❌ Clé API manquante. Définissez GEMINI_API_KEY dans votre fichier .env")

# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# COLLECTION_NOM = "commentaires_normalises"
# NB_TESTS = 10 

# # Client Genai avec la nouvelle API
# client_genai = genai.Client(api_key=GOOGLE_API_KEY)

# # Dossier de sortie
# os.makedirs("Rapports", exist_ok=True)

# # ✅ NOM DE MODÈLE CORRECT (sans "models/")
# MODEL_NAME = 'gemini-1.5-flash'

# # Connexion MongoDB
# mongo_client = MongoClient(MONGO_URI)
# db = mongo_client[DB_NAME]
# collection = db[COLLECTION_NOM]
# docs = list(collection.find({}, {"Commentaire_Client": 1}).limit(NB_TESTS))

# def interroger_gemini(texte, id_comm):
#     prompt = f"""
#     Tu es un système expert en analyse de la satisfaction client.
#     Analyse ce commentaire d'Algérie Télécom : "{texte}"
#     Retourne STRICTEMENT un objet JSON avec ces clés :
#       "id": {id_comm},
#       "sentiment": "positive" | "negative" | "neutral",
#       "confidence": 0.x
#     Règles : Sarcasme ou insulte = negative.
#     """
#     try:
#         start_time = time.time()
        
#         # ✅ Appel avec le nouveau SDK
#         response = client_genai.models.generate_content(
#             model=MODEL_NAME,  # ← Pas de préfixe "models/"
#             contents=prompt,
#             config=types.GenerateContentConfig(
#                 response_mime_type="application/json"
#             )
#         )
        
#         duree = time.time() - start_time
        
#         # Nettoyage JSON
#         raw_text = response.text.strip()
#         if raw_text.startswith("```json"):
#             raw_text = raw_text.replace("```json", "").replace("```", "").strip()
            
#         data = json.loads(raw_text)
#         return data.get('sentiment'), data.get('confidence'), duree
        
#     except Exception as e:
#         error_msg = str(e)
#         # Affichage détaillé pour débogage
#         if "API key expired" in error_msg:
#             print(f"🔑 ERREUR CLÉ API : Votre clé a expiré. Renouvelez-la sur aistudio.google.com")
#         elif "not found" in error_msg.lower():
#             print(f"🔍 ERREUR MODÈLE : Vérifiez que le nom du modèle est correct (sans 'models/')")
#         print(f"❌ Erreur sur le doc {id_comm}: {error_msg[:150]}")
#         return "error", 0, 0

# resultats = []
# print(f"🚀 Démarrage de l'analyse Gemini ({MODEL_NAME})...")
# print(f"🔑 Clé API configurée : {'✅' if GOOGLE_API_KEY.startswith('AIza') else '❌'}")

# for i, doc in enumerate(docs):
#     texte = doc.get("Commentaire_Client", "")
#     print(f"🔄 Analyse {i+1}/{NB_TESTS}...")
    
#     sent, conf, duree = interroger_gemini(texte, i+1)
    
#     resultats.append({
#         "ID": i+1,
#         "Texte": texte[:100] + "..." if len(texte) > 100 else texte,
#         "Gemini_Sent": sent,
#         "Gemini_Conf": conf,
#         "Gemini_Time": round(duree, 2)
#     })
    
#     time.sleep(2)  # Pause pour respecter les quotas gratuits

# # Sauvegarde
# df = pd.DataFrame(resultats)
# df.to_csv("Rapports/resultats_gemini_test.csv", index=False, encoding='utf-8-sig')

# print("\n" + "="*40)
# errors = df[df['Gemini_Sent'] == 'error']
# if not errors.empty:
#     print(f"⚠️ {len(errors)}/{NB_TESTS} analyses ont échoué.")
#     print("💡 Vérifiez : clé API, nom du modèle, quotas")
# else:
#     print("✅ SUCCÈS ! Toutes les analyses sont terminées.")
# print(f"📂 Résultat : Rapports/resultats_gemini_test.csv")
# print("="*40)



# mongo_client.close()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# scripts/nlp/evaluer_llm_judge.py
# ✅ LLM-as-a-Judge : Évaluation automatique de DziriBERT

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
torch.set_num_threads(2)
from pymongo import MongoClient
from ollama import chat
import json
import time
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COL_SOURCE = "commentaires_normalises"
COL_DEST = "evaluation_llm_judge"
MODEL_DZIRI = "alger-ia/dziribert_sentiment"
MODEL_JUDGE = "llama3"  # ou "deepseek-r1", "aya"
NB_ECHANTILLON = 20 # Nombre de commentaires à évaluer

LABELS = {0: "Négatif", 1: "Neutre", 2: "Positif"}

print("=" * 80)
print("🤖 LLM-as-a-Judge : Évaluation Automatique de DziriBERT")
print("=" * 80)

# ============================================================
# CHARGEMENT DE DZIRIBERT
# ============================================================
print("\n📊 Étape 1 : Chargement de DziriBERT...")
try:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_DZIRI)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_DZIRI)
    model.eval()
    print("   ✅ DziriBERT prêt")
except Exception as e:
    print(f"   ❌ Erreur : {e}")
    exit(1)

# ============================================================
# TEST CONNEXION OLLAMA (Le Juge)
# ============================================================
print("\n🧠 Étape 2 : Test du LLM Juge (Ollama)...")
try:
    chat(model=MODEL_JUDGE, messages=[{'role': 'user', 'content': 'test'}])
    print(f"   ✅ {MODEL_JUDGE} prêt (via Ollama)")
except Exception as e:
    print(f"   ❌ Ollama non disponible : {e}")
    print(f"   → Lance : ollama pull {MODEL_JUDGE}")
    exit(1)

# ============================================================
# FONCTION : Prédiction DziriBERT
# ============================================================
def predire_dziribert(texte):
    """Retourne sentiment + confiance"""
    inputs = tokenizer(texte, return_tensors="pt", truncation=True, max_length=128)
    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.softmax(outputs.logits, dim=-1)[0]
        idx = torch.argmax(probs).item()
    return LABELS[idx], round(probs[idx].item(), 4)

# ============================================================
# FONCTION : LLM Juge
# ============================================================
def llm_juge(commentaire, prediction_dziribert, confiance):
    """
    Le LLM évalue si la prédiction de DziriBERT est correcte.
    Retourne : True (correct) ou False (incorrect)
    """
    prompt = f"""
Tu es un expert en analyse de sentiment pour le dialecte algérien (Darija/Arabe/Français).

Voici un commentaire client et la prédiction d'un modèle d'IA :

COMMENTAIRE : "{commentaire[:300]}"

PRÉDICTION DU MODÈLE : {prediction_dziribert} (confiance : {confiance:.2%})

TA TÂCHE :
Évalue si la prédiction du modèle est CORRECTE ou NON.

RÉPONDS UNIQUEMENT au format JSON suivant (rien d'autre) :
{{
    "correct": true ou false,
    "raison": "une phrase expliquant ton jugement"
}}

Exemple de réponse valide :
{{"correct": true, "raison": "Le commentaire exprime clairement du mécontentement"}}
{{"correct": false, "raison": "Le commentaire est en fait neutre, pas négatif"}}
"""
    
    try:
        response = chat(model=MODEL_JUDGE, messages=[{'role': 'user', 'content': prompt}])
        txt = response['message']['content'].strip()
        
        # Nettoyer le JSON (parfois Ollama ajoute des ```)
        if '```' in txt:
            txt = txt.split('```')[1]
            if txt.startswith('json'):
                txt = txt[4:]
        txt = txt.strip()
        
        # Parser le JSON
        result = json.loads(txt)
        
        return {
            "correct": result.get("correct", False),
            "raison": result.get("raison", ""),
            "erreur": None
        }
    except Exception as e:
        return {
            "correct": False,
            "raison": "",
            "erreur": str(e)[:100]
        }

# ============================================================
# ÉTAPE 3 : PRÉDICTION + ÉVALUATION
# ============================================================
print("\n📈 Étape 3 : Prédiction + Évaluation LLM...")
print(f"   → {NB_ECHANTILLON} commentaires à traiter\n")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
coll_source = db[COL_SOURCE]
coll_dest = db[COL_DEST]
coll_dest.delete_many({})  # Vider la collection de résultats

# Récupérer un échantillon aléatoire
docs = list(coll_source.aggregate([{"$sample": {"size": NB_ECHANTILLON}}]))
print(f"   ✅ {len(docs)} commentaires chargés\n")

resultats = []
correct_count = 0
start_time = time.time()

for i, doc in enumerate(docs):
    texte = doc.get("Commentaire_Client", "")
    if not texte or len(texte.strip()) < 3:
        continue
    
    # 1. DziriBERT prédit
    sentiment_pred, confiance = predire_dziribert(texte)
    
    # 2. LLM Juge évalue
    jugement = llm_juge(texte, sentiment_pred, confiance)
    
    # 3. Compter
    if jugement["correct"]:
        correct_count += 1
        statut = "✅"
    else:
        statut = "❌"
    
    # 4. Afficher progression
    print(f"[{len(resultats)+1}/{NB_ECHANTILLON}] {statut} {texte[:50]}...")
    print(f"        DziriBERT : {sentiment_pred} ({confiance:.2%})")
    print(f"        Juge : {'CORRECT' if jugement['correct'] else 'INCORRECT'} - {jugement['raison'][:60]}")
    print()
    
    # 5. Sauvegarder dans MongoDB
    doc_eval = {
        "commentaire": texte[:500],
        "sentiment_pred": sentiment_pred,
        "confiance": confiance,
        "jugement_correct": jugement["correct"],
        "raison_juge": jugement["raison"],
        "erreur_juge": jugement["erreur"],
        "modele_juge": MODEL_JUDGE,
        "date_evaluation": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    coll_dest.insert_one(doc_eval)
    
    resultats.append(doc_eval)

temps_total = time.time() - start_time
accuracy = correct_count / len(resultats) if resultats else 0

# ============================================================
# ÉTAPE 4 : RÉSULTATS FINAUX
# ============================================================
print("=" * 80)
print("📊 RÉSULTATS DE L'ÉVALUATION (LLM-as-a-Judge)")
print("=" * 80)

print(f"""
📈 MÉTRIQUES PRINCIPALES :
   • Nombre de commentaires évalués : {len(resultats)}
   • Prédictions CORRECTES : {correct_count}
   • Prédictions INCORRECTES : {len(resultats) - correct_count}
   • ACCURACY : {accuracy:.2%} {'✅' if accuracy > 0.85 else '⚠️'}
   • Temps total : {temps_total/60:.1f} minutes
   • Vitesse : {len(resultats)/temps_total:.2f} docs/sec

🎯 INTERPRÉTATION :
   • Accuracy > 85% → Modèle VALIDÉ ✅
   • Accuracy 70-85% → Modèle ACCEPTABLE ⚠️
   • Accuracy < 70% → Modèle À AMÉLIORER ❌
""")

# Stats par sentiment
print("📋 PERFORMANCE PAR SENTIMENT :")
par_sentiment = {}
for r in resultats:
    sent = r["sentiment_pred"]
    if sent not in par_sentiment:
        par_sentiment[sent] = {"total": 0, "correct": 0}
    par_sentiment[sent]["total"] += 1
    if r["jugement_correct"]:
        par_sentiment[sent]["correct"] += 1

for sent, stats in par_sentiment.items():
    acc = stats["correct"] / stats["total"] if stats["total"] > 0 else 0
    print(f"   • {sent:10} : {stats['correct']}/{stats['total']} ({acc:.2%})")

# Exemples d'erreurs
erreurs = [r for r in resultats if not r["jugement_correct"]]
if erreurs:
    print(f"\n⚠️  EXEMPLES D'ERREURS ({len(erreurs)} cas) :")
    for i, err in enumerate(erreurs[:3]):
        print(f"   [{i+1}] Commentaire : {err['commentaire'][:80]}...")
        print(f"       Prédiction : {err['sentiment_pred']}")
        print(f"       Raison juge : {err['raison_juge'][:80]}")
        print()

# ============================================================
# SAUVEGARDE RAPPORT
# ============================================================
rapport = {
    "methode": "LLM-as-a-Judge",
    "modele_evalue": MODEL_DZIRI,
    "modele_juge": MODEL_JUDGE,
    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "n_commentaires": len(resultats),
    "correct": correct_count,
    "incorrect": len(resultats) - correct_count,
    "accuracy": round(accuracy, 4),
    "temps_minutes": round(temps_total/60, 2),
    "validation": "✅ MODÈLE VALIDÉ" if accuracy > 0.85 else "⚠️ À AMÉLIORER",
    "performance_par_sentiment": {
        sent: {
            "total": stats["total"],
            "correct": stats["correct"],
            "accuracy": round(stats["correct"]/stats["total"], 4) if stats["total"] > 0 else 0
        }
        for sent, stats in par_sentiment.items()
    }
}

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
fichier_rapport = f"Rapports/llm_judge_evaluation_{timestamp}.json"
with open(fichier_rapport, "w", encoding="utf-8") as f:
    json.dump(rapport, f, ensure_ascii=False, indent=2)

print("=" * 80)
print(f"✅ RAPPORT SAUVEGARDÉ : {fichier_rapport}")
print(f"✅ RÉSULTATS DANS MONGODB : {DB_NAME}.{COL_DEST}")
print("=" * 80)

if accuracy > 0.85:
    print("\n🏆 CONCLUSION : DziriBERT est VALIDÉ par LLM-as-a-Judge !")
    print("✅ Tu peux l'utiliser en confiance sur les 26 000 commentaires !")
else:
    print("\n⚠️ CONCLUSION : Accuracy < 85%")
    print("🔧 Piste : Vérifier le preprocessing ou analyser les erreurs ci-dessus")

client.close()