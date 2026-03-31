# import pandas as pd
# from pymongo import MongoClient
# import ollama
# import json
# import time
# import os

# # --- CONFIGURATION ---
# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# COLLECTION_NOM = "commentaires_normalises"
# NB_TESTS = 10

# # Création du dossier de rapport s'il n'existe pas
# os.makedirs("Rapports", exist_ok=True)

# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]
# collection = db[COLLECTION_NOM]

# docs = list(collection.find({}, {"Commentaire_Client": 1}).limit(NB_TESTS))

# def interroger_ollama_expert(modele, texte, id_comm):
#     prompt = f"""
#     Tu es un système expert en analyse de la satisfaction client (positive, negative, neutral).
#     Tu vas analyser exactement le commentaire ci-dessous.
#     Retourne STRICTEMENT un objet JSON contenant les informations suivantes :
#       "id": {id_comm},
#       "comment": "{texte}",
#       "sentiment": "positive" | "negative" | "neutral",
#       "confidence": 0.x
#     Règles essentielles :
#     - Si le commentaire exprime de la satisfaction ou gratitude → positive
#     - Si le commentaire exprime une plainte, colère, frustration, ironie ou sarcasme → negative
#     - Si le commentaire est descriptif, une question, information ou sans émotion → neutral
#     - Le sarcasme ou ironie = toujours negative
#     - confidence est un score entre 0 et 1
#     """
#     try:
#         start_time = time.time()
#         # On utilise le format JSON
#         response = ollama.chat(model=modele, format='json', messages=[{'role': 'user', 'content': prompt}])
#         duree = time.time() - start_time
        
#         data = json.loads(response['message']['content'])
#         return data['sentiment'], data['confidence'], duree
#     except Exception as e:
#         print(f"Erreur avec {modele} sur le doc {id_comm}: {e}")
#         return "error", 0, 0

# resultats = []

# for i, doc in enumerate(docs):
#     texte = doc.get("Commentaire_Client", "").replace('"', "'")
#     print(f"🔄 Analyse du commentaire {i+1}/{NB_TESTS}...")
    
#     # TEST 1 : Llama 3
#     s_llama, c_llama, t_llama = interroger_ollama_expert('llama3', texte, i+1)
    
#     # TEST 2 : Aya (le remplaçant de command-r)
#     s_aya, c_aya, t_aya = interroger_ollama_expert('aya', texte, i+1)
    
#     resultats.append({
#         "ID": i+1,
#         "Texte": texte,
#         "Llama3_Sent": s_llama, "Llama3_Conf": c_llama, "Llama3_Time": round(t_llama, 2),
#         "Aya_Sent": s_aya, "Aya_Conf": c_aya, "Aya_Time": round(t_aya, 2)
#     })

# # Sauvegarde locale
# df = pd.DataFrame(resultats)
# df.to_csv("Rapports/duel_expert_resultats.csv", index=False)
# print("\n✅ Analyse terminée ! Le fichier est dans : scripts/annotation/Rapports/duel_expert_resultats.csv")

# client.close()

import pandas as pd
from pymongo import MongoClient
import ollama
import time
import os

# --- CONFIGURATION ---
MODELS = ['llama3', 'mistral', 'aya', 'deepseek-r1:7b']
NB_TESTS = 10

client = MongoClient("mongodb://localhost:27018/")
db = client["telecom_algerie"]
collection = db["commentaires_normalises"]
docs = list(collection.find({}, {"Commentaire_Client": 1}).limit(NB_TESTS))

all_results = []

for i, doc in enumerate(docs):
    texte = doc.get("Commentaire_Client", "")
    print(f"\n📝 Commentaire {i+1}/{NB_TESTS} : {texte[:50]}...")
    
    res_com = {"ID": i+1, "Texte": texte}
    
    for m in MODELS:
        print(f"  🤖 Interrogation de {m}...")
        try:
            start = time.time()
            response = ollama.chat(model=m, messages=[
                {'role': 'user', 'content': f"Analyse ce sentiment (positive, negative, neutral) en UN SEUL MOT : {texte}"}
            ])
            duree = time.time() - start
            res_com[f"{m}_sent"] = response['message']['content'].strip().lower()
            res_com[f"{m}_time"] = round(duree, 2)
        except Exception as e:
            res_com[f"{m}_sent"] = "erreur"
    
    all_results.append(res_com)

# Sauvegarde finale
df = pd.DataFrame(all_results)
os.makedirs("Rapports", exist_ok=True)
df.to_csv("Rapports/comparaison_4_modeles.csv", index=False)

print("\n✅ Félicitations Mouna ! Ton tableau comparatif est prêt dans Rapports/comparaison_4_modeles.csv")