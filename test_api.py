from google import genai
import json
import time

# 1. Utilisation de la version stable 'v1'
client = genai.Client(
    api_key="VOTREAIzaSyDkfYTxvYmr287LeO-AO73r0hlEkoNmmVk_CLE_API_ICI",
    http_options={'api_version': 'v1'} 
)

# On définit le modèle sans le préfixe 'models/' car la bibliothèque le gère
MODEL_ID = "gemini-1.5-flash"

def analyze_batch(comments_list):
    prompt = f"""
    En tant qu'expert en analyse de sentiment, analyse ces commentaires (mélange de français et d'arabe algérien).
    Pour chaque commentaire, retourne STRICTEMENT une liste d'objets JSON avec : id, sentiment (positive, negative, neutral), confidence (0-1).
    Règles : Sarcasme = negative. Questions/Infos = neutral.
    
    Commentaires :
    {comments_list}
    """
    try:
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            # On s'assure que la configuration est bien passée
            config={
                'response_mime_type': 'application/json',
                'temperature': 0.1 # Plus bas pour plus de précision technique
            }
        )
        return response.text
    except Exception as e:
        print(f"Erreur lors de l'appel API : {e}")
        return None

# Le reste de votre boucle (Chargement, Batching, Sauvegarde) reste identique.
# 1. Charger vos données
with open('telecom_algerie.commentaires_normalises1.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 2. Traitement par lots
batch_size = 30
output_file = 'resultats_analyse.jsonl'

print(f"Début du traitement de {len(data)} commentaires...")

for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    input_batch = [{"id": item["_id"], "text": item["Commentaire_Client"]} for item in batch]
    
    print(f"Traitement du lot {i} à {i+batch_size}...")
    result_raw = analyze_batch(input_batch)
    
    if result_raw:
        # Sauvegarde immédiate dans le fichier pour sécuriser les données
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(result_raw + "\n")
    
    # Petite pause pour éviter de saturer l'API
    time.sleep(1)

print(f"Traitement terminé. Résultats sauvegardés dans {output_file}")