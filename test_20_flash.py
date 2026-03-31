import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")

model = "gemini-2.0-flash"
url = f"https://generativelanguage.googleapis.com/v1/models/{model}:generateContent?key={api_key}"

# ✅ Payload avec camelCase (obligatoire pour l'API REST)
payload = {
    "contents": [{"parts": [{"text": "Réponds uniquement par: OK"}]}],
    "generationConfig": {
        "responseMimeType": "text/plain"  # ✅ camelCase, PAS snake_case
    }
}

print(f"🔄 Test avec {model} (camelCase)...")
try:
    response = requests.post(url, json=payload, timeout=30)
    print(f"📡 Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        print(f"✅ SUCCÈS ! Réponse : {text}")
    else:
        print(f"❌ Réponse erreur : {response.text[:400]}")
except Exception as e:
    print(f"❌ Exception : {type(e).__name__} - {e}")
