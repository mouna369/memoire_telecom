import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")

model = "gemini-2.0-flash"
url = f"https://generativelanguage.googleapis.com/v1/models/{model}:generateContent?key={api_key}"

# ✅ Payload MINIMAL sans generationConfig
payload = {
    "contents": [{
        "parts": [{
            "text": "Réponds uniquement par le mot: SUCCESS"
        }]
    }]
}

print(f"🔄 Test minimal avec {model}...")
try:
    response = requests.post(url, json=payload, timeout=30)
    print(f"📡 HTTP Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        print(f"✅ SUCCÈS ! Réponse : {text}")
    else:
        print(f"❌ Erreur : {response.text[:500]}")
except Exception as e:
    print(f"❌ Exception : {type(e).__name__} - {e}")
