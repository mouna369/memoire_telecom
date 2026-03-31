import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")

# Modèles et endpoints à tester
tests = [
    ("v1", "gemini-1.5-flash"),
    ("v1beta", "gemini-1.5-flash"),
    ("v1", "gemini-2.0-flash-exp"),
    ("v1beta", "gemini-2.0-flash-exp"),
]

for version, model in tests:
    url = f"https://generativelanguage.googleapis.com/{version}/models/{model}:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": "OK"}]}]
    }
    
    print(f"\n🔄 Test: {version}/{model}")
    try:
        response = requests.post(url, json=payload, timeout=30)
        if response.status_code == 200:
            data = response.json()
            text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "N/A")
            print(f"✅ SUCCÈS ! Réponse: {text.strip()[:50]}")
            break  # Stop au premier qui marche
        else:
            print(f"❌ HTTP {response.status_code}: {response.text[:100]}")
    except Exception as e:
        print(f"❌ Exception: {str(e)[:100]}")
