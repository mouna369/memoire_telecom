import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")

# Tester les deux endpoints
endpoints = [
    "https://generativelanguage.googleapis.com/v1/models",
    "https://generativelanguage.googleapis.com/v1beta/models"
]

for endpoint in endpoints:
    url = f"{endpoint}?key={api_key}"
    print(f"\n🔍 Vérification sur {endpoint.split('/')[-2]}...")
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            models = [m.get("name", "") for m in data.get("models", []) if "flash" in m.get("name", "").lower()]
            if models:
                print(f"✅ Modèles 'flash' trouvés :")
                for m in models:
                    print(f"   • {m}")
            else:
                print(f"⚠️  Aucun modèle 'flash' trouvé sur cet endpoint")
        else:
            print(f"❌ HTTP {response.status_code}: {response.text[:150]}")
    except Exception as e:
        print(f"❌ Erreur: {e}")
