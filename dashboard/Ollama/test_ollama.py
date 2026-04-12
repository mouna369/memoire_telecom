import requests
import json

url = "http://localhost:11434/api/generate"

payload = {
    "model": "qwen3:4b",
    "prompt": "Bonjour, que peux-tu faire ? Réponds brièvement en français.",
    "stream": False
}

print("⏳ Appel à Ollama en cours...")
response = requests.post(url, json=payload, timeout=60)

if response.status_code == 200:
    result = response.json()
    print("\n✅ Réponse reçue :")
    print(result['response'])
else:
    print(f"❌ Erreur HTTP {response.status_code}")
    print(response.text)