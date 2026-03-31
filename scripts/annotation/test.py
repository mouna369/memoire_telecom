import urllib.request
import json

# Remplace par ta clé
API_KEY ="AIzaSyAqwBb6cJVtzpVedVSDNG97a8N0kWmsa5g"

url = f"https://generativelanguage.googleapis.com/v1beta/models?key={API_KEY}"

try:
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read())
    print("✅ Modèles disponibles :")
    for m in data.get("models", []):
        name = m.get("name", "")
        if "generateContent" in m.get("supportedGenerationMethods", []):
            print(f"   • {name}")
except Exception as e:
    print(f"❌ Erreur : {e}")