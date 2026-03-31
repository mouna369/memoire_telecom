#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_gemini_api.py – Teste si la clé API Gemini fonctionne
Utilise la nouvelle librairie google-genai
"""

from google import genai

# ── Mets ta clé API ici ────────────────────────────────────────────────
API_KEY = "AIzaSyDkfYTxvYmr287LeO-AO73r0hlEkoNmmVk"
# ──────────────────────────────────────────────────────────────────────

print("─" * 50)
print("  🔑 TEST DE LA CLÉ API GEMINI")
print("─" * 50)

try:
    client = genai.Client(api_key=API_KEY)

    response = client.models.generate_content(
        model="gemini-2.0-flash-lite",
        contents="Réponds juste 'OK' en un mot."
    )

    print(f"  ✅ API fonctionne !")
    print(f"  📩 Réponse : {response.text.strip()}")
    print(f"\n  🚀 Prêt pour l'annotation automatique !")

except Exception as e:
    print(f"  ❌ Erreur : {e}")