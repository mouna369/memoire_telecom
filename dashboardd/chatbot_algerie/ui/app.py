#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — Interface Streamlit du ChatBot Algérien (DziriBERT)
Lancer : streamlit run ui/app.py
"""

import streamlit as st
import requests
import uuid
from datetime import datetime

API_URL = "http://localhost:8000"

st.set_page_config(
    page_title="ChatBot Algérien — DziriBERT",
    page_icon="🤖",
    layout="centered",
)

# ─── Style ───────────────────────────────────────────────────
st.markdown("""
<style>
.langue-badge {
    font-size: 11px; padding: 2px 8px; border-radius: 10px;
    font-weight: 600; display: inline-block; margin-right: 4px;
}
.lang-fr  { background: #dbeafe; color: #1e40af; }
.lang-ar  { background: #fef3c7; color: #92400e; }
.lang-dz  { background: #d1fae5; color: #065f46; }
</style>
""", unsafe_allow_html=True)

# ─── Titre ───────────────────────────────────────────────────
st.title("🤖 ChatBot Algérien")
st.caption("Powered by **DziriBERT** — Parlez en français, darija ou arabe")

# ─── État de session ─────────────────────────────────────────
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())[:8]
if "historique" not in st.session_state:
    st.session_state.historique = []
if "stats" not in st.session_state:
    st.session_state.stats = {"total": 0, "conf_moy": 0.0}

# ─── Sidebar ─────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Paramètres")
    user_name = st.text_input("Votre nom", value="Visiteur")
    st.info(f"Session : `{st.session_state.session_id}`")

    st.divider()
    st.subheader("📊 Stats session")
    total = len(st.session_state.historique) // 2
    st.metric("Messages envoyés", total)
    if total > 0:
        confs = [m.get("confiance", 0) for m in st.session_state.historique
                 if m["role"] == "assistant"]
        moy = sum(confs) / len(confs) if confs else 0
        st.metric("Confiance moyenne", f"{moy:.0%}")

    st.divider()
    if st.button("🗑️ Nouvelle conversation"):
        st.session_state.historique = []
        st.session_state.session_id = str(uuid.uuid4())[:8]
        st.rerun()

    st.divider()
    st.subheader("💡 Exemples")
    exemples = [
        "Salam, wach rak ?",
        "Ma kaynach connexion 4G",
        "Kif ncharg mon forfait ?",
        "3ndi chikaya !",
        "واش راك صحبي",
    ]
    for ex in exemples:
        if st.button(ex, use_container_width=True):
            st.session_state["prefill"] = ex
            st.rerun()

# ─── Historique ──────────────────────────────────────────────
LANG_LABELS = {
    "fr": ("fr", "lang-fr"),
    "ar": ("ar", "lang-ar"),
    "darija_latin": ("dz", "lang-dz"),
    "inconnu": ("?", "lang-dz"),
}

for msg in st.session_state.historique:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])
        if msg["role"] == "assistant" and "meta" in msg:
            m = msg["meta"]
            lang_key = m.get("langue_detectee", "fr")
            label, css = LANG_LABELS.get(lang_key, ("?", "lang-dz"))
            badge = f'<span class="langue-badge {css}">{label}</span>'
            top3_str = " | ".join(
                f"{t['intention']} {t['score']:.0%}" for t in m.get("top3", [])[:3]
            )
            st.markdown(
                f"{badge} **{m['intention']}** — {m['confiance']:.0%} confiance",
                unsafe_allow_html=True,
            )
            if top3_str:
                st.caption(f"Top 3 : {top3_str}")

# ─── Input ───────────────────────────────────────────────────
prefill = st.session_state.pop("prefill", "")
prompt = st.chat_input("Écrivez en français, darija ou arabe...") or prefill

if prompt:
    # Message utilisateur
    st.session_state.historique.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    # Appel API
    with st.spinner("..."):
        try:
            r = requests.post(f"{API_URL}/chat", json={
                "message": prompt,
                "session_id": st.session_state.session_id,
                "user_name": user_name,
            }, timeout=10)
            data = r.json()
            reponse = data["reponse"]
            meta = {
                "intention":       data["intention"],
                "confiance":       data["confiance"],
                "langue_detectee": data["langue_detectee"],
                "top3":            data.get("top3", []),
            }
        except Exception as e:
            reponse = "⚠️ Erreur de connexion à l'API. Vérifiez que le serveur tourne."
            meta = {}

    st.session_state.historique.append({
        "role": "assistant", "content": reponse, "meta": meta
    })

    with st.chat_message("assistant"):
        st.write(reponse)
        if meta:
            lang_key = meta.get("langue_detectee", "fr")
            label, css = LANG_LABELS.get(lang_key, ("?", "lang-dz"))
            badge = f'<span class="langue-badge {css}">{label}</span>'
            st.markdown(
                f"{badge} **{meta['intention']}** — {meta['confiance']:.0%} confiance",
                unsafe_allow_html=True,
            )

    st.rerun()
