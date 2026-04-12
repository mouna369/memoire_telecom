#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
chatbot.py — ChatBot Algérien avec Ollama (Mistral 7B) + MongoDB
Conversation naturelle en français, darija, arabe
Analyse intelligente des commentaires télécom

Lancer :
  1. ollama pull mistral
  2. ollama serve
  3. docker-compose up -d
"""

import streamlit as st
from ollama import Client
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import re, json, time, os
from langdetect import detect, LangDetectException

# ──────────────────────────────────────────────────────────────────
# CONFIG (adaptée pour Docker)
# ──────────────────────────────────────────────────────────────────

# MongoDB : utilise le nom du service Docker (pas localhost)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb_pfe:27017/")
DB_NAME = "telecom_algerie"

# Ollama : utilise host.docker.internal (depuis le conteneur)
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL = "qwen2.5:7b" # ou "tinyllama", "llama3", etc.

# Création du client Ollama avec le bon hôte
ollama_client = Client(host=OLLAMA_HOST)

st.set_page_config(
    page_title="ChatBot Algérien — Ollama",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────
# STYLE DARK — ChatGPT-inspired (inchangé, trop long mais conservé)
# ──────────────────────────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap" rel="stylesheet">
<style>
html, body, [class*="css"] {
    font-family: 'Syne', sans-serif !important;
    background: #0a0b0f !important;
    color: #e8e6e0 !important;
}
[data-testid="stAppViewContainer"] { background: #0a0b0f !important; }
[data-testid="stSidebar"] {
    background: #0d0e14 !important;
    border-right: 1px solid #1a1d2e !important;
}
.msg-user { display: flex; justify-content: flex-end; margin: 12px 0; }
.msg-user .bubble {
    background: #1a2540; border: 1px solid #2a3560;
    border-radius: 18px 18px 4px 18px;
    padding: 12px 18px; max-width: 70%; font-size: 15px;
    line-height: 1.65; color: #c8d3f5;
}
.msg-bot { display: flex; gap: 12px; margin: 12px 0; align-items: flex-start; }
.bot-icon {
    width: 36px; height: 36px; min-width: 36px;
    background: linear-gradient(135deg, #00e5a0, #0ea5e9);
    border-radius: 10px; display: flex; align-items: center;
    justify-content: center; font-size: 18px; margin-top: 2px;
}
.msg-bot .bubble {
    background: #0f1117; border: 1px solid #1e2130;
    border-radius: 4px 18px 18px 18px;
    padding: 14px 18px; max-width: 75%; font-size: 15px;
    line-height: 1.75; color: #e8e6e0;
}
.msg-bot .bubble strong { color: #00e5a0; }
.msg-bot .bubble code {
    background: #1a1d2e; border: 1px solid #2a2d3e;
    border-radius: 4px; padding: 1px 6px;
    font-family: 'IBM Plex Mono', monospace; font-size: 13px; color: #f59e0b;
}
.msg-meta {
    font-size: 11px; color: #374151;
    font-family: 'IBM Plex Mono', monospace;
    margin-top: 5px; padding-left: 48px;
}
.stChatInput { background: #0f1117 !important; }
.stChatInput > div {
    background: #0f1117 !important;
    border: 1px solid #1e2130 !important;
    border-radius: 12px !important;
}
.stChatInput textarea {
    background: transparent !important;
    color: #e8e6e0 !important;
    font-family: 'Syne', sans-serif !important;
}
.stButton > button {
    background: transparent !important;
    border: 1px solid #1e2130 !important;
    color: #9ca3af !important;
    border-radius: 8px !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 12px !important;
    width: 100%; text-align: left !important;
    padding: 8px 12px !important;
    transition: all .2s !important;
}
.stButton > button:hover {
    border-color: #00e5a0 !important;
    color: #00e5a0 !important;
    background: rgba(0,229,160,.06) !important;
}
.sidebar-title { font-size: 20px; font-weight: 700; color: #e8e6e0; letter-spacing: -.01em; }
.sidebar-sub {
    font-size: 10px; color: #4b5563;
    font-family: 'IBM Plex Mono', monospace;
    letter-spacing: .08em; text-transform: uppercase;
    margin-bottom: 1.5rem;
}
.accent { color: #00e5a0; }
.section-lbl {
    font-size: 10px; color: #374151;
    font-family: 'IBM Plex Mono', monospace;
    letter-spacing: .1em; text-transform: uppercase;
    padding: 1rem 0 .4rem;
}
.badge {
    display: inline-block; padding: 1px 8px;
    border-radius: 20px; font-size: 10px; font-weight: 600;
    font-family: 'IBM Plex Mono', monospace;
}
.b-fr { background:rgba(59,130,246,.15); color:#60a5fa; border:1px solid rgba(59,130,246,.3); }
.b-ar { background:rgba(251,191,36,.15); color:#fbbf24; border:1px solid rgba(251,191,36,.3); }
.b-dz { background:rgba(0,229,160,.12); color:#00e5a0; border:1px solid rgba(0,229,160,.3); }
.typing { display:flex; gap:5px; align-items:center; padding:8px 0; }
.typing span {
    width:7px; height:7px; border-radius:50%;
    background:#00e5a0; display:inline-block;
    animation: bounce 1.2s infinite;
}
.typing span:nth-child(2) { animation-delay:.2s; }
.typing span:nth-child(3) { animation-delay:.4s; }
@keyframes bounce {
    0%,60%,100% { transform:translateY(0); opacity:.4; }
    30% { transform:translateY(-6px); opacity:1; }
}
hr { border-color: #1a1d2e !important; margin: .75rem 0 !important; }
::-webkit-scrollbar { width:4px; }
::-webkit-scrollbar-track { background:#0a0b0f; }
::-webkit-scrollbar-thumb { background:#1e2130; border-radius:10px; }
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────
# MONGODB — connexion + helpers
# ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    try:
        c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        c.server_info()
        return c[DB_NAME]
    except Exception as e:
        st.sidebar.error(f"MongoDB ✗ : {e}")
        return None

db = get_db()
MONGO_OK = db is not None

def get_context_mongo() -> str:
    """Extrait un résumé analytique de MongoDB pour enrichir le prompt."""
    if not MONGO_OK:
        return "Base MongoDB non disponible."
    try:
        ctx_parts = []
        
        # Utilise la collection 'dataset_unifie'
        col = db["dataset_unifie"]
        total = col.count_documents({})
        
        # Compter les sentiments
        neg = col.count_documents({"label_final": "negatif"})
        pos = col.count_documents({"label_final": "positif"})
        neu = col.count_documents({"label_final": "neutre"})
        
        # Compter les conflits (si le champ 'conflit' existe)
        conflits = col.count_documents({"conflit": True})
        
        ctx_parts.append(f"Collection 'dataset_unifie' : {total} commentaires")
        ctx_parts.append(f"Sentiments : {neg} négatifs, {pos} positifs, {neu} neutres")
        ctx_parts.append(f"Commentaires avec conflit : {conflits}")
        
        # Ajoute quelques exemples de commentaires négatifs récents
        exemples = col.find({"label_final": "negatif"}).limit(3)
        ctx_parts.append("Exemples de commentaires négatifs :")
        for ex in exemples:
            texte = ex.get("Commentaire_Client_Original", "")[:100]
            ctx_parts.append(f"- {texte}")
        
        return "\n".join(ctx_parts)
    except Exception as e:
        return f"Erreur lecture MongoDB : {e}"

# ──────────────────────────────────────────────────────────────────
# DÉTECTION LANGUE
# ──────────────────────────────────────────────────────────────────
def detecter_langue(texte: str) -> str:
    if re.search(r'[\u0600-\u06FF]', texte):
        return "ar"
    if re.search(r'\b[23789]\b|3likom|wach|chnou|bezzaf|machi|kayn|ndir|sahbi|khra|5ayeb', texte.lower()):
        return "dz"
    try:
        return "fr" if detect(texte) == "fr" else "dz"
    except LangDetectException:
        return "fr"

LANG_BADGE = {
    "fr": '<span class="badge b-fr">FR</span>',
    "ar": '<span class="badge b-ar">عر</span>',
    "dz": '<span class="badge b-dz">DZ</span>',
}

# ──────────────────────────────────────────────────────────────────
# SAUVEGARDE CONVERSATIONS
# ──────────────────────────────────────────────────────────────────
def sauvegarder_conv(conv_id: str, messages: list):
    """Sauvegarde dans session_state + MongoDB."""
    st.session_state.conversations[conv_id]["messages"] = messages
    st.session_state.conversations[conv_id]["updated_at"] = datetime.now().isoformat()
    if MONGO_OK and "chat_sessions" in db.list_collection_names():
        db.chat_sessions.update_one(
            {"conv_id": conv_id},
            {"$set": {
                "messages": messages,
                "updated_at": datetime.now(),
                "title": st.session_state.conversations[conv_id].get("title", "Conversation"),
            }},
            upsert=True,
        )

def charger_convs_mongo() -> dict:
    """Charge les conversations depuis MongoDB."""
    if not MONGO_OK or "chat_sessions" not in db.list_collection_names():
        return {}
    docs = list(db.chat_sessions.find({}, {"_id": 0}).sort("updated_at", -1).limit(30))
    convs = {}
    for d in docs:
        cid = d["conv_id"]
        convs[cid] = {
            "title":      d.get("title", "Conversation"),
            "messages":   d.get("messages", []),
            "created_at": str(d.get("created_at", "")),
            "updated_at": str(d.get("updated_at", "")),
        }
    return convs

# ──────────────────────────────────────────────────────────────────
# SESSION STATE
# ──────────────────────────────────────────────────────────────────
if "conversations" not in st.session_state:
    st.session_state.conversations = charger_convs_mongo()

if "active_conv" not in st.session_state:
    st.session_state.active_conv = None

def nouvelle_conversation():
    import uuid
    cid = str(uuid.uuid4())[:8]
    st.session_state.conversations[cid] = {
        "title":      "Nouvelle conversation",
        "messages":   [],
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }
    st.session_state.active_conv = cid
    if MONGO_OK and "chat_sessions" in db.list_collection_names():
        db.chat_sessions.insert_one({
            "conv_id":    cid,
            "title":      "Nouvelle conversation",
            "messages":   [],
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        })
    return cid

if not st.session_state.active_conv or st.session_state.active_conv not in st.session_state.conversations:
    if st.session_state.conversations:
        st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
    else:
        nouvelle_conversation()

# ──────────────────────────────────────────────────────────────────
# SYSTÈME PROMPT (adapté à ta base)
# ──────────────────────────────────────────────────────────────────
def build_system_prompt() -> str:
    ctx = get_context_mongo()
    return f"""Tu es un assistant analytique intelligent spécialisé dans l'analyse de commentaires télécom algériens.
Tu comprends et réponds en français, en darija algérienne (latin ou arabe), et en arabe standard.
Tu as accès en temps réel aux données de la base MongoDB du projet télécom.

=== DONNÉES ACTUELLES DE LA BASE ===
{ctx}
====================================

Règles importantes :
- Réponds TOUJOURS dans la même langue que l'utilisateur
- Base-toi UNIQUEMENT sur les vraies données ci-dessus
- Sois précis, concis, donne des chiffres réels
- Si l'information n'est pas dans la base, dis-le clairement
"""

# ──────────────────────────────────────────────────────────────────
# APPEL OLLAMA (streaming avec le client personnalisé)
# ──────────────────────────────────────────────────────────────────
def appeler_ollama_stream(messages_hist: list, question: str):
    """Appelle Ollama en streaming et retourne un générateur."""
    system_prompt = build_system_prompt()

    ollama_messages = [{"role": "system", "content": system_prompt}]
    for msg in messages_hist[-20:]:
        ollama_messages.append({"role": msg["role"], "content": msg["content"]})
    ollama_messages.append({"role": "user", "content": question})

    try:
        stream = ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=ollama_messages,
            stream=True,
            options={"temperature": 0.7, "num_predict": 1024, "top_p": 0.9},
        )
        for chunk in stream:
            delta = chunk["message"]["content"]
            if delta:
                yield delta
    except Exception as e:
        yield f"\n⚠️ Erreur Ollama ({OLLAMA_HOST}) : {e}\n"

# ──────────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="sidebar-title">Chat<span class="accent">Bot</span></div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-sub">Ollama · Mistral 7B · Télécom DZ</div>', unsafe_allow_html=True)

    if st.button("＋  Nouvelle conversation"):
        nouvelle_conversation()
        st.rerun()

    st.markdown("---")

    col_m, col_o = st.columns(2)
    with col_m:
        if MONGO_OK:
            st.success("MongoDB ✓", icon="🟢")
        else:
            st.error("MongoDB ✗", icon="🔴")
    with col_o:
        try:
            ollama_client.list()
            st.success("Ollama ✓", icon="🟢")
        except Exception:
            st.error("Ollama ✗", icon="🔴")

    st.caption(f"Ollama: {OLLAMA_HOST}")
    st.caption(f"MongoDB: {MONGO_URI}")

    st.markdown("---")
    st.markdown('<div class="section-lbl">Conversations récentes</div>', unsafe_allow_html=True)

    convs = st.session_state.conversations
    for cid, conv in list(convs.items()):
        title = conv.get("title", "Conversation")[:35]
        is_active = cid == st.session_state.active_conv
        label = f"{'▶ ' if is_active else ''}{title}"
        if st.button(label, key=f"btn_conv_{cid}"):
            st.session_state.active_conv = cid
            st.rerun()

    st.markdown("---")

    if st.button("🗑️ Supprimer cette conversation"):
        cid = st.session_state.active_conv
        if cid and cid in convs:
            del st.session_state.conversations[cid]
            if MONGO_OK and "chat_sessions" in db.list_collection_names():
                db.chat_sessions.delete_one({"conv_id": cid})
            st.session_state.active_conv = None
            st.rerun()

    st.markdown("---")
    st.markdown('<div class="section-lbl">Questions suggérées</div>', unsafe_allow_html=True)
    suggestions = [
        "Pourquoi ya des commentaires négatifs ?",
        "Analyse les sentiments aujourd'hui",
        "wach kayn bezzaf chikayat ?",
        "Donne moi les stats globales",
        "ما هي أكثر المشاكل شيوعاً؟",
        "Quel est le pic de négatifs ?",
    ]
    for sug in suggestions:
        if st.button(sug, key=f"sug_{sug}"):
            st.session_state["pending_question"] = sug
            st.rerun()

# ──────────────────────────────────────────────────────────────────
# ZONE PRINCIPALE — CHAT
# ──────────────────────────────────────────────────────────────────
active_cid = st.session_state.active_conv
active_conv = st.session_state.conversations.get(active_cid, {})
messages = active_conv.get("messages", [])

col_title, col_export = st.columns([4, 1])
with col_title:
    st.markdown(f"""
    <div style="padding:.5rem 0 1rem;">
        <div style="font-size:18px;font-weight:600;color:#e8e6e0;">
            {active_conv.get('title','Conversation')}
        </div>
        <div style="font-size:11px;color:#4b5563;">
            {len(messages)} messages · {OLLAMA_MODEL} · {'MongoDB ✓' if MONGO_OK else 'MongoDB ✗'}
        </div>
    </div>
    """, unsafe_allow_html=True)

with col_export:
    if messages:
        export = json.dumps(messages, ensure_ascii=False, indent=2, default=str)
        st.download_button("⬇️ Exporter", data=export, file_name=f"conv_{active_cid}.json", mime="application/json")

# Affichage des messages
if not messages:
    st.markdown("""
    <div style="text-align:center;padding:4rem 2rem;color:#374151;">
        <div style="font-size:48px;margin-bottom:1rem;">🤖</div>
        <div style="font-size:18px;font-weight:600;color:#6b7280;">Comment puis-je vous aider ?</div>
        <div style="font-size:13px;">Parlez en français · darija · عربي</div>
    </div>
    """, unsafe_allow_html=True)
else:
    for msg in messages:
        role, content, lang = msg["role"], msg["content"], msg.get("lang", "fr")
        ts = msg.get("timestamp", "")[:16]
        badge = LANG_BADGE.get(lang, "")
        if role == "user":
            st.markdown(f'<div class="msg-user"><div class="bubble">{content}</div></div>'
                        f'<div class="msg-meta" style="text-align:right;">{badge} {ts}</div>', unsafe_allow_html=True)
        else:
            content_html = content.replace("\n\n", "<br><br>").replace("\n", "<br>")
            content_html = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', content_html)
            content_html = re.sub(r'`(.*?)`', r'<code>\1</code>', content_html)
            st.markdown(f'<div class="msg-bot"><div class="bot-icon">🤖</div><div><div class="bubble">{content_html}</div><div class="msg-meta">{ts}</div></div></div>', unsafe_allow_html=True)

# Input
pending = st.session_state.pop("pending_question", None)
question = st.chat_input("Posez votre question en français, darija ou arabe…") or pending

if question:
    lang_det = detecter_langue(question)
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M")
    messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
    if len(messages) == 1:
        titre = question[:40] + ("…" if len(question) > 40 else "")
        st.session_state.conversations[active_cid]["title"] = titre
        if MONGO_OK and "chat_sessions" in db.list_collection_names():
            db.chat_sessions.update_one({"conv_id": active_cid}, {"$set": {"title": titre}})
    sauvegarder_conv(active_cid, messages)
    st.rerun()

# Génération réponse
if messages and messages[-1]["role"] == "user":
    with st.container():
        st.markdown('<div class="msg-bot"><div class="bot-icon">🤖</div><div class="bubble"><div class="typing"><span></span><span></span><span></span></div></div></div>', unsafe_allow_html=True)
    
    full_response = ""
    response_placeholder = st.empty()
    for delta in appeler_ollama_stream(messages[:-1], messages[-1]["content"]):
        full_response += delta
        content_html = full_response.replace("\n\n", "<br><br>").replace("\n", "<br>")
        content_html = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', content_html)
        content_html = re.sub(r'`(.*?)`', r'<code>\1</code>', content_html)
        response_placeholder.markdown(f'<div class="msg-bot"><div class="bot-icon">🤖</div><div class="bubble">{content_html}▌</div></div>', unsafe_allow_html=True)
    
    messages.append({"role": "assistant", "content": full_response, "lang": "fr", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")})
    sauvegarder_conv(active_cid, messages)
    st.rerun()