#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
chatbot_ollama.py — CORRIGÉ pour Docker
- MongoDB  : port 27018 (ton docker-compose)
- Ollama   : host.docker.internal:11434 (machine host depuis Docker)
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import re, json, os

# ── Ollama avec host dynamique ─────────────────────────────────────
import ollama as _ollama_lib
from ollama import Client as OllamaClient

# Depuis Docker → pointe vers la machine host
OLLAMA_HOST  = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
ollama_client = OllamaClient(host=OLLAMA_HOST)

# ── MongoDB ────────────────────────────────────────────────────────
from pymongo import MongoClient

# Depuis Docker → mongodb est le nom du service dans docker-compose
# Depuis ta machine locale → localhost:27018
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb_pfe:27017/")
DB_NAME    = "telecom_algerie"
OLLAMA_MODEL = "mistral"

# ── Détection langue ───────────────────────────────────────────────
try:
    from langdetect import detect, LangDetectException
    LANGDETECT_OK = True
except ImportError:
    LANGDETECT_OK = False

def detecter_langue(texte: str) -> str:
    if re.search(r'[\u0600-\u06FF]', texte):
        return "ar"
    if re.search(r'\b[23789]\b|3likom|wach|chnou|bezzaf|machi|kayn|ndir|sahbi|slm|kifak', texte.lower()):
        return "dz"
    if LANGDETECT_OK:
        try:
            return "fr" if detect(texte) == "fr" else "dz"
        except LangDetectException:
            return "fr"
    return "fr"

# ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ChatBot Algérien — Ollama",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
.msg-user { display:flex; justify-content:flex-end; margin:12px 0; }
.msg-user .bubble {
    background:#1a2540; border:1px solid #2a3560;
    border-radius:18px 18px 4px 18px;
    padding:12px 18px; max-width:70%;
    font-size:15px; line-height:1.65; color:#c8d3f5;
}
.msg-bot { display:flex; gap:12px; margin:12px 0; align-items:flex-start; }
.bot-icon {
    width:36px; height:36px; min-width:36px;
    background:linear-gradient(135deg,#00e5a0,#0ea5e9);
    border-radius:10px; display:flex;
    align-items:center; justify-content:center;
    font-size:18px; margin-top:2px;
}
.msg-bot .bubble {
    background:#0f1117; border:1px solid #1e2130;
    border-radius:4px 18px 18px 18px;
    padding:14px 18px; max-width:75%;
    font-size:15px; line-height:1.75; color:#e8e6e0;
}
.msg-bot .bubble strong { color:#00e5a0; }
.msg-bot .bubble code {
    background:#1a1d2e; border:1px solid #2a2d3e;
    border-radius:4px; padding:1px 6px;
    font-family:'IBM Plex Mono',monospace;
    font-size:13px; color:#f59e0b;
}
.msg-meta {
    font-size:11px; color:#374151;
    font-family:'IBM Plex Mono',monospace;
    margin-top:5px; padding-left:48px;
}
.stButton > button {
    background:transparent !important; border:1px solid #1e2130 !important;
    color:#9ca3af !important; border-radius:8px !important;
    font-family:'IBM Plex Mono',monospace !important; font-size:12px !important;
    width:100%; text-align:left !important; padding:8px 12px !important;
    transition:all .2s !important;
}
.stButton > button:hover {
    border-color:#00e5a0 !important; color:#00e5a0 !important;
    background:rgba(0,229,160,.06) !important;
}
.badge { display:inline-block; padding:1px 8px; border-radius:20px;
    font-size:10px; font-weight:600; font-family:'IBM Plex Mono',monospace; }
.b-fr{background:rgba(59,130,246,.15);color:#60a5fa;border:1px solid rgba(59,130,246,.3);}
.b-ar{background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.3);}
.b-dz{background:rgba(0,229,160,.12);color:#00e5a0;border:1px solid rgba(0,229,160,.3);}
.typing{display:flex;gap:5px;align-items:center;padding:8px 0;}
.typing span{
    width:7px;height:7px;border-radius:50%;background:#00e5a0;
    display:inline-block;animation:bounce 1.2s infinite;
}
.typing span:nth-child(2){animation-delay:.2s;}
.typing span:nth-child(3){animation-delay:.4s;}
@keyframes bounce{
    0%,60%,100%{transform:translateY(0);opacity:.4;}
    30%{transform:translateY(-6px);opacity:1;}
}
.section-lbl{
    font-size:10px;color:#374151;font-family:'IBM Plex Mono',monospace;
    letter-spacing:.1em;text-transform:uppercase;padding:1rem 0 .4rem;
}
.sidebar-title{font-size:20px;font-weight:700;color:#e8e6e0;}
.sidebar-sub{font-size:10px;color:#4b5563;font-family:'IBM Plex Mono',monospace;
    letter-spacing:.08em;text-transform:uppercase;margin-bottom:1.5rem;}
.accent{color:#00e5a0;}
hr{border-color:#1a1d2e !important;margin:.75rem 0 !important;}
::-webkit-scrollbar{width:4px;}
::-webkit-scrollbar-track{background:#0a0b0f;}
::-webkit-scrollbar-thumb{background:#1e2130;border-radius:10px;}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────
# CONNEXIONS
# ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    try:
        c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        c.server_info()
        return c[DB_NAME]
    except Exception as e:
        st.sidebar.error(f"MongoDB ✗ : {e}")
        return None

def check_ollama() -> bool:
    try:
        ollama_client.list()
        return True
    except Exception:
        return False

db = get_db()
MONGO_OK   = db is not None
OLLAMA_OK  = check_ollama()

# ──────────────────────────────────────────────────────────────────
# CONTEXTE MONGODB
# ──────────────────────────────────────────────────────────────────
def get_context_mongo() -> str:
    if not MONGO_OK:
        return "Base MongoDB non disponible."
    try:
        lignes = []
        cols = db.list_collection_names()
        lignes.append(f"Collections : {', '.join(cols)}")

        if "commentaires_bruts" in cols:
            total = db.commentaires_bruts.count_documents({})
            non_traites = db.commentaires_bruts.count_documents({"traite": False})
            lignes.append(f"Commentaires bruts : {total} (non traités : {non_traites})")

        if "commentaires_normalises" in cols:
            n = db.commentaires_normalises.count_documents({})
            lignes.append(f"Commentaires normalisés : {n}")

        if "sentiments" in cols:
            neg = db.sentiments.count_documents({"sentiment": {"$in": ["negatif","négatif","negative"]}})
            pos = db.sentiments.count_documents({"sentiment": {"$in": ["positif","positive"]}})
            neu = db.sentiments.count_documents({"sentiment": {"$in": ["neutre","neutral"]}})
            lignes.append(f"Sentiments → négatif:{neg} | positif:{pos} | neutre:{neu}")

            ex_neg = list(db.sentiments.find(
                {"sentiment": {"$in": ["negatif","négatif","negative"]}},
                {"_id":0, "Commentaire_Client":1}
            ).sort("date",-1).limit(5))
            if ex_neg:
                lignes.append("Derniers commentaires négatifs :")
                for e in ex_neg:
                    lignes.append(f"  - {str(e.get('Commentaire_Client',''))[:100]}")

        if "commentaires_bruts" in cols:
            today = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
            today_n = db.commentaires_bruts.count_documents({"date":{"$gte":today}})
            lignes.append(f"Aujourd'hui : {today_n} commentaires")

        return "\n".join(lignes)
    except Exception as e:
        return f"Erreur MongoDB : {e}"

# ──────────────────────────────────────────────────────────────────
# PROMPT SYSTÈME
# ──────────────────────────────────────────────────────────────────
def build_system_prompt() -> str:
    ctx = get_context_mongo()
    return f"""Tu es un assistant analytique intelligent spécialisé dans l'analyse de commentaires télécom algériens.
Tu comprends et réponds en français, en darija algérienne (latin ou arabe), et en arabe standard.
Tu as accès en temps réel aux données MongoDB du projet télécom Algérie.

=== DONNÉES RÉELLES DE LA BASE ===
{ctx}
==================================

Règles :
- Réponds TOUJOURS dans la même langue que l'utilisateur
- Si darija → utilise des expressions naturelles algériennes
- Base tes analyses sur les vraies données ci-dessus
- Donne des chiffres précis quand tu en as
- Si une info manque, dis-le sans inventer
"""

# ──────────────────────────────────────────────────────────────────
# APPEL OLLAMA STREAMING
# ──────────────────────────────────────────────────────────────────
def appeler_ollama_stream(messages_hist: list, question: str):
    msgs = [{"role":"system","content":build_system_prompt()}]
    for m in messages_hist[-20:]:
        msgs.append({"role":m["role"],"content":m["content"]})
    msgs.append({"role":"user","content":question})
    try:
        stream = ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=msgs,
            stream=True,
            options={"temperature":0.7,"num_predict":1024,"top_p":0.9},
        )
        for chunk in stream:
            delta = chunk["message"]["content"]
            if delta:
                yield delta
    except Exception as e:
        yield f"\n⚠️ Erreur Ollama ({OLLAMA_HOST}) : {e}"

def render_md(txt: str) -> str:
    txt = txt.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    txt = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', txt)
    txt = re.sub(r'\*(.*?)\*',     r'<em>\1</em>',         txt)
    txt = re.sub(r'`(.*?)`',       r'<code>\1</code>',     txt)
    txt = txt.replace("\n\n","<br><br>").replace("\n","<br>")
    txt = re.sub(r'<br>[-•] ','<br>• ', txt)
    return txt

LANG_BADGE = {
    "fr":'<span class="badge b-fr">FR</span>',
    "ar":'<span class="badge b-ar">عر</span>',
    "dz":'<span class="badge b-dz">DZ</span>',
}

# ──────────────────────────────────────────────────────────────────
# SESSION STATE
# ──────────────────────────────────────────────────────────────────
def charger_convs_mongo() -> dict:
    if not MONGO_OK:
        return {}
    docs = list(db.chat_sessions.find({},{"_id":0}).sort("updated_at",-1).limit(30))
    return {d["conv_id"]: {
        "title":    d.get("title","Conversation"),
        "messages": d.get("messages",[]),
        "created_at": str(d.get("created_at","")),
    } for d in docs}

if "conversations" not in st.session_state:
    st.session_state.conversations = charger_convs_mongo()
if "active_conv" not in st.session_state:
    st.session_state.active_conv = None

def nouvelle_conversation():
    import uuid
    cid = str(uuid.uuid4())[:8]
    st.session_state.conversations[cid] = {
        "title":"Nouvelle conversation","messages":[],
        "created_at":datetime.now().isoformat(),
    }
    st.session_state.active_conv = cid
    if MONGO_OK:
        db.chat_sessions.insert_one({
            "conv_id":cid,"title":"Nouvelle conversation",
            "messages":[],"created_at":datetime.now(),"updated_at":datetime.now(),
        })
    return cid

def sauvegarder_conv(cid, messages):
    st.session_state.conversations[cid]["messages"] = messages
    if MONGO_OK:
        db.chat_sessions.update_one(
            {"conv_id":cid},
            {"$set":{"messages":messages,"updated_at":datetime.now(),
                     "title":st.session_state.conversations[cid].get("title","")}},
            upsert=True,
        )

if not st.session_state.active_conv or \
   st.session_state.active_conv not in st.session_state.conversations:
    if st.session_state.conversations:
        st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
    else:
        nouvelle_conversation()

# ──────────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sidebar-title">Chat<span class="accent">Bot</span></div>
    <div class="sidebar-sub">Ollama · Mistral 7B · Télécom DZ</div>
    """, unsafe_allow_html=True)

    if st.button("＋  Nouvelle conversation"):
        nouvelle_conversation()
        st.rerun()

    st.markdown("---")

    col_m, col_o = st.columns(2)
    with col_m:
        if MONGO_OK:
            st.success("MongoDB ✓")
        else:
            st.error("MongoDB ✗")
    with col_o:
        if OLLAMA_OK:
            st.success("Ollama ✓")
        else:
            st.error("Ollama ✗")

    # Afficher l'adresse Ollama utilisée
    st.caption(f"🔗 Ollama : `{OLLAMA_HOST}`")
    st.caption(f"🗄️ Mongo  : `{MONGO_URI}`")

    st.markdown("---")
    st.markdown('<div class="section-lbl">Conversations récentes</div>', unsafe_allow_html=True)

    convs = st.session_state.conversations
    for cid, conv in list(convs.items()):
        title = conv.get("title","Conversation")[:35]
        is_active = cid == st.session_state.active_conv
        label = f"{'▶ ' if is_active else ''}{title}"
        if st.button(label, key=f"btn_conv_{cid}"):
            st.session_state.active_conv = cid
            st.rerun()

    st.markdown("---")

    if st.button("🗑️  Supprimer cette conversation"):
        cid = st.session_state.active_conv
        if cid and cid in convs:
            del st.session_state.conversations[cid]
            if MONGO_OK:
                db.chat_sessions.delete_one({"conv_id":cid})
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
        "Compare positif vs négatif",
        "Montre les derniers commentaires",
    ]
    for sug in suggestions:
        if st.button(sug, key=f"sug_{sug}"):
            st.session_state["pending_question"] = sug
            st.rerun()

# ──────────────────────────────────────────────────────────────────
# ZONE CHAT
# ──────────────────────────────────────────────────────────────────
active_cid  = st.session_state.active_conv
active_conv = st.session_state.conversations.get(active_cid, {})
messages    = active_conv.get("messages", [])

col_title, col_export = st.columns([4,1])
with col_title:
    st.markdown(f"""
    <div style="padding:.5rem 0 1rem;">
        <div style="font-size:18px;font-weight:600;color:#e8e6e0;">
            {active_conv.get('title','Conversation')}
        </div>
        <div style="font-size:11px;color:#4b5563;font-family:'IBM Plex Mono',monospace;">
            {len(messages)} messages · {OLLAMA_MODEL} ·
            {'MongoDB ✓' if MONGO_OK else 'MongoDB ✗'} ·
            {'Ollama ✓' if OLLAMA_OK else 'Ollama ✗'}
        </div>
    </div>
    """, unsafe_allow_html=True)

with col_export:
    if messages:
        export = json.dumps(messages, ensure_ascii=False, indent=2, default=str)
        st.download_button("⬇️ Exporter", data=export,
            file_name=f"conv_{active_cid}.json", mime="application/json")

# ── Affichage messages ────────────────────────────────────────────
if not messages:
    st.markdown("""
    <div style="text-align:center;padding:4rem 2rem;color:#374151;">
        <div style="font-size:48px;margin-bottom:1rem;">🤖</div>
        <div style="font-size:18px;font-weight:600;color:#6b7280;margin-bottom:.5rem;">
            Comment puis-je vous aider ?
        </div>
        <div style="font-size:13px;color:#374151;font-family:'IBM Plex Mono',monospace;">
            Parlez en français · darija · عربي
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    for msg in messages:
        role    = msg["role"]
        content = msg["content"]
        lang    = msg.get("lang","fr")
        ts      = msg.get("timestamp","")[:16]
        badge   = LANG_BADGE.get(lang,"")

        if role == "user":
            safe = content.replace("<","&lt;").replace(">","&gt;")
            st.markdown(f"""
            <div class="msg-user"><div class="bubble">{safe}</div></div>
            <div class="msg-meta" style="text-align:right;padding-right:4px;">
                {badge} {ts}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="msg-bot">
                <div class="bot-icon">🤖</div>
                <div>
                    <div class="bubble">{render_md(content)}</div>
                    <div class="msg-meta">{ts}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

# ── Input ─────────────────────────────────────────────────────────
pending  = st.session_state.pop("pending_question", None)
question = st.chat_input("Posez votre question en français, darija ou arabe…") or pending

if question:
    lang_det = detecter_langue(question)
    ts_now   = datetime.now().strftime("%Y-%m-%d %H:%M")
    messages.append({"role":"user","content":question,"lang":lang_det,"timestamp":ts_now})
    if len(messages) == 1:
        titre = question[:40] + ("…" if len(question)>40 else "")
        st.session_state.conversations[active_cid]["title"] = titre
        if MONGO_OK:
            db.chat_sessions.update_one({"conv_id":active_cid},{"$set":{"title":titre}})
    sauvegarder_conv(active_cid, messages)
    st.rerun()

# ── Génération réponse ────────────────────────────────────────────
if messages and messages[-1]["role"] == "user":
    if not OLLAMA_OK:
        st.error(f"""
        ⚠️ Ollama non accessible sur `{OLLAMA_HOST}`

        **Solutions :**
        1. Sur ta machine Ubuntu (hors Docker) : `ollama serve`
        2. Vérifier que le modèle est téléchargé : `ollama pull mistral`
        3. Tester : `curl http://localhost:11434/api/tags`
        """)
    else:
        placeholder = st.empty()
        placeholder.markdown("""
        <div class="msg-bot">
          <div class="bot-icon">🤖</div>
          <div class="bubble">
            <div class="typing"><span></span><span></span><span></span></div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        full_response = ""
        for delta in appeler_ollama_stream(messages[:-1], messages[-1]["content"]):
            full_response += delta
            placeholder.markdown(f"""
            <div class="msg-bot">
              <div class="bot-icon">🤖</div>
              <div class="bubble">{render_md(full_response)}▌</div>
            </div>
            """, unsafe_allow_html=True)

        placeholder.empty()
        messages.append({
            "role":"assistant","content":full_response,
            "lang":"fr","timestamp":datetime.now().strftime("%Y-%m-%d %H:%M"),
        })
        sauvegarder_conv(active_cid, messages)
        st.rerun()