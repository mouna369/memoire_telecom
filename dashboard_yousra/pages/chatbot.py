# # # # # # #!/usr/bin/env python3
# # # # # # # -*- coding: utf-8 -*-
# # # # # # """
# # # # # # chatbot.py — CORRIGÉ — MongoDB service name + Ollama IP fixe
# # # # # # """

# # # # # # import streamlit as st
# # # # # # from ollama import Client
# # # # # # from pymongo import MongoClient
# # # # # # from datetime import datetime
# # # # # # import re, json, os, uuid
# # # # # # from langdetect import detect, LangDetectException

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # CONFIG — lit les variables d'environnement du docker-compose
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # ✅ MONGO_URI vient de docker-compose : mongodb://mongodb:27017/
# # # # # # #    "mongodb" = nom du SERVICE dans docker-compose (pas le container_name)
# # # # # # MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
# # # # # # DB_NAME      = "telecom_algerie"

# # # # # # # ✅ OLLAMA_HOST vient de docker-compose : http://172.17.0.1:11434
# # # # # # #    172.17.0.1 = IP de la machine host vue depuis Docker sur Linux
# # # # # # OLLAMA_HOST  = os.environ.get("OLLAMA_HOST", "http://172.17.0.1:11434")
# # # # # # OLLAMA_MODEL = "llama3.2:1b"

# # # # # # ollama_client = Client(host=OLLAMA_HOST)

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # PAGE CONFIG
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # st.set_page_config(
# # # # # #     page_title="ChatBot Algérien — Ollama",
# # # # # #     page_icon="🤖",
# # # # # #     layout="wide",
# # # # # #     initial_sidebar_state="expanded",
# # # # # # )

# # # # # # if "stop_generation" not in st.session_state:
# # # # # #     st.session_state.stop_generation = False

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # STYLE
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # st.markdown("""
# # # # # # <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap" rel="stylesheet">
# # # # # # <style>
# # # # # # html,body,[class*="css"]{font-family:'Syne',sans-serif!important;background:#0a0b0f!important;color:#e8e6e0!important;}
# # # # # # [data-testid="stAppViewContainer"]{background:#0a0b0f!important;}
# # # # # # [data-testid="stSidebar"]{background:#0d0e14!important;border-right:1px solid #1a1d2e!important;}
# # # # # # .msg-user{display:flex;justify-content:flex-end;margin:12px 0;}
# # # # # # .msg-user .bubble{background:#1a2540;border:1px solid #2a3560;border-radius:18px 18px 4px 18px;padding:12px 18px;max-width:70%;font-size:15px;line-height:1.65;color:#c8d3f5;}
# # # # # # .msg-bot{display:flex;gap:12px;margin:12px 0;align-items:flex-start;}
# # # # # # .bot-icon{width:36px;height:36px;min-width:36px;background:linear-gradient(135deg,#00e5a0,#0ea5e9);border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:18px;margin-top:2px;}
# # # # # # .msg-bot .bubble{background:#0f1117;border:1px solid #1e2130;border-radius:4px 18px 18px 18px;padding:14px 18px;max-width:75%;font-size:15px;line-height:1.75;color:#e8e6e0;}
# # # # # # .msg-bot .bubble strong{color:#00e5a0;}
# # # # # # .msg-bot .bubble code{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:4px;padding:1px 6px;font-family:'IBM Plex Mono',monospace;font-size:13px;color:#f59e0b;}
# # # # # # .msg-meta{font-size:11px;color:#374151;font-family:'IBM Plex Mono',monospace;margin-top:5px;padding-left:48px;}
# # # # # # .stButton>button{background:transparent!important;border:1px solid #1e2130!important;color:#9ca3af!important;border-radius:8px!important;font-family:'IBM Plex Mono',monospace!important;font-size:12px!important;width:100%;text-align:left!important;padding:8px 12px!important;transition:all .2s!important;}
# # # # # # .stButton>button:hover{border-color:#00e5a0!important;color:#00e5a0!important;background:rgba(0,229,160,.06)!important;}
# # # # # # .badge{display:inline-block;padding:1px 8px;border-radius:20px;font-size:10px;font-weight:600;font-family:'IBM Plex Mono',monospace;}
# # # # # # .b-fr{background:rgba(59,130,246,.15);color:#60a5fa;border:1px solid rgba(59,130,246,.3);}
# # # # # # .b-ar{background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.3);}
# # # # # # .b-dz{background:rgba(0,229,160,.12);color:#00e5a0;border:1px solid rgba(0,229,160,.3);}
# # # # # # .sidebar-title{font-size:20px;font-weight:700;color:#e8e6e0;}
# # # # # # .sidebar-sub{font-size:10px;color:#4b5563;font-family:'IBM Plex Mono',monospace;letter-spacing:.08em;text-transform:uppercase;margin-bottom:1.5rem;}
# # # # # # .accent{color:#00e5a0;}
# # # # # # .section-lbl{font-size:10px;color:#374151;font-family:'IBM Plex Mono',monospace;letter-spacing:.1em;text-transform:uppercase;padding:1rem 0 .4rem;}
# # # # # # .typing{display:flex;gap:5px;align-items:center;padding:8px 0;}
# # # # # # .typing span{width:7px;height:7px;border-radius:50%;background:#00e5a0;display:inline-block;animation:bounce 1.2s infinite;}
# # # # # # .typing span:nth-child(2){animation-delay:.2s;}
# # # # # # .typing span:nth-child(3){animation-delay:.4s;}
# # # # # # @keyframes bounce{0%,60%,100%{transform:translateY(0);opacity:.4;}30%{transform:translateY(-6px);opacity:1;}}
# # # # # # hr{border-color:#1a1d2e!important;margin:.75rem 0!important;}
# # # # # # ::-webkit-scrollbar{width:4px;}
# # # # # # ::-webkit-scrollbar-track{background:#0a0b0f;}
# # # # # # ::-webkit-scrollbar-thumb{background:#1e2130;border-radius:10px;}
# # # # # # </style>
# # # # # # """, unsafe_allow_html=True)

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # CONNEXIONS — avec diagnostic clair
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # @st.cache_resource(ttl=0)
# # # # # # def get_db():
# # # # # #     try:
# # # # # #         c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
# # # # # #         c.admin.command("ping")
# # # # # #         return c[DB_NAME]
# # # # # #     except Exception as e:
# # # # # #         return None

# # # # # # def check_ollama() -> bool:
# # # # # #     try:
# # # # # #         ollama_client.list()
# # # # # #         return True
# # # # # #     except Exception:
# # # # # #         return False

# # # # # # db       = get_db()
# # # # # # MONGO_OK = db is not None
# # # # # # OLLAMA_OK = check_ollama()

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # CONTEXTE MONGODB
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # def get_context_mongo(question: str = "") -> str:
# # # # # #     if not MONGO_OK:
# # # # # #         return "Base MongoDB non disponible."
# # # # # #     try:
# # # # # #         lignes = []
# # # # # #         cols = db.list_collection_names()
# # # # # #         lignes.append(f"Collections : {', '.join(cols)}")

# # # # # #         if "dataset_unifie" in cols:
# # # # # #             col = db["dataset_unifie"]
# # # # # #             total    = col.count_documents({})
# # # # # #             neg      = col.count_documents({"label_final": "negatif"})
# # # # # #             pos      = col.count_documents({"label_final": "positif"})
# # # # # #             neu      = col.count_documents({"label_final": "neutre"})
# # # # # #             conflits = col.count_documents({"conflit": True})
# # # # # #             lignes.append(f"dataset_unifie : {total} commentaires")
# # # # # #             lignes.append(f"Sentiments → négatif:{neg} | positif:{pos} | neutre:{neu}")
# # # # # #             lignes.append(f"Conflits d'annotation : {conflits}")

# # # # # #             # Exemples négatifs récents
# # # # # #             ex = list(col.find({"label_final":"negatif"},{"_id":0,"Commentaire_Client_Original":1}).limit(5))
# # # # # #             if ex:
# # # # # #                 lignes.append("Exemples négatifs :")
# # # # # #                 for e in ex:
# # # # # #                     lignes.append(f"  - {str(e.get('Commentaire_Client_Original',''))[:100]}")

# # # # # #         if "commentaires_bruts" in cols:
# # # # # #             today = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
# # # # # #             today_n = db.commentaires_bruts.count_documents({"date":{"$gte":today}})
# # # # # #             lignes.append(f"Commentaires aujourd'hui : {today_n}")

# # # # # #         return "\n".join(lignes)
# # # # # #     except Exception as e:
# # # # # #         return f"Erreur MongoDB : {e}"

# # # # # # def repondre_question_specifique(question: str):
# # # # # #     """Recherche directe par Group_ID sans passer par Ollama."""
# # # # # #     match = re.search(r'groupe[ _]?(\d+)', question, re.IGNORECASE)
# # # # # #     if match and MONGO_OK:
# # # # # #         group_id = f"groupe_{int(match.group(1)):04d}"
# # # # # #         try:
# # # # # #             doc = db["dataset_unifie"].find_one({"Group_ID": group_id})
# # # # # #             if doc:
# # # # # #                 texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# # # # # #                 label = doc.get("label_final", "?")
# # # # # #                 return f"📝 **{group_id}** :\n> {texte}\n\n🏷️ Sentiment : **{label}**"
# # # # # #             return f"❌ Aucun commentaire trouvé pour **{group_id}**."
# # # # # #         except Exception as e:
# # # # # #             return f"Erreur : {e}"
# # # # # #     return None

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # DÉTECTION LANGUE
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # def detecter_langue(texte: str) -> str:
# # # # # #     if re.search(r'[\u0600-\u06FF]', texte):
# # # # # #         return "ar"
# # # # # #     if re.search(r'\b[23789]\b|3likom|wach|chnou|bezzaf|machi|kayn|ndir|sahbi|slm|kifak', texte.lower()):
# # # # # #         return "dz"
# # # # # #     try:
# # # # # #         return "fr" if detect(texte) == "fr" else "dz"
# # # # # #     except LangDetectException:
# # # # # #         return "fr"

# # # # # # LANG_MAPPING = {"fr":"français","ar":"arabe","dz":"darija algérienne"}
# # # # # # LANG_BADGE   = {
# # # # # #     "fr":'<span class="badge b-fr">FR</span>',
# # # # # #     "ar":'<span class="badge b-ar">عر</span>',
# # # # # #     "dz":'<span class="badge b-dz">DZ</span>',
# # # # # # }

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # PROMPT SYSTÈME
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # def build_system_prompt(langue: str) -> str:
# # # # # #     ctx = get_context_mongo()
# # # # # #     return f"""Tu es un assistant analytique spécialisé dans l'analyse de commentaires télécom algériens.
# # # # # # Tu comprends le français, la darija algérienne et l'arabe standard.

# # # # # # === DONNÉES MONGODB ===
# # # # # # {ctx}
# # # # # # =======================

# # # # # # RÈGLES :
# # # # # # - Réponds en : {LANG_MAPPING.get(langue,'français')}
# # # # # # - Base-toi UNIQUEMENT sur les données ci-dessus
# # # # # # - Donne des chiffres précis, ne fabrique rien
# # # # # # - Si une info manque, dis-le clairement
# # # # # # """

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # STREAMING OLLAMA
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # def appeler_ollama_stream(messages_hist: list, question: str, langue: str):
# # # # # #     msgs = [{"role":"system","content":build_system_prompt(langue)}]
# # # # # #     for m in messages_hist[-20:]:
# # # # # #         msgs.append({"role":m["role"],"content":m["content"]})
# # # # # #     msgs.append({"role":"user","content":question})
# # # # # #     try:
# # # # # #         stream = ollama_client.chat(
# # # # # #             model=OLLAMA_MODEL, messages=msgs, stream=True,
# # # # # #             options={"temperature":0.7,"num_predict":512,"top_p":0.9},
# # # # # #         )
# # # # # #         for chunk in stream:
# # # # # #             if st.session_state.stop_generation:
# # # # # #                 st.session_state.stop_generation = False
# # # # # #                 yield "\n\n⏹️ Génération arrêtée."
# # # # # #                 break
# # # # # #             delta = chunk["message"]["content"]
# # # # # #             if delta:
# # # # # #                 yield delta
# # # # # #     except Exception as e:
# # # # # #         yield f"\n⚠️ Erreur Ollama ({OLLAMA_HOST}) : {e}"

# # # # # # def render_md(txt: str) -> str:
# # # # # #     txt = txt.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
# # # # # #     txt = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', txt)
# # # # # #     txt = re.sub(r'\*(.*?)\*',     r'<em>\1</em>',         txt)
# # # # # #     txt = re.sub(r'`(.*?)`',       r'<code>\1</code>',     txt)
# # # # # #     txt = txt.replace("\n\n","<br><br>").replace("\n","<br>")
# # # # # #     txt = re.sub(r'<br>[-•] ','<br>• ',txt)
# # # # # #     return txt

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # SAUVEGARDE CONVERSATIONS
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # def charger_convs_mongo() -> dict:
# # # # # #     if not MONGO_OK:
# # # # # #         return {}
# # # # # #     try:
# # # # # #         docs = list(db.chat_sessions.find({},{"_id":0}).sort("updated_at",-1).limit(30))
# # # # # #         return {d["conv_id"]: {
# # # # # #             "title":    d.get("title","Conversation"),
# # # # # #             "messages": d.get("messages",[]),
# # # # # #             "created_at": str(d.get("created_at","")),
# # # # # #         } for d in docs}
# # # # # #     except Exception:
# # # # # #         return {}

# # # # # # def sauvegarder_conv(cid, messages):
# # # # # #     st.session_state.conversations[cid]["messages"] = messages
# # # # # #     if MONGO_OK:
# # # # # #         try:
# # # # # #             db.chat_sessions.update_one(
# # # # # #                 {"conv_id":cid},
# # # # # #                 {"$set":{"messages":messages,"updated_at":datetime.now(),
# # # # # #                          "title":st.session_state.conversations[cid].get("title","")}},
# # # # # #                 upsert=True,
# # # # # #             )
# # # # # #         except Exception:
# # # # # #             pass

# # # # # # def nouvelle_conversation():
# # # # # #     cid = str(uuid.uuid4())[:8]
# # # # # #     st.session_state.conversations[cid] = {
# # # # # #         "title":"Nouvelle conversation","messages":[],
# # # # # #         "created_at":datetime.now().isoformat(),
# # # # # #     }
# # # # # #     st.session_state.active_conv = cid
# # # # # #     if MONGO_OK:
# # # # # #         try:
# # # # # #             db.chat_sessions.insert_one({
# # # # # #                 "conv_id":cid,"title":"Nouvelle conversation",
# # # # # #                 "messages":[],"created_at":datetime.now(),"updated_at":datetime.now(),
# # # # # #             })
# # # # # #         except Exception:
# # # # # #             pass
# # # # # #     return cid

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # SESSION STATE
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # if "conversations" not in st.session_state:
# # # # # #     st.session_state.conversations = charger_convs_mongo()
# # # # # # if "active_conv" not in st.session_state:
# # # # # #     st.session_state.active_conv = None

# # # # # # if not st.session_state.active_conv or \
# # # # # #    st.session_state.active_conv not in st.session_state.conversations:
# # # # # #     if st.session_state.conversations:
# # # # # #         st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
# # # # # #     else:
# # # # # #         nouvelle_conversation()

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # SIDEBAR
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # with st.sidebar:
# # # # # #     st.markdown(f"""
# # # # # #     <div class="sidebar-title">Chat<span class="accent">Bot</span></div>
# # # # # #     <div class="sidebar-sub">Ollama · {OLLAMA_MODEL} · Télécom DZ</div>
# # # # # #     """, unsafe_allow_html=True)

# # # # # #     if st.button("＋ Nouvelle conversation"):
# # # # # #         nouvelle_conversation()
# # # # # #         st.rerun()

# # # # # #     st.markdown("---")

# # # # # #     # ── Statuts avec diagnostic ───────────────────────────────────
# # # # # #     col_m, col_o = st.columns(2)
# # # # # #     with col_m:
# # # # # #         if MONGO_OK:
# # # # # #             st.success("MongoDB ✓")
# # # # # #         else:
# # # # # #             st.error("MongoDB ✗")
# # # # # #     with col_o:
# # # # # #         if OLLAMA_OK:
# # # # # #             st.success("Ollama ✓")
# # # # # #         else:
# # # # # #             st.error("Ollama ✗")

# # # # # #     # Affiche les adresses utilisées pour le debug
# # # # # #     st.caption(f"🗄️ `{MONGO_URI}`")
# # # # # #     st.caption(f"🤖 `{OLLAMA_HOST}`")

# # # # # #     # Diagnostic si erreur
# # # # # #     if not MONGO_OK:
# # # # # #         st.warning("⚠️ Vérifiez que le service 'mongodb' est lancé dans Docker")
# # # # # #     if not OLLAMA_OK:
# # # # # #         st.warning("⚠️ Lancez `ollama serve` sur votre machine Ubuntu")
# # # # # #         st.code("ollama serve", language="bash")

# # # # # #     st.markdown("---")
# # # # # #     st.markdown('<div class="section-lbl">Conversations récentes</div>', unsafe_allow_html=True)

# # # # # #     convs = st.session_state.conversations
# # # # # #     for cid, conv in list(convs.items()):
# # # # # #         title = conv.get("title","Conversation")[:35]
# # # # # #         is_active = cid == st.session_state.active_conv
# # # # # #         label = f"{'▶ ' if is_active else ''}{title}"
# # # # # #         if st.button(label, key=f"btn_conv_{cid}"):
# # # # # #             st.session_state.active_conv = cid
# # # # # #             st.rerun()

# # # # # #     st.markdown("---")

# # # # # #     if st.button("🗑️ Supprimer conversation"):
# # # # # #         cid = st.session_state.active_conv
# # # # # #         if cid and cid in convs:
# # # # # #             del st.session_state.conversations[cid]
# # # # # #             if MONGO_OK:
# # # # # #                 try:
# # # # # #                     db.chat_sessions.delete_one({"conv_id":cid})
# # # # # #                 except Exception:
# # # # # #                     pass
# # # # # #             st.session_state.active_conv = None
# # # # # #             st.rerun()

# # # # # #     st.markdown("---")
# # # # # #     st.markdown('<div class="section-lbl">Questions suggérées</div>', unsafe_allow_html=True)
# # # # # #     suggestions = [
# # # # # #         "Donne moi les stats globales",
# # # # # #         "Pourquoi ya des commentaires négatifs ?",
# # # # # #         "wach kayn bezzaf chikayat ?",
# # # # # #         "Quelles collections sont disponibles ?",
# # # # # #         "ما هي أكثر المشاكل شيوعاً؟",
# # # # # #         "Quel est le taux de conflits ?",
# # # # # #         "Montre des exemples négatifs",
# # # # # #         "Compare positif vs négatif",
# # # # # #     ]
# # # # # #     for sug in suggestions:
# # # # # #         if st.button(sug, key=f"sug_{sug}"):
# # # # # #             st.session_state["pending_question"] = sug
# # # # # #             st.rerun()

# # # # # #     if st.session_state.conversations.get(st.session_state.active_conv, {}).get("messages"):
# # # # # #         st.markdown("---")
# # # # # #         msgs_export = st.session_state.conversations[st.session_state.active_conv]["messages"]
# # # # # #         st.download_button(
# # # # # #             "⬇️ Exporter JSON",
# # # # # #             data=json.dumps(msgs_export, ensure_ascii=False, indent=2, default=str),
# # # # # #             file_name=f"conv_{st.session_state.active_conv}.json",
# # # # # #             mime="application/json",
# # # # # #             use_container_width=True,
# # # # # #         )

# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # # ZONE PRINCIPALE — CHAT
# # # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # active_cid  = st.session_state.active_conv
# # # # # # active_conv = st.session_state.conversations.get(active_cid, {})
# # # # # # messages    = active_conv.get("messages", [])

# # # # # # col_title, col_export = st.columns([4,1])
# # # # # # with col_title:
# # # # # #     st.markdown(f"""
# # # # # #     <div style="padding:.5rem 0 1rem;">
# # # # # #       <div style="font-size:18px;font-weight:600;color:#e8e6e0;">
# # # # # #         {active_conv.get('title','Conversation')}
# # # # # #       </div>
# # # # # #       <div style="font-size:11px;color:#4b5563;font-family:'IBM Plex Mono',monospace;">
# # # # # #         {len(messages)} messages · {OLLAMA_MODEL} ·
# # # # # #         {'🟢 MongoDB' if MONGO_OK else '🔴 MongoDB'} ·
# # # # # #         {'🟢 Ollama' if OLLAMA_OK else '🔴 Ollama'}
# # # # # #       </div>
# # # # # #     </div>
# # # # # #     """, unsafe_allow_html=True)

# # # # # # # Affichage messages
# # # # # # if not messages:
# # # # # #     st.markdown("""
# # # # # #     <div style="text-align:center;padding:4rem 2rem;color:#374151;">
# # # # # #       <div style="font-size:48px;margin-bottom:1rem;">🤖</div>
# # # # # #       <div style="font-size:18px;font-weight:600;color:#6b7280;margin-bottom:.5rem;">
# # # # # #         Comment puis-je vous aider ?
# # # # # #       </div>
# # # # # #       <div style="font-size:13px;color:#374151;font-family:'IBM Plex Mono',monospace;">
# # # # # #         Parlez en français · darija · عربي
# # # # # #       </div>
# # # # # #     </div>
# # # # # #     """, unsafe_allow_html=True)
# # # # # # else:
# # # # # #     for msg in messages:
# # # # # #         role    = msg["role"]
# # # # # #         content = msg["content"]
# # # # # #         lang    = msg.get("lang","fr")
# # # # # #         ts      = msg.get("timestamp","")[:16]
# # # # # #         badge   = LANG_BADGE.get(lang,"")
# # # # # #         if role == "user":
# # # # # #             safe = content.replace("<","&lt;").replace(">","&gt;")
# # # # # #             st.markdown(f"""
# # # # # #             <div class="msg-user"><div class="bubble">{safe}</div></div>
# # # # # #             <div class="msg-meta" style="text-align:right;padding-right:4px;">{badge} {ts}</div>
# # # # # #             """, unsafe_allow_html=True)
# # # # # #         else:
# # # # # #             st.markdown(f"""
# # # # # #             <div class="msg-bot">
# # # # # #               <div class="bot-icon">🤖</div>
# # # # # #               <div>
# # # # # #                 <div class="bubble">{render_md(content)}</div>
# # # # # #                 <div class="msg-meta">{ts}</div>
# # # # # #               </div>
# # # # # #             </div>
# # # # # #             """, unsafe_allow_html=True)

# # # # # # # ── Input ─────────────────────────────────────────────────────────
# # # # # # pending  = st.session_state.pop("pending_question", None)
# # # # # # question = st.chat_input("Posez votre question en français, darija ou arabe…") or pending

# # # # # # if question:
# # # # # #     rep_directe = repondre_question_specifique(question)
# # # # # #     ts_now = datetime.now().strftime("%Y-%m-%d %H:%M")
# # # # # #     lang_det = detecter_langue(question)

# # # # # #     if rep_directe:
# # # # # #         messages.append({"role":"user","content":question,"lang":lang_det,"timestamp":ts_now})
# # # # # #         messages.append({"role":"assistant","content":rep_directe,"lang":"fr","timestamp":ts_now})
# # # # # #         sauvegarder_conv(active_cid, messages)
# # # # # #         st.rerun()
# # # # # #     else:
# # # # # #         messages.append({"role":"user","content":question,"lang":lang_det,"timestamp":ts_now})
# # # # # #         if len(messages) == 1:
# # # # # #             titre = question[:40] + ("…" if len(question)>40 else "")
# # # # # #             st.session_state.conversations[active_cid]["title"] = titre
# # # # # #             if MONGO_OK:
# # # # # #                 try:
# # # # # #                     db.chat_sessions.update_one(
# # # # # #                         {"conv_id":active_cid},{"$set":{"title":titre}}
# # # # # #                     )
# # # # # #                 except Exception:
# # # # # #                     pass
# # # # # #         sauvegarder_conv(active_cid, messages)
# # # # # #         st.rerun()

# # # # # # # ── Génération ────────────────────────────────────────────────────
# # # # # # if messages and messages[-1]["role"] == "user":
# # # # # #     st.session_state.stop_generation = False

# # # # # #     if st.button("⏹️ Stop", key="stop_btn"):
# # # # # #         st.session_state.stop_generation = True

# # # # # #     if not OLLAMA_OK:
# # # # # #         st.error(f"""
# # # # # #         ⚠️ Ollama non accessible sur `{OLLAMA_HOST}`

# # # # # #         **Sur ta machine Ubuntu :**
# # # # # #         ```bash
# # # # # #         ollama serve
# # # # # #         ```
# # # # # #         Puis vérifie avec :
# # # # # #         ```bash
# # # # # #         curl http://172.17.0.1:11434/api/tags
# # # # # #         ```
# # # # # #         """)
# # # # # #     else:
# # # # # #         placeholder = st.empty()
# # # # # #         placeholder.markdown("""
# # # # # #         <div class="msg-bot">
# # # # # #           <div class="bot-icon">🤖</div>
# # # # # #           <div class="bubble">
# # # # # #             <div class="typing"><span></span><span></span><span></span></div>
# # # # # #           </div>
# # # # # #         </div>
# # # # # #         """, unsafe_allow_html=True)

# # # # # #         full_response = ""
# # # # # #         langue_user = messages[-1].get("lang","fr")

# # # # # #         for delta in appeler_ollama_stream(messages[:-1], messages[-1]["content"], langue_user):
# # # # # #             full_response += delta
# # # # # #             placeholder.markdown(f"""
# # # # # #             <div class="msg-bot">
# # # # # #               <div class="bot-icon">🤖</div>
# # # # # #               <div class="bubble">{render_md(full_response)}▌</div>
# # # # # #             </div>
# # # # # #             """, unsafe_allow_html=True)

# # # # # #         placeholder.empty()
# # # # # #         messages.append({
# # # # # #             "role":"assistant","content":full_response,
# # # # # #             "lang":langue_user,
# # # # # #             "timestamp":datetime.now().strftime("%Y-%m-%d %H:%M"),
# # # # # #         })
# # # # # #         sauvegarder_conv(active_cid, messages)
# # # # # #         st.rerun()



# # # # # # Chatbot groq · PY333333333333333333333333333333333333
# # # # # #!/usr/bin/env python3
# # # # # # -*- coding: utf-8 -*-
# # # # # """
# # # # # chatbot_groq.py — ChatBot Algérien avec GROQ (ultra rapide) + MongoDB
# # # # # Conversation naturelle en français, darija, arabe
# # # # # Analyse intelligente des commentaires télécom
# # # # # """

# # # # # import streamlit as st
# # # # # from groq import Groq
# # # # # from pymongo import MongoClient
# # # # # from datetime import datetime
# # # # # import re, json, os, uuid
# # # # # from langdetect import detect, LangDetectException
# # # # # from streamlit_mic_recorder import mic_recorder
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # CONFIG
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # Clé Groq — depuis variable d'environnement docker-compose
# # # # # GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
# # # # # GROQ_MODEL   = "llama-3.1-8b-instant"  # excellent arabe/français/darija
# # # # # # Autres modèles dispo :
# # # # # # "mixtral-8x7b-32768"     → très bon aussi
# # # # # # "gemma2-9b-it"           → plus rapide
# # # # # # "llama-3.1-8b-instant"  → ultra rapide mais moins précis

# # # # # MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018/")
# # # # # DB_NAME   = "telecom_algerie"

# # # # # groq_client = Groq(api_key=GROQ_API_KEY)

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # PAGE CONFIG
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # st.set_page_config(
# # # # #     page_title="ChatBot Algérien — Groq ⚡",
# # # # #     page_icon="🤖",
# # # # #     layout="wide",
# # # # #     initial_sidebar_state="expanded",
# # # # # )

# # # # # if "stop_generation" not in st.session_state:
# # # # #     st.session_state.stop_generation = False

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # STYLE
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # st.markdown("""
# # # # # <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap" rel="stylesheet">
# # # # # <style>
# # # # # html,body,[class*="css"]{
# # # # #     font-family:'Syne',sans-serif!important;
# # # # #     background:#0a0b0f!important;
# # # # #     color:#e8e6e0!important;
# # # # # }
# # # # # [data-testid="stAppViewContainer"]{background:#0a0b0f!important;}
# # # # # [data-testid="stSidebar"]{
# # # # #     background:#0d0e14!important;
# # # # #     border-right:1px solid #1a1d2e!important;
# # # # # }
# # # # # .msg-user{display:flex;justify-content:flex-end;margin:12px 0;}
# # # # # .msg-user .bubble{
# # # # #     background:#1a2540;border:1px solid #2a3560;
# # # # #     border-radius:18px 18px 4px 18px;
# # # # #     padding:12px 18px;max-width:70%;
# # # # #     font-size:15px;line-height:1.65;color:#c8d3f5;
# # # # # }
# # # # # .msg-bot{display:flex;gap:12px;margin:12px 0;align-items:flex-start;}
# # # # # .bot-icon{
# # # # #     width:36px;height:36px;min-width:36px;
# # # # #     background:linear-gradient(135deg,#f97316,#eab308);
# # # # #     border-radius:10px;display:flex;
# # # # #     align-items:center;justify-content:center;
# # # # #     font-size:18px;margin-top:2px;
# # # # # }
# # # # # .msg-bot .bubble{
# # # # #     background:#0f1117;border:1px solid #1e2130;
# # # # #     border-radius:4px 18px 18px 18px;
# # # # #     padding:14px 18px;max-width:75%;
# # # # #     font-size:15px;line-height:1.75;color:#e8e6e0;
# # # # # }
# # # # # .msg-bot .bubble strong{color:#f97316;}
# # # # # .msg-bot .bubble code{
# # # # #     background:#1a1d2e;border:1px solid #2a2d3e;
# # # # #     border-radius:4px;padding:1px 6px;
# # # # #     font-family:'IBM Plex Mono',monospace;
# # # # #     font-size:13px;color:#fbbf24;
# # # # # }
# # # # # .msg-meta{
# # # # #     font-size:11px;color:#374151;
# # # # #     font-family:'IBM Plex Mono',monospace;
# # # # #     margin-top:5px;padding-left:48px;
# # # # # }
# # # # # .stButton>button{
# # # # #     background:transparent!important;
# # # # #     border:1px solid #1e2130!important;
# # # # #     color:#9ca3af!important;border-radius:8px!important;
# # # # #     font-family:'IBM Plex Mono',monospace!important;
# # # # #     font-size:12px!important;width:100%;
# # # # #     text-align:left!important;padding:8px 12px!important;
# # # # #     transition:all .2s!important;
# # # # # }
# # # # # .stButton>button:hover{
# # # # #     border-color:#f97316!important;
# # # # #     color:#f97316!important;
# # # # #     background:rgba(249,115,22,.06)!important;
# # # # # }
# # # # # .badge{
# # # # #     display:inline-block;padding:1px 8px;
# # # # #     border-radius:20px;font-size:10px;font-weight:600;
# # # # #     font-family:'IBM Plex Mono',monospace;
# # # # # }
# # # # # .b-fr{background:rgba(59,130,246,.15);color:#60a5fa;border:1px solid rgba(59,130,246,.3);}
# # # # # .b-ar{background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.3);}
# # # # # .b-dz{background:rgba(249,115,22,.15);color:#fb923c;border:1px solid rgba(249,115,22,.3);}
# # # # # .sidebar-title{font-size:20px;font-weight:700;color:#e8e6e0;}
# # # # # .sidebar-sub{
# # # # #     font-size:10px;color:#4b5563;
# # # # #     font-family:'IBM Plex Mono',monospace;
# # # # #     letter-spacing:.08em;text-transform:uppercase;
# # # # #     margin-bottom:1.5rem;
# # # # # }
# # # # # .accent{color:#f97316;}
# # # # # .section-lbl{
# # # # #     font-size:10px;color:#374151;
# # # # #     font-family:'IBM Plex Mono',monospace;
# # # # #     letter-spacing:.1em;text-transform:uppercase;
# # # # #     padding:1rem 0 .4rem;
# # # # # }
# # # # # .typing{display:flex;gap:5px;align-items:center;padding:8px 0;}
# # # # # .typing span{
# # # # #     width:7px;height:7px;border-radius:50%;
# # # # #     background:#f97316;display:inline-block;
# # # # #     animation:bounce 1.2s infinite;
# # # # # }
# # # # # .typing span:nth-child(2){animation-delay:.2s;}
# # # # # .typing span:nth-child(3){animation-delay:.4s;}
# # # # # @keyframes bounce{
# # # # #     0%,60%,100%{transform:translateY(0);opacity:.4;}
# # # # #     30%{transform:translateY(-6px);opacity:1;}
# # # # # }
# # # # # .groq-badge{
# # # # #     display:inline-block;
# # # # #     background:linear-gradient(135deg,#f97316,#eab308);
# # # # #     color:#000;font-size:10px;font-weight:700;
# # # # #     padding:2px 8px;border-radius:20px;
# # # # #     font-family:'IBM Plex Mono',monospace;
# # # # #     margin-left:6px;
# # # # # }
# # # # # hr{border-color:#1a1d2e!important;margin:.75rem 0!important;}
# # # # # ::-webkit-scrollbar{width:4px;}
# # # # # ::-webkit-scrollbar-track{background:#0a0b0f;}
# # # # # ::-webkit-scrollbar-thumb{background:#1e2130;border-radius:10px;}
# # # # # </style>
# # # # # """, unsafe_allow_html=True)

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # MONGODB
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # @st.cache_resource(ttl=0)
# # # # # def get_db():
# # # # #     try:
# # # # #         c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
# # # # #         c.admin.command("ping")
# # # # #         return c[DB_NAME]
# # # # #     except Exception:
# # # # #         return None

# # # # # db       = get_db()
# # # # # MONGO_OK = db is not None

# # # # # def check_groq() -> bool:
# # # # #     try:
# # # # #         groq_client.models.list()
# # # # #         return True
# # # # #     except Exception:
# # # # #         return False

# # # # # GROQ_OK = check_groq()

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # CONTEXTE MONGODB
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # def get_context_mongo() -> str:
# # # # #     if not MONGO_OK:
# # # # #         return "Base MongoDB non disponible."
# # # # #     try:
# # # # #         lignes = []
# # # # #         cols = db.list_collection_names()
# # # # #         lignes.append(f"Collections disponibles : {', '.join(cols)}")

# # # # #         if "dataset_unifie" in cols:
# # # # #             col = db["dataset_unifie"]
# # # # #             total = col.count_documents({})
# # # # #             neg = col.count_documents({"label_final": "negatif"})
# # # # #             pos = col.count_documents({"label_final": "positif"})
# # # # #             neu = col.count_documents({"label_final": "neutre"})
# # # # #             conflits = col.count_documents({"conflit": True})
# # # # #             lignes.append(f"dataset_unifie : {total} commentaires")
# # # # #             lignes.append(f"Sentiments → négatif:{neg} | positif:{pos} | neutre:{neu}")
# # # # #             lignes.append(f"Conflits d'annotation : {conflits}")

# # # # #             # 🔥 Statistiques par mois (en parsant la string de date)
# # # # #             # Format supposé : "DD/MM/YYYY HH:MM" ou "DD/MM/YYYY"
# # # # #             pipeline_mois = [
# # # # #                 {"$match": {"label_final": {"$in": ["negatif", "positif", "neutre"]}}},
# # # # #                 {"$addFields": {
# # # # #                     "mois": {
# # # # #                         "$let": {
# # # # #                             "vars": {
# # # # #                                 "dateParts": {"$split": ["$dates", "/"]}
# # # # #                             },
# # # # #                             "in": {"$arrayElemAt": ["$$dateParts", 1]}
# # # # #                         }
# # # # #                     }
# # # # #                 }},
# # # # #                 {"$group": {
# # # # #                     "_id": "$mois",
# # # # #                     "total": {"$sum": 1},
# # # # #                     "neg": {"$sum": {"$cond": [{"$eq": ["$label_final", "negatif"]}, 1, 0]}}
# # # # #                 }},
# # # # #                 {"$sort": {"_id": 1}}
# # # # #             ]
            
# # # # #             stats_mois = list(col.aggregate(pipeline_mois))
# # # # #             if stats_mois:
# # # # #                 lignes.append("📊 Répartition par mois :")
# # # # #                 mois_noms = {
# # # # #                     "01": "Janvier", "02": "Février", "03": "Mars", "04": "Avril",
# # # # #                     "05": "Mai", "06": "Juin", "07": "Juillet", "08": "Août",
# # # # #                     "09": "Septembre", "10": "Octobre", "11": "Novembre", "12": "Décembre"
# # # # #                 }
# # # # #                 for stat in stats_mois:
# # # # #                     mois_num = stat['_id']
# # # # #                     total_mois = stat['total']
# # # # #                     pct_neg = round(stat['neg'] / total_mois * 100, 1) if total_mois > 0 else 0
# # # # #                     nom_mois = mois_noms.get(mois_num, mois_num)
# # # # #                     lignes.append(f"  - {nom_mois}: {pct_neg}% négatifs ({stat['neg']}/{total_mois})")
# # # # #             else:
# # # # #                 lignes.append("⚠️ Données mensuelles non disponibles")

# # # # #         return "\n".join(lignes)
# # # # #     except Exception as e:
# # # # #         return f"Erreur MongoDB : {e}"
# # # # # def repondre_question_specifique(question: str):
# # # # #     """Recherche directe par Group_ID sans passer par le LLM."""
# # # # #     match = re.search(r'groupe[ _]?(\d+)', question, re.IGNORECASE)
# # # # #     if match and MONGO_OK:
# # # # #         group_id = f"groupe_{int(match.group(1)):04d}"
# # # # #         try:
# # # # #             doc = db["dataset_unifie"].find_one({"Group_ID": group_id})
# # # # #             if doc:
# # # # #                 texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# # # # #                 label = doc.get("label_final", "?")
# # # # #                 return f"📝 **{group_id}** :\n> {texte}\n\n🏷️ Sentiment : **{label}**"
# # # # #             return f"❌ Aucun commentaire trouvé pour **{group_id}**."
# # # # #         except Exception as e:
# # # # #             return f"Erreur : {e}"
# # # # #     return None
# # # # # def repondre_question_par_date(question: str) -> str or None:
# # # # #     """Recherche les commentaires par date (format DD/MM/YYYY)"""
# # # # #     import re
# # # # #     # Cherche une date au format DD/MM/YYYY dans la question
# # # # #     match = re.search(r'(\d{2}/\d{2}/\d{4})', question)
# # # # #     if match and MONGO_OK:
# # # # #         date_cherchee = match.group(1)
# # # # #         try:
# # # # #             col = db["dataset_unifie"]
# # # # #             # Chercher les commentaires avec cette date (dans le champ 'dates')
# # # # #             docs = list(col.find({"dates": {"$regex": date_cherchee}}).limit(5))
            
# # # # #             if docs:
# # # # #                 resultat = f"📝 **Commentaires du {date_cherchee}** :\n\n"
# # # # #                 for i, doc in enumerate(docs, 1):
# # # # #                     texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# # # # #                     label = doc.get("label_final", "?")
# # # # #                     resultat += f"{i}. {texte}\n   🏷️ Sentiment : {label}\n\n"
# # # # #                 return resultat
# # # # #             else:
# # # # #                 return f"❌ Aucun commentaire trouvé pour la date **{date_cherchee}**."
# # # # #         except Exception as e:
# # # # #             return f"Erreur recherche : {e}"
# # # # #     return None
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # DÉTECTION LANGUE
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # def detecter_langue(texte: str) -> str:
# # # # #     if re.search(r'[\u0600-\u06FF]', texte):
# # # # #         return "ar"
# # # # #     if re.search(
# # # # #         r'\b[23789]\b|3likom|wach|chnou|bezzaf|machi|kayn|ndir|sahbi|slm|kifak|rahi|mazal',
# # # # #         texte.lower()
# # # # #     ):
# # # # #         return "dz"
# # # # #     try:
# # # # #         return "fr" if detect(texte) == "fr" else "dz"
# # # # #     except LangDetectException:
# # # # #         return "fr"

# # # # # LANG_MAPPING = {"fr": "français", "ar": "arabe", "dz": "darija algérienne"}
# # # # # LANG_BADGE   = {
# # # # #     "fr": '<span class="badge b-fr">FR</span>',
# # # # #     "ar": '<span class="badge b-ar">عر</span>',
# # # # #     "dz": '<span class="badge b-dz">DZ</span>',
# # # # # }

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # PROMPT SYSTÈME
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # def build_system_prompt(langue: str) -> str:
# # # # #     ctx = get_context_mongo()
# # # # #     return f"""Tu es un assistant analytique intelligent spécialisé dans l'analyse 
# # # # # de commentaires clients des opérateurs télécom algériens (Djezzy, Mobilis, Ooredoo).
# # # # # Tu comprends et réponds parfaitement en français, en darija algérienne et en arabe standard.

# # # # # === DONNÉES RÉELLES DE LA BASE MONGODB ===
# # # # # {ctx}
# # # # # ==========================================

# # # # # RÈGLES IMPORTANTES :
# # # # # - Réponds OBLIGATOIREMENT en : {LANG_MAPPING.get(langue, 'français')}
# # # # # - Si l'utilisateur parle darija → réponds en darija naturelle algérienne
# # # # # - Base tes analyses UNIQUEMENT sur les vraies données ci-dessus
# # # # # - Donne des chiffres précis, ne fabrique rien
# # # # # - Si une information manque, dis-le clairement
# # # # # - Formate tes réponses avec des listes et titres quand pertinent
# # # # # - Tu peux analyser les tendances, expliquer les pics négatifs, comparer des périodes
# # # # # """

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # APPEL GROQ — STREAMING ⚡
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # def appeler_groq_stream(messages_hist: list, question: str, langue: str):
# # # # #     """Appelle Groq en streaming — ultra rapide !"""
# # # # #     msgs = [{"role": "system", "content": build_system_prompt(langue)}]

# # # # #     # Historique (max 10 derniers échanges)
# # # # #     for m in messages_hist[-20:]:
# # # # #         msgs.append({"role": m["role"], "content": m["content"]})
# # # # #     msgs.append({"role": "user", "content": question})

# # # # #     try:
# # # # #         stream = groq_client.chat.completions.create(
# # # # #             model=GROQ_MODEL,
# # # # #             messages=msgs,
# # # # #             stream=True,
# # # # #             temperature=0.7,
# # # # #             max_tokens=256,
# # # # #             top_p=0.9,
# # # # #         )
# # # # #         for chunk in stream:
# # # # #             if st.session_state.stop_generation:
# # # # #                 st.session_state.stop_generation = False
# # # # #                 yield "\n\n⏹️ Génération arrêtée."
# # # # #                 break
# # # # #             delta = chunk.choices[0].delta.content
# # # # #             if delta:
# # # # #                 yield delta
# # # # #     except Exception as e:
# # # # #         yield f"\n⚠️ Erreur Groq : {e}\n\nVérifiez votre clé API sur https://console.groq.com"

# # # # # def render_md(txt: str) -> str:
# # # # #     """Convertit markdown simple en HTML sécurisé."""
# # # # #     txt = txt.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
# # # # #     txt = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', txt)
# # # # #     txt = re.sub(r'\*(.*?)\*',     r'<em>\1</em>',         txt)
# # # # #     txt = re.sub(r'`(.*?)`',       r'<code>\1</code>',     txt)
# # # # #     txt = txt.replace("\n\n","<br><br>").replace("\n","<br>")
# # # # #     txt = re.sub(r'<br>[-•] ','<br>• ', txt)
# # # # #     return txt

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # SAUVEGARDE CONVERSATIONS
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # def charger_convs_mongo() -> dict:
# # # # #     if not MONGO_OK:
# # # # #         return {}
# # # # #     try:
# # # # #         docs = list(db.chat_sessions.find({},{"_id":0}).sort("updated_at",-1).limit(30))
# # # # #         return {d["conv_id"]: {
# # # # #             "title":    d.get("title","Conversation"),
# # # # #             "messages": d.get("messages",[]),
# # # # #             "created_at": str(d.get("created_at","")),
# # # # #         } for d in docs}
# # # # #     except Exception:
# # # # #         return {}

# # # # # def sauvegarder_conv(cid, messages):
# # # # #     st.session_state.conversations[cid]["messages"] = messages
# # # # #     if MONGO_OK:
# # # # #         try:
# # # # #             db.chat_sessions.update_one(
# # # # #                 {"conv_id": cid},
# # # # #                 {"$set": {
# # # # #                     "messages":   messages,
# # # # #                     "updated_at": datetime.now(),
# # # # #                     "title":      st.session_state.conversations[cid].get("title",""),
# # # # #                 }},
# # # # #                 upsert=True,
# # # # #             )
# # # # #         except Exception:
# # # # #             pass

# # # # # def nouvelle_conversation():
# # # # #     cid = str(uuid.uuid4())[:8]
# # # # #     st.session_state.conversations[cid] = {
# # # # #         "title":      "Nouvelle conversation",
# # # # #         "messages":   [],
# # # # #         "created_at": datetime.now().isoformat(),
# # # # #     }
# # # # #     st.session_state.active_conv = cid
# # # # #     if MONGO_OK:
# # # # #         try:
# # # # #             db.chat_sessions.insert_one({
# # # # #                 "conv_id":    cid,
# # # # #                 "title":      "Nouvelle conversation",
# # # # #                 "messages":   [],
# # # # #                 "created_at": datetime.now(),
# # # # #                 "updated_at": datetime.now(),
# # # # #             })
# # # # #         except Exception:
# # # # #             pass
# # # # #     return cid

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # SESSION STATE
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # if "conversations" not in st.session_state:
# # # # #     st.session_state.conversations = charger_convs_mongo()
# # # # # if "active_conv" not in st.session_state:
# # # # #     st.session_state.active_conv = None

# # # # # if not st.session_state.active_conv or \
# # # # #    st.session_state.active_conv not in st.session_state.conversations:
# # # # #     if st.session_state.conversations:
# # # # #         st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
# # # # #     else:
# # # # #         nouvelle_conversation()

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # SIDEBAR
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # with st.sidebar:
# # # # #     st.markdown(f"""
# # # # #     <div class="sidebar-title">
# # # # #         Chat<span class="accent">Bot</span>
# # # # #         <span class="groq-badge">⚡ GROQ</span>
# # # # #     </div>
# # # # #     <div class="sidebar-sub">{GROQ_MODEL} · Télécom DZ</div>
# # # # #     """, unsafe_allow_html=True)

# # # # #     if st.button("＋ Nouvelle conversation"):
# # # # #         nouvelle_conversation()
# # # # #         st.rerun()

# # # # #     st.markdown("---")

# # # # #     # Statuts
# # # # #     col_m, col_g = st.columns(2)
# # # # #     with col_m:
# # # # #         if MONGO_OK:
# # # # #             st.success("MongoDB ✓")
# # # # #         else:
# # # # #             st.error("MongoDB ✗")
# # # # #     with col_g:
# # # # #         if GROQ_OK:
# # # # #             st.success("Groq ✓")
# # # # #         else:
# # # # #             st.error("Groq ✗")

# # # # #     if not GROQ_OK:
# # # # #         st.warning("Vérifiez votre clé GROQ_API_KEY")
# # # # #         st.code("https://console.groq.com", language="bash")

# # # # #     st.markdown("---")
# # # # #     st.markdown('<div class="section-lbl">Conversations récentes</div>', unsafe_allow_html=True)

# # # # #     convs = st.session_state.conversations
# # # # #     for cid, conv in list(convs.items()):
# # # # #         title = conv.get("title","Conversation")[:35]
# # # # #         is_active = cid == st.session_state.active_conv
# # # # #         label = f"{'▶ ' if is_active else ''}{title}"
# # # # #         if st.button(label, key=f"btn_conv_{cid}"):
# # # # #             st.session_state.active_conv = cid
# # # # #             st.rerun()

# # # # #     st.markdown("---")

# # # # #     if st.button("🗑️ Supprimer conversation"):
# # # # #         cid = st.session_state.active_conv
# # # # #         if cid and cid in convs:
# # # # #             del st.session_state.conversations[cid]
# # # # #             if MONGO_OK:
# # # # #                 try:
# # # # #                     db.chat_sessions.delete_one({"conv_id": cid})
# # # # #                 except Exception:
# # # # #                     pass
# # # # #             st.session_state.active_conv = None
# # # # #             st.rerun()

# # # # #     st.markdown("---")
# # # # #     st.markdown('<div class="section-lbl">Questions suggérées</div>', unsafe_allow_html=True)

# # # # #     suggestions = [
# # # # #         "Donne moi les stats globales",
# # # # #         "Pourquoi ya des commentaires négatifs ?",
# # # # #         "wach kayn bezzaf chikayat ?",
# # # # #         "كم عدد التعليقات السلبية؟",
# # # # #         "Analyse les tendances",
# # # # #         "Exemples de commentaires négatifs",
# # # # #         "ما هي أكثر المشاكل شيوعاً؟",
# # # # #         "Compare positif vs négatif",
# # # # #     ]
# # # # #     for sug in suggestions:
# # # # #         if st.button(sug, key=f"sug_{sug}"):
# # # # #             st.session_state["pending_question"] = sug
# # # # #             st.rerun()

# # # # #     # Export
# # # # #     active_msgs = convs.get(st.session_state.active_conv, {}).get("messages", [])
# # # # #     if active_msgs:
# # # # #         st.markdown("---")
# # # # #         st.download_button(
# # # # #             "⬇️ Exporter JSON",
# # # # #             data=json.dumps(active_msgs, ensure_ascii=False, indent=2, default=str),
# # # # #             file_name=f"conv_{st.session_state.active_conv}.json",
# # # # #             mime="application/json",
# # # # #             use_container_width=True,
# # # # #         )

# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # # ZONE PRINCIPALE — CHAT
# # # # # # ──────────────────────────────────────────────────────────────────
# # # # # active_cid  = st.session_state.active_conv
# # # # # active_conv = st.session_state.conversations.get(active_cid, {})
# # # # # messages    = active_conv.get("messages", [])

# # # # # col_title, col_info = st.columns([4, 1])
# # # # # with col_title:
# # # # #     st.markdown(f"""
# # # # #     <div style="padding:.5rem 0 1rem;">
# # # # #         <div style="font-size:18px;font-weight:600;color:#e8e6e0;">
# # # # #             {active_conv.get('title','Conversation')}
# # # # #         </div>
# # # # #         <div style="font-size:11px;color:#4b5563;font-family:'IBM Plex Mono',monospace;">
# # # # #             {len(messages)} messages · ⚡ {GROQ_MODEL} ·
# # # # #             {'🟢 MongoDB' if MONGO_OK else '🔴 MongoDB'} ·
# # # # #             {'🟢 Groq' if GROQ_OK else '🔴 Groq'}
# # # # #         </div>
# # # # #     </div>
# # # # #     """, unsafe_allow_html=True)

# # # # # # ── Affichage messages ────────────────────────────────────────────
# # # # # if not messages:
# # # # #     st.markdown("""
# # # # #     <div style="text-align:center;padding:4rem 2rem;color:#374151;">
# # # # #         <div style="font-size:48px;margin-bottom:1rem;">⚡</div>
# # # # #         <div style="font-size:18px;font-weight:600;color:#6b7280;margin-bottom:.5rem;">
# # # # #             Comment puis-je vous aider ?
# # # # #         </div>
# # # # #         <div style="font-size:13px;color:#374151;font-family:'IBM Plex Mono',monospace;">
# # # # #             Parlez en français · darija · عربي · réponse en 1-2 sec
# # # # #         </div>
# # # # #     </div>
# # # # #     """, unsafe_allow_html=True)
# # # # # else:
# # # # #     for msg in messages:
# # # # #         role    = msg["role"]
# # # # #         content = msg["content"]
# # # # #         lang    = msg.get("lang", "fr")
# # # # #         ts      = msg.get("timestamp", "")[:16]
# # # # #         badge   = LANG_BADGE.get(lang, "")

# # # # #         if role == "user":
# # # # #             safe = content.replace("<","&lt;").replace(">","&gt;")
# # # # #             st.markdown(f"""
# # # # #             <div class="msg-user"><div class="bubble">{safe}</div></div>
# # # # #             <div class="msg-meta" style="text-align:right;padding-right:4px;">
# # # # #                 {badge} {ts}
# # # # #             </div>
# # # # #             """, unsafe_allow_html=True)
# # # # #         else:
# # # # #             st.markdown(f"""
# # # # #             <div class="msg-bot">
# # # # #                 <div class="bot-icon">⚡</div>
# # # # #                 <div>
# # # # #                     <div class="bubble">{render_md(content)}</div>
# # # # #                     <div class="msg-meta">{ts}</div>
# # # # #                 </div>
# # # # #             </div>
# # # # #             """, unsafe_allow_html=True)

# # # # # # ── Input ─────────────────────────────────────────────────────────
# # # # # pending  = st.session_state.pop("pending_question", None)
# # # # # question = st.chat_input("Posez votre question en français, darija ou arabe…") or pending

# # # # # if question:
# # # # #     # Vérifier d'abord par Group_ID
# # # # #     rep_directe = repondre_question_specifique(question)
    
# # # # #     # Si pas trouvé, vérifier par date
# # # # #     if not rep_directe:
# # # # #         rep_directe = repondre_question_par_date(question)
# # # # #     ts_now      = datetime.now().strftime("%Y-%m-%d %H:%M")
# # # # #     lang_det    = detecter_langue(question)

# # # # #     if rep_directe:
# # # # #         messages.append({"role":"user","content":question,"lang":lang_det,"timestamp":ts_now})
# # # # #         messages.append({"role":"assistant","content":rep_directe,"lang":"fr","timestamp":ts_now})
# # # # #         sauvegarder_conv(active_cid, messages)
# # # # #         st.rerun()
# # # # #     else:
# # # # #         messages.append({"role":"user","content":question,"lang":lang_det,"timestamp":ts_now})
# # # # #         if len(messages) == 1:
# # # # #             titre = question[:40] + ("…" if len(question) > 40 else "")
# # # # #             st.session_state.conversations[active_cid]["title"] = titre
# # # # #             if MONGO_OK:
# # # # #                 try:
# # # # #                     db.chat_sessions.update_one(
# # # # #                         {"conv_id": active_cid}, {"$set": {"title": titre}}
# # # # #                     )
# # # # #                 except Exception:
# # # # #                     pass
# # # # #         sauvegarder_conv(active_cid, messages)
# # # # #         st.rerun()

# # # # # # ── Génération réponse ────────────────────────────────────────────
# # # # # if messages and messages[-1]["role"] == "user":
# # # # #     st.session_state.stop_generation = False

# # # # #     if st.button("⏹️ Stop", key="stop_btn"):
# # # # #         st.session_state.stop_generation = True

# # # # #     if not GROQ_OK:
# # # # #         st.error("""
# # # # #         ⚠️ Clé Groq invalide ou manquante !
# # # # #         1. Va sur https://console.groq.com
# # # # #         2. Crée une clé API gratuite
# # # # #         3. Ajoute dans docker-compose : GROQ_API_KEY=gsk_xxx...
# # # # #         """)
# # # # #     else:
# # # # #         placeholder = st.empty()
# # # # #         placeholder.markdown("""
# # # # #         <div class="msg-bot">
# # # # #             <div class="bot-icon">⚡</div>
# # # # #             <div class="bubble">
# # # # #                 <div class="typing"><span></span><span></span><span></span></div>
# # # # #             </div>
# # # # #         </div>
# # # # #         """, unsafe_allow_html=True)

# # # # #         full_response = ""
# # # # #         langue_user   = messages[-1].get("lang", "fr")

# # # # #         for delta in appeler_groq_stream(messages[:-1], messages[-1]["content"], langue_user):
# # # # #             full_response += delta
# # # # #             placeholder.markdown(f"""
# # # # #             <div class="msg-bot">
# # # # #                 <div class="bot-icon">⚡</div>
# # # # #                 <div class="bubble">{render_md(full_response)}▌</div>
# # # # #             </div>
# # # # #             """, unsafe_allow_html=True)

# # # # #         placeholder.empty()
# # # # #         messages.append({
# # # # #             "role":      "assistant",
# # # # #             "content":   full_response,
# # # # #             "lang":      langue_user,
# # # # #             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
# # # # #         })
# # # # #         sauvegarder_conv(active_cid, messages)
# # # # #         st.rerun()

# # # # #!/usr/bin/env python3
# # # # # -*- coding: utf-8 -*-
# # # # """
# # # # chatbot_groq.py — ChatBot Algérien avec GROQ (ultra rapide) + MongoDB
# # # # Conversation naturelle en français, darija, arabe
# # # # Analyse intelligente des commentaires télécom
# # # # """

# # # # import streamlit as st
# # # # from groq import Groq
# # # # from pymongo import MongoClient
# # # # from datetime import datetime
# # # # import re, json, os, uuid
# # # # from langdetect import detect, LangDetectException
# # # # from streamlit_mic_recorder import mic_recorder
# # # # import speech_recognition as sr

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # CONFIG
# # # # # ──────────────────────────────────────────────────────────────────
# # # # GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
# # # # GROQ_MODEL   = "llama-3.1-8b-instant"

# # # # MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018/")
# # # # DB_NAME   = "telecom_algerie"

# # # # groq_client = Groq(api_key=GROQ_API_KEY)


# # # # # Importer les modules
# # # # import sys
# # # # sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# # # # from modules.graphiques import *
# # # # from modules.export import *
# # # # from modules.statistiques import *
# # # # from modules.analyse_textuelle import *

# # # # # Dans la fonction de traitement des questions
# # # # def appeler_groq_stream(messages_hist: list, question: str, langue: str):
# # # #     q = question.lower()
    
# # # #     # Graphiques
# # # #     if "graphique évolution" in q or "courbe" in q or "tendance graphique" in q:
# # # #         fig = graphique_evolution_sentiments()
# # # #         if fig:
# # # #             st.plotly_chart(fig, use_container_width=True)
# # # #             return "📊 Voici le graphique d'évolution des sentiments."
# # # #         return "❌ Pas assez de données."
    
# # # #     if "camembert" in q or "répartition" in q:
# # # #         fig = graphique_repartition_sentiments()
# # # #         if fig:
# # # #             st.plotly_chart(fig, use_container_width=True)
# # # #             return "🥧 Voici la répartition des sentiments."
    
# # # #     if "top mots" in q or "nuage" in q:
# # # #         if "nuage" in q:
# # # #             nuage = generer_nuage_mots()
# # # #             if nuage:
# # # #                 st.markdown(nuage, unsafe_allow_html=True)
# # # #                 return "☁️ Voici le nuage de mots des commentaires négatifs."
# # # #         else:
# # # #             fig = graphique_top_mots()
# # # #             if fig:
# # # #                 st.plotly_chart(fig, use_container_width=True)
# # # #                 return "📊 Voici les mots les plus fréquents."
    
# # # #     # Export
# # # #     if "exporte" in q or "télécharge" in q:
# # # #         if "csv" in q:
# # # #             lien = exporter_csv()
# # # #             return lien
# # # #         elif "excel" in q:
# # # #             lien = exporter_excel()
# # # #             return lien
# # # #         elif "json" in q:
# # # #             lien = exporter_json()
# # # #             return lien
    
# # # #     # Tableau stats
# # # #     if "tableau" in q or "statistiques tableau" in q:
# # # #         tableau = generer_tableau_html()
# # # #         st.markdown(tableau, unsafe_allow_html=True)
# # # #         return "📋 Voici le tableau des statistiques."


# # # # # ──────────────────────────────────────────────────────────────────
# # # # # PAGE CONFIG
# # # # # ──────────────────────────────────────────────────────────────────
# # # # st.set_page_config(
# # # #     page_title="ChatBot Algérien — Groq ⚡",
# # # #     page_icon="🤖",
# # # #     layout="wide",
# # # #     initial_sidebar_state="expanded",
# # # # )

# # # # if "stop_generation" not in st.session_state:
# # # #     st.session_state.stop_generation = False

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # STYLE (garder ton CSS existant ici)
# # # # # ──────────────────────────────────────────────────────────────────
# # # # st.markdown("""
# # # # <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap" rel="stylesheet">
# # # # <style>
# # # # html,body,[class*="css"]{
# # # #     font-family:'Syne',sans-serif!important;
# # # #     background:#0a0b0f!important;
# # # #     color:#e8e6e0!important;
# # # # }
# # # # [data-testid="stAppViewContainer"]{background:#0a0b0f!important;}
# # # # [data-testid="stSidebar"]{
# # # #     background:#0d0e14!important;
# # # #     border-right:1px solid #1a1d2e!important;
# # # # }
# # # # .msg-user{display:flex;justify-content:flex-end;margin:12px 0;}
# # # # .msg-user .bubble{
# # # #     background:#1a2540;border:1px solid #2a3560;
# # # #     border-radius:18px 18px 4px 18px;
# # # #     padding:12px 18px;max-width:70%;
# # # #     font-size:15px;line-height:1.65;color:#c8d3f5;
# # # # }
# # # # .msg-bot{display:flex;gap:12px;margin:12px 0;align-items:flex-start;}
# # # # .bot-icon{
# # # #     width:36px;height:36px;min-width:36px;
# # # #     background:linear-gradient(135deg,#f97316,#eab308);
# # # #     border-radius:10px;display:flex;
# # # #     align-items:center;justify-content:center;
# # # #     font-size:18px;margin-top:2px;
# # # # }
# # # # .msg-bot .bubble{
# # # #     background:#0f1117;border:1px solid #1e2130;
# # # #     border-radius:4px 18px 18px 18px;
# # # #     padding:14px 18px;max-width:75%;
# # # #     font-size:15px;line-height:1.75;color:#e8e6e0;
# # # # }
# # # # .msg-bot .bubble strong{color:#f97316;}
# # # # .msg-bot .bubble code{
# # # #     background:#1a1d2e;border:1px solid #2a2d3e;
# # # #     border-radius:4px;padding:1px 6px;
# # # #     font-family:'IBM Plex Mono',monospace;
# # # #     font-size:13px;color:#fbbf24;
# # # # }
# # # # .msg-meta{
# # # #     font-size:11px;color:#374151;
# # # #     font-family:'IBM Plex Mono',monospace;
# # # #     margin-top:5px;padding-left:48px;
# # # # }
# # # # .stButton>button{
# # # #     background:transparent!important;
# # # #     border:1px solid #1e2130!important;
# # # #     color:#9ca3af!important;border-radius:8px!important;
# # # #     font-family:'IBM Plex Mono',monospace!important;
# # # #     font-size:12px!important;width:100%;
# # # #     text-align:left!important;padding:8px 12px!important;
# # # #     transition:all .2s!important;
# # # # }
# # # # .stButton>button:hover{
# # # #     border-color:#f97316!important;
# # # #     color:#f97316!important;
# # # #     background:rgba(249,115,22,.06)!important;
# # # # }
# # # # .badge{
# # # #     display:inline-block;padding:1px 8px;
# # # #     border-radius:20px;font-size:10px;font-weight:600;
# # # #     font-family:'IBM Plex Mono',monospace;
# # # # }
# # # # .b-fr{background:rgba(59,130,246,.15);color:#60a5fa;border:1px solid rgba(59,130,246,.3);}
# # # # .b-ar{background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.3);}
# # # # .b-dz{background:rgba(249,115,22,.15);color:#fb923c;border:1px solid rgba(249,115,22,.3);}
# # # # .sidebar-title{font-size:20px;font-weight:700;color:#e8e6e0;}
# # # # .sidebar-sub{
# # # #     font-size:10px;color:#4b5563;
# # # #     font-family:'IBM Plex Mono',monospace;
# # # #     letter-spacing:.08em;text-transform:uppercase;
# # # #     margin-bottom:1.5rem;
# # # # }
# # # # .accent{color:#f97316;}
# # # # .section-lbl{
# # # #     font-size:10px;color:#374151;
# # # #     font-family:'IBM Plex Mono',monospace;
# # # #     letter-spacing:.1em;text-transform:uppercase;
# # # #     padding:1rem 0 .4rem;
# # # # }
# # # # .typing{display:flex;gap:5px;align-items:center;padding:8px 0;}
# # # # .typing span{
# # # #     width:7px;height:7px;border-radius:50%;
# # # #     background:#f97316;display:inline-block;
# # # #     animation:bounce 1.2s infinite;
# # # # }
# # # # .typing span:nth-child(2){animation-delay:.2s;}
# # # # .typing span:nth-child(3){animation-delay:.4s;}
# # # # @keyframes bounce{
# # # #     0%,60%,100%{transform:translateY(0);opacity:.4;}
# # # #     30%{transform:translateY(-6px);opacity:1;}
# # # # }
# # # # .groq-badge{
# # # #     display:inline-block;
# # # #     background:linear-gradient(135deg,#f97316,#eab308);
# # # #     color:#000;font-size:10px;font-weight:700;
# # # #     padding:2px 8px;border-radius:20px;
# # # #     font-family:'IBM Plex Mono',monospace;
# # # #     margin-left:6px;
# # # # }
# # # # hr{border-color:#1a1d2e!important;margin:.75rem 0!important;}
# # # # ::-webkit-scrollbar{width:4px;}
# # # # ::-webkit-scrollbar-track{background:#0a0b0f;}
# # # # ::-webkit-scrollbar-thumb{background:#1e2130;border-radius:10px;}
# # # # </style>
# # # # """, unsafe_allow_html=True)

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # MONGODB
# # # # # ──────────────────────────────────────────────────────────────────
# # # # @st.cache_resource(ttl=0)
# # # # def get_db():
# # # #     try:
# # # #         c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
# # # #         c.admin.command("ping")
# # # #         return c[DB_NAME]
# # # #     except Exception:
# # # #         return None

# # # # db = get_db()
# # # # MONGO_OK = db is not None

# # # # def check_groq() -> bool:
# # # #     try:
# # # #         groq_client.models.list()
# # # #         return True
# # # #     except Exception:
# # # #         return False

# # # # GROQ_OK = check_groq()

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # CONTEXTE MONGODB
# # # # # ──────────────────────────────────────────────────────────────────
# # # # def get_context_mongo() -> str:
# # # #     if not MONGO_OK:
# # # #         return "Base MongoDB non disponible."
# # # #     try:
# # # #         lignes = []
# # # #         cols = db.list_collection_names()
# # # #         lignes.append(f"Collections disponibles : {', '.join(cols)}")

# # # #         if "dataset_unifie" in cols:
# # # #             col = db["dataset_unifie"]
# # # #             total = col.count_documents({})
# # # #             neg = col.count_documents({"label_final": "negatif"})
# # # #             pos = col.count_documents({"label_final": "positif"})
# # # #             neu = col.count_documents({"label_final": "neutre"})
# # # #             conflits = col.count_documents({"conflit": True})
# # # #             lignes.append(f"dataset_unifie : {total} commentaires")
# # # #             lignes.append(f"Sentiments → négatif:{neg} | positif:{pos} | neutre:{neu}")
# # # #             lignes.append(f"Conflits d'annotation : {conflits}")

# # # #         return "\n".join(lignes)
# # # #     except Exception as e:
# # # #         return f"Erreur MongoDB : {e}"

# # # # def repondre_question_specifique(question: str):
# # # #     """Recherche directe par Group_ID sans passer par le LLM."""
# # # #     match = re.search(r'groupe[ _]?(\d+)', question, re.IGNORECASE)
# # # #     if match and MONGO_OK:
# # # #         group_id = f"groupe_{int(match.group(1)):04d}"
# # # #         try:
# # # #             doc = db["dataset_unifie"].find_one({"Group_ID": group_id})
# # # #             if doc:
# # # #                 texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# # # #                 label = doc.get("label_final", "?")
# # # #                 return f"📝 **{group_id}** :\n> {texte}\n\n🏷️ Sentiment : **{label}**"
# # # #             return f"❌ Aucun commentaire trouvé pour **{group_id}**."
# # # #         except Exception as e:
# # # #             return f"Erreur : {e}"
# # # #     return None

# # # # def repondre_question_par_date(question: str) -> str or None:
# # # #     """Recherche les commentaires par date (format DD/MM/YYYY)"""
# # # #     match = re.search(r'(\d{2}/\d{2}/\d{4})', question)
# # # #     if match and MONGO_OK:
# # # #         date_cherchee = match.group(1)
# # # #         try:
# # # #             col = db["dataset_unifie"]
# # # #             docs = list(col.find({"dates": {"$regex": date_cherchee}}).limit(5))
# # # #             if docs:
# # # #                 resultat = f"📝 **Commentaires du {date_cherchee}** :\n\n"
# # # #                 for i, doc in enumerate(docs, 1):
# # # #                     texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# # # #                     label = doc.get("label_final", "?")
# # # #                     resultat += f"{i}. {texte}\n   🏷️ Sentiment : {label}\n\n"
# # # #                 return resultat
# # # #             else:
# # # #                 return f"❌ Aucun commentaire trouvé pour la date **{date_cherchee}**."
# # # #         except Exception as e:
# # # #             return f"Erreur recherche : {e}"
# # # #     return None

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # DÉTECTION LANGUE
# # # # # ──────────────────────────────────────────────────────────────────
# # # # def detecter_langue(texte: str) -> str:
# # # #     if re.search(r'[\u0600-\u06FF]', texte):
# # # #         return "ar"
# # # #     if re.search(r'\b[23789]\b|3likom|wach|chnou|bezzaf|machi|kayn|ndir|sahbi|slm|kifak|rahi|mazal', texte.lower()):
# # # #         return "dz"
# # # #     try:
# # # #         return "fr" if detect(texte) == "fr" else "dz"
# # # #     except LangDetectException:
# # # #         return "fr"

# # # # LANG_MAPPING = {"fr": "français", "ar": "arabe", "dz": "darija algérienne"}
# # # # LANG_BADGE = {
# # # #     "fr": '<span class="badge b-fr">FR</span>',
# # # #     "ar": '<span class="badge b-ar">عر</span>',
# # # #     "dz": '<span class="badge b-dz">DZ</span>',
# # # # }

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # PROMPT SYSTÈME
# # # # # ──────────────────────────────────────────────────────────────────
# # # # def build_system_prompt(langue: str) -> str:
# # # #     ctx = get_context_mongo()
# # # #     return f"""Tu es un assistant analytique intelligent spécialisé dans l'analyse 
# # # # de commentaires clients des opérateurs télécom algériens (Djezzy, Mobilis, Ooredoo).
# # # # Tu comprends et réponds parfaitement en français, en darija algérienne et en arabe standard.

# # # # === DONNÉES RÉELLES DE LA BASE MONGODB ===
# # # # {ctx}
# # # # ==========================================

# # # # RÈGLES IMPORTANTES :
# # # # - Réponds OBLIGATOIREMENT en : {LANG_MAPPING.get(langue, 'français')}
# # # # - Si l'utilisateur parle darija → réponds en darija naturelle algérienne
# # # # - Base tes analyses UNIQUEMENT sur les vraies données ci-dessus
# # # # - Donne des chiffres précis, ne fabrique rien
# # # # - Si une information manque, dis-le clairement
# # # # - Formate tes réponses avec des listes et titres quand pertinent
# # # # """

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # APPEL GROQ — STREAMING ⚡
# # # # # ──────────────────────────────────────────────────────────────────
# # # # def appeler_groq_stream(messages_hist: list, question: str, langue: str):
# # # #     """Appelle Groq en streaming — ultra rapide !"""
# # # #     msgs = [{"role": "system", "content": build_system_prompt(langue)}]

# # # #     for m in messages_hist[-20:]:
# # # #         msgs.append({"role": m["role"], "content": m["content"]})
# # # #     msgs.append({"role": "user", "content": question})

# # # #     try:
# # # #         stream = groq_client.chat.completions.create(
# # # #             model=GROQ_MODEL,
# # # #             messages=msgs,
# # # #             stream=True,
# # # #             temperature=0.7,
# # # #             max_tokens=256,
# # # #             top_p=0.9,
# # # #         )
# # # #         for chunk in stream:
# # # #             if st.session_state.stop_generation:
# # # #                 st.session_state.stop_generation = False
# # # #                 yield "\n\n⏹️ Génération arrêtée."
# # # #                 break
# # # #             delta = chunk.choices[0].delta.content
# # # #             if delta:
# # # #                 yield delta
# # # #     except Exception as e:
# # # #         yield f"\n⚠️ Erreur Groq : {e}\n"

# # # # def render_md(txt: str) -> str:
# # # #     txt = txt.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
# # # #     txt = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', txt)
# # # #     txt = re.sub(r'\*(.*?)\*', r'<em>\1</em>', txt)
# # # #     txt = re.sub(r'`(.*?)`', r'<code>\1</code>', txt)
# # # #     txt = txt.replace("\n\n","<br><br>").replace("\n","<br>")
# # # #     txt = re.sub(r'<br>[-•] ','<br>• ', txt)
# # # #     return txt

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # SAUVEGARDE CONVERSATIONS
# # # # # ──────────────────────────────────────────────────────────────────
# # # # def charger_convs_mongo() -> dict:
# # # #     if not MONGO_OK:
# # # #         return {}
# # # #     try:
# # # #         docs = list(db.chat_sessions.find({},{"_id":0}).sort("updated_at",-1).limit(30))
# # # #         return {d["conv_id"]: {
# # # #             "title": d.get("title","Conversation"),
# # # #             "messages": d.get("messages",[]),
# # # #             "created_at": str(d.get("created_at","")),
# # # #         } for d in docs}
# # # #     except Exception:
# # # #         return {}

# # # # def sauvegarder_conv(cid, messages):
# # # #     st.session_state.conversations[cid]["messages"] = messages
# # # #     if MONGO_OK:
# # # #         try:
# # # #             db.chat_sessions.update_one(
# # # #                 {"conv_id": cid},
# # # #                 {"$set": {
# # # #                     "messages": messages,
# # # #                     "updated_at": datetime.now(),
# # # #                     "title": st.session_state.conversations[cid].get("title",""),
# # # #                 }},
# # # #                 upsert=True,
# # # #             )
# # # #         except Exception:
# # # #             pass

# # # # def nouvelle_conversation():
# # # #     cid = str(uuid.uuid4())[:8]
# # # #     st.session_state.conversations[cid] = {
# # # #         "title": "Nouvelle conversation",
# # # #         "messages": [],
# # # #         "created_at": datetime.now().isoformat(),
# # # #     }
# # # #     st.session_state.active_conv = cid
# # # #     if MONGO_OK:
# # # #         try:
# # # #             db.chat_sessions.insert_one({
# # # #                 "conv_id": cid,
# # # #                 "title": "Nouvelle conversation",
# # # #                 "messages": [],
# # # #                 "created_at": datetime.now(),
# # # #                 "updated_at": datetime.now(),
# # # #             })
# # # #         except Exception:
# # # #             pass
# # # #     return cid

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # SESSION STATE
# # # # # ──────────────────────────────────────────────────────────────────
# # # # if "conversations" not in st.session_state:
# # # #     st.session_state.conversations = charger_convs_mongo()
# # # # if "active_conv" not in st.session_state:
# # # #     st.session_state.active_conv = None

# # # # if not st.session_state.active_conv or st.session_state.active_conv not in st.session_state.conversations:
# # # #     if st.session_state.conversations:
# # # #         st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
# # # #     else:
# # # #         nouvelle_conversation()

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # SIDEBAR
# # # # # ──────────────────────────────────────────────────────────────────
# # # # with st.sidebar:
# # # #     st.markdown(f"""
# # # #     <div class="sidebar-title">Chat<span class="accent">Bot</span><span class="groq-badge">⚡ GROQ</span></div>
# # # #     <div class="sidebar-sub">{GROQ_MODEL} · Télécom DZ</div>
# # # #     """, unsafe_allow_html=True)

# # # #     if st.button("＋ Nouvelle conversation"):
# # # #         nouvelle_conversation()
# # # #         st.rerun()

# # # #     st.markdown("---")

# # # #     col_m, col_g = st.columns(2)
# # # #     with col_m:
# # # #         if MONGO_OK:
# # # #             st.success("MongoDB ✓")
# # # #         else:
# # # #             st.error("MongoDB ✗")
# # # #     with col_g:
# # # #         if GROQ_OK:
# # # #             st.success("Groq ✓")
# # # #         else:
# # # #             st.error("Groq ✗")

# # # #     st.markdown("---")
# # # #     st.markdown('<div class="section-lbl">Conversations récentes</div>', unsafe_allow_html=True)

# # # #     convs = st.session_state.conversations
# # # #     for cid, conv in list(convs.items()):
# # # #         title = conv.get("title","Conversation")[:35]
# # # #         is_active = cid == st.session_state.active_conv
# # # #         label = f"{'▶ ' if is_active else ''}{title}"
# # # #         if st.button(label, key=f"btn_conv_{cid}"):
# # # #             st.session_state.active_conv = cid
# # # #             st.rerun()

# # # #     st.markdown("---")

# # # #     if st.button("🗑️ Supprimer conversation"):
# # # #         cid = st.session_state.active_conv
# # # #         if cid and cid in convs:
# # # #             del st.session_state.conversations[cid]
# # # #             if MONGO_OK:
# # # #                 try:
# # # #                     db.chat_sessions.delete_one({"conv_id": cid})
# # # #                 except Exception:
# # # #                     pass
# # # #             st.session_state.active_conv = None
# # # #             st.rerun()

# # # #     st.markdown("---")
# # # #     st.markdown('<div class="section-lbl">Questions suggérées</div>', unsafe_allow_html=True)

# # # #     suggestions = [
# # # #         "Donne moi les stats globales",
# # # #         "Pourquoi ya des commentaires négatifs ?",
# # # #         "wach kayn bezzaf chikayat ?",
# # # #         "كم عدد التعليقات السلبية؟",
# # # #         "Analyse les tendances",
# # # #         "Exemples de commentaires négatifs",
# # # #         "ما هي أكثر المشاكل شيوعاً؟",
# # # #         "Compare positif vs négatif",
# # # #     ]
# # # #     for sug in suggestions:
# # # #         if st.button(sug, key=f"sug_{sug}"):
# # # #             st.session_state["pending_question"] = sug
# # # #             st.rerun()

# # # #     # Export
# # # #     active_msgs = convs.get(st.session_state.active_conv, {}).get("messages", [])
# # # #     if active_msgs:
# # # #         st.markdown("---")
# # # #         st.download_button(
# # # #             "⬇️ Exporter JSON",
# # # #             data=json.dumps(active_msgs, ensure_ascii=False, indent=2, default=str),
# # # #             file_name=f"conv_{st.session_state.active_conv}.json",
# # # #             mime="application/json",
# # # #             use_container_width=True,
# # # #         )

# # # # #     # RECONNAISSANCE VOCALE DANS LA SIDEBAR
# # # # #     st.markdown("---")
# # # # #     st.markdown('<div class="section-lbl">🎤 Saisie vocale</div>', unsafe_allow_html=True)
    
# # # # #     audio = mic_recorder(
# # # # #         start_prompt="Cliquez pour parler",
# # # # #         stop_prompt="Arrêter",
# # # # #         just_once=True,
# # # # #         use_container_width=True,
# # # # #         key="mic_recorder"
# # # # #     )
    
# # # # #  # Dans la partie reconnaissance vocale
# # # # # if audio and 'bytes' in audio:
# # # # #     try:
# # # # #         # Convertir en WAV
# # # # #         audio_bytes = io.BytesIO(audio['bytes'])
# # # # #         sound = AudioSegment.from_file(audio_bytes)
        
# # # # #         # Exporter en WAV
# # # # #         sound.export("temp_audio.wav", format="wav")
        
# # # # #         # Transcrire
# # # # #         recognizer = sr.Recognizer()
# # # # #         with sr.AudioFile("temp_audio.wav") as source:
# # # # #             audio_data = recognizer.record(source)
# # # # #             texte = recognizer.recognize_google(audio_data, language="fr-FR")
# # # # #             st.success(f"📝 {texte}")
# # # # #             st.session_state["pending_question"] = texte
# # # # #             st.rerun()
# # # # #     except Exception as e:
# # # # #         st.error(f"Erreur : {e}")

# # # # # ──────────────────────────────────────────────────────────────────
# # # # # ZONE PRINCIPALE — CHAT
# # # # # ──────────────────────────────────────────────────────────────────
# # # # active_cid = st.session_state.active_conv
# # # # active_conv = st.session_state.conversations.get(active_cid, {})
# # # # messages = active_conv.get("messages", [])

# # # # col_title, col_info = st.columns([4, 1])
# # # # with col_title:
# # # #     st.markdown(f"""
# # # #     <div style="padding:.5rem 0 1rem;">
# # # #         <div style="font-size:18px;font-weight:600;color:#e8e6e0;">
# # # #             {active_conv.get('title','Conversation')}
# # # #         </div>
# # # #         <div style="font-size:11px;color:#4b5563;font-family:'IBM Plex Mono',monospace;">
# # # #             {len(messages)} messages · ⚡ {GROQ_MODEL} ·
# # # #             {'🟢 MongoDB' if MONGO_OK else '🔴 MongoDB'} ·
# # # #             {'🟢 Groq' if GROQ_OK else '🔴 Groq'}
# # # #         </div>
# # # #     </div>
# # # #     """, unsafe_allow_html=True)

# # # # # Affichage des messages
# # # # if not messages:
# # # #     st.markdown("""
# # # #     <div style="text-align:center;padding:4rem 2rem;color:#374151;">
# # # #         <div style="font-size:48px;margin-bottom:1rem;">⚡</div>
# # # #         <div style="font-size:18px;font-weight:600;color:#6b7280;margin-bottom:.5rem;">
# # # #             Comment puis-je vous aider ?
# # # #         </div>
# # # #         <div style="font-size:13px;color:#374151;font-family:'IBM Plex Mono',monospace;">
# # # #             Parlez en français · darija · عربي · réponse en 1-2 sec
# # # #         </div>
# # # #     </div>
# # # #     """, unsafe_allow_html=True)
# # # # else:
# # # #     for msg in messages:
# # # #         role = msg["role"]
# # # #         content = msg["content"]
# # # #         lang = msg.get("lang", "fr")
# # # #         ts = msg.get("timestamp", "")[:16]
# # # #         badge = LANG_BADGE.get(lang, "")

# # # #         if role == "user":
# # # #             safe = content.replace("<","&lt;").replace(">","&gt;")
# # # #             st.markdown(f"""
# # # #             <div class="msg-user"><div class="bubble">{safe}</div></div>
# # # #             <div class="msg-meta" style="text-align:right;padding-right:4px;">
# # # #                 {badge} {ts}
# # # #             </div>
# # # #             """, unsafe_allow_html=True)
# # # #         else:
# # # #             st.markdown(f"""
# # # #             <div class="msg-bot">
# # # #                 <div class="bot-icon">⚡</div>
# # # #                 <div>
# # # #                     <div class="bubble">{render_md(content)}</div>
# # # #                     <div class="msg-meta">{ts}</div>
# # # #                 </div>
# # # #             </div>
# # # #             """, unsafe_allow_html=True)

# # # # # ── Input utilisateur ─────────────────────────────────────────────
# # # # # pending = st.session_state.pop("pending_question", None)

# # # # # question = st.chat_input("Posez votre question en français, darija ou arabe…") or pending

# # # # # if question:
# # # # #     # Vérifier les réponses directes d'abord
# # # # #     rep_directe = repondre_question_specifique(question)
# # # # #     if not rep_directe:
# # # # #         rep_directe = repondre_question_par_date(question)
    
# # # # #     ts_now = datetime.now().strftime("%Y-%m-%d %H:%M")
# # # # #     lang_det = detecter_langue(question)
    
# # # # #     if rep_directe:
# # # # #         messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
# # # # #         messages.append({"role": "assistant", "content": rep_directe, "lang": "fr", "timestamp": ts_now})
# # # # #         sauvegarder_conv(active_cid, messages)
# # # # #         st.rerun()
# # # # #     else:
# # # # #         messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
# # # # #         if len(messages) == 1:
# # # # #             titre = question[:40] + ("…" if len(question) > 40 else "")
# # # # #             st.session_state.conversations[active_cid]["title"] = titre
# # # # #             if MONGO_OK:
# # # # #                 try:
# # # # #                     db.chat_sessions.update_one({"conv_id": active_cid}, {"$set": {"title": titre}})
# # # # #                 except Exception:
# # # # #                     pass
# # # # #         sauvegarder_conv(active_cid, messages)
# # # # #         st.rerun()
# # # # # ── Input ─────────────────────────────────────────────────────────
# # # # # ── Input utilisateur ─────────────────────────────────────────────
# # # # st.markdown("""
# # # # <style>
# # # # .input-bar {
# # # #     position: fixed;
# # # #     bottom: 0; left: 0; right: 0;
# # # #     background: #0d0e14;
# # # #     border-top: 1px solid #1a1d2e;
# # # #     padding: 14px 24px;
# # # #     z-index: 999;
# # # #     display: flex;
# # # #     align-items: center;
# # # #     gap: 10px;
# # # # }
# # # # .input-bar textarea {
# # # #     flex: 1;
# # # #     background: #0f1117 !important;
# # # #     border: 1px solid #1e2130 !important;
# # # #     border-radius: 12px !important;
# # # #     color: #e8e6e0 !important;
# # # #     padding: 12px 16px !important;
# # # #     font-size: 14px !important;
# # # #     font-family: 'Syne', sans-serif !important;
# # # #     resize: none !important;
# # # #     outline: none !important;
# # # #     min-height: 48px;
# # # #     max-height: 120px;
# # # # }
# # # # .input-bar textarea:focus {
# # # #     border-color: #f97316 !important;
# # # # }
# # # # .icon-btn {
# # # #     background: #0f1117;
# # # #     border: 1px solid #1e2130;
# # # #     border-radius: 10px;
# # # #     width: 44px; height: 44px;
# # # #     display: flex; align-items: center; justify-content: center;
# # # #     font-size: 18px; cursor: pointer;
# # # #     color: #9ca3af;
# # # #     transition: all .2s;
# # # #     flex-shrink: 0;
# # # # }
# # # # .icon-btn:hover {
# # # #     border-color: #f97316;
# # # #     color: #f97316;
# # # #     background: rgba(249,115,22,.08);
# # # # }
# # # # .send-btn {
# # # #     background: linear-gradient(135deg, #f97316, #eab308);
# # # #     border: none;
# # # #     border-radius: 10px;
# # # #     width: 44px; height: 44px;
# # # #     display: flex; align-items: center; justify-content: center;
# # # #     font-size: 18px; cursor: pointer;
# # # #     color: #000;
# # # #     flex-shrink: 0;
# # # #     transition: opacity .2s;
# # # # }
# # # # .send-btn:hover { opacity: 0.85; }
# # # # /* espace en bas pour ne pas cacher les messages */
# # # # .main > div { padding-bottom: 100px; }
# # # # </style>
# # # # """, unsafe_allow_html=True)

# # # # pending = st.session_state.pop("pending_question", None)

# # # # # Barre d'input en bas
# # # # col_mic, col_file, col_input, col_send = st.columns([0.6, 0.6, 8, 0.8])

# # # # with col_mic:
# # # #     audio = mic_recorder(
# # # #         start_prompt="🎤",
# # # #         stop_prompt="⏹️",
# # # #         just_once=True,
# # # #         use_container_width=True,
# # # #         key="mic_recorder"
# # # #     )

# # # # with col_file:
# # # #     fichier = st.file_uploader("", type=["txt", "csv", "json", "pdf"], 
# # # #                                 label_visibility="collapsed",
# # # #                                 key="file_uploader")
# # # #     if fichier:
# # # #         contenu = fichier.read().decode("utf-8", errors="ignore")[:2000]
# # # #         st.session_state["pending_question"] = f"Analyse ce fichier ({fichier.name}) :\n\n{contenu}"
# # # #         st.rerun()

# # # # with col_input:
# # # #     question_input = st.text_area(
# # # #         "", 
# # # #         placeholder="Écris ton message en français, darija ou عربي…",
# # # #         label_visibility="collapsed",
# # # #         height=68,
# # # #         key="chat_input"
# # # #     )

# # # # with col_send:
# # # #     send = st.button("➤", use_container_width=True, key="send_btn")

# # # # # Traitement micro
# # # # if audio and 'bytes' in audio:
# # # #     try:
# # # #         import io
# # # #         from pydub import AudioSegment
# # # #         audio_bytes = io.BytesIO(audio['bytes'])
# # # #         sound = AudioSegment.from_file(audio_bytes)
# # # #         sound.export("temp_audio.wav", format="wav")
# # # #         recognizer = sr.Recognizer()
# # # #         with sr.AudioFile("temp_audio.wav") as source:
# # # #             audio_data = recognizer.record(source)
# # # #             texte = recognizer.recognize_google(audio_data, language="fr-FR")
# # # #             st.session_state["pending_question"] = texte
# # # #             st.rerun()
# # # #     except Exception as e:
# # # #         st.warning(f"Micro : {e}")

# # # # # Question finale
# # # # question = None
# # # # if send and question_input and question_input.strip():
# # # #     question = question_input.strip()
# # # # elif pending:
# # # #     question = pending

# # # # if question:
# # # #     rep_directe = repondre_question_specifique(question)
# # # #     if not rep_directe:
# # # #         rep_directe = repondre_question_par_date(question)

# # # #     ts_now = datetime.now().strftime("%Y-%m-%d %H:%M")
# # # #     lang_det = detecter_langue(question)

# # # #     if rep_directe:
# # # #         messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
# # # #         messages.append({"role": "assistant", "content": rep_directe, "lang": "fr", "timestamp": ts_now})
# # # #         sauvegarder_conv(active_cid, messages)
# # # #         st.rerun()
# # # #     else:
# # # #         messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
# # # #         if len(messages) == 1:
# # # #             titre = question[:40] + ("…" if len(question) > 40 else "")
# # # #             st.session_state.conversations[active_cid]["title"] = titre
# # # #             if MONGO_OK:
# # # #                 try:
# # # #                     db.chat_sessions.update_one({"conv_id": active_cid}, {"$set": {"title": titre}})
# # # #                 except Exception:
# # # #                     pass
# # # #         sauvegarder_conv(active_cid, messages)
# # # #         st.rerun()

# # # # # ── Génération réponse ────────────────────────────────────────────
# # # # if messages and messages[-1]["role"] == "user":
# # # #     st.session_state.stop_generation = False

# # # #     if not GROQ_OK:
# # # #         st.error("⚠️ Clé Groq invalide ou manquante !")
# # # #     else:
# # # #         placeholder = st.empty()
# # # #         placeholder.markdown("""
# # # #         <div class="msg-bot">
# # # #             <div class="bot-icon">⚡</div>
# # # #             <div class="bubble">
# # # #                 <div class="typing"><span></span><span></span><span></span></div>
# # # #             </div>
# # # #         </div>
# # # #         """, unsafe_allow_html=True)

# # # #         full_response = ""
# # # #         langue_user = messages[-1].get("lang", "fr")

# # # #         for delta in appeler_groq_stream(messages[:-1], messages[-1]["content"], langue_user):
# # # #             full_response += delta
# # # #             placeholder.markdown(f"""
# # # #             <div class="msg-bot">
# # # #                 <div class="bot-icon">⚡</div>
# # # #                 <div class="bubble">{render_md(full_response)}▌</div>
# # # #             </div>
# # # #             """, unsafe_allow_html=True)

# # # #         placeholder.empty()
# # # #         messages.append({
# # # #             "role": "assistant",
# # # #             "content": full_response,
# # # #             "lang": langue_user,
# # # #             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
# # # #         })
# # # #         sauvegarder_conv(active_cid, messages)
# # # #         st.rerun()
# # # # # ── Génération réponse ────────────────────────────────────────────
# # # # if messages and messages[-1]["role"] == "user":
# # # #     st.session_state.stop_generation = False

# # # #     if st.button("⏹️ Stop", key="stop_btn"):
# # # #         st.session_state.stop_generation = True

# # # #     if not GROQ_OK:
# # # #         st.error("""
# # # #         ⚠️ Clé Groq invalide ou manquante !
# # # #         1. Va sur https://console.groq.com
# # # #         2. Crée une clé API gratuite
# # # #         """)
# # # #     else:
# # # #         placeholder = st.empty()
# # # #         placeholder.markdown("""
# # # #         <div class="msg-bot">
# # # #             <div class="bot-icon">⚡</div>
# # # #             <div class="bubble">
# # # #                 <div class="typing"><span></span><span></span><span></span></div>
# # # #             </div>
# # # #         </div>
# # # #         """, unsafe_allow_html=True)

# # # #         full_response = ""
# # # #         langue_user = messages[-1].get("lang", "fr")

# # # #         for delta in appeler_groq_stream(messages[:-1], messages[-1]["content"], langue_user):
# # # #             full_response += delta
# # # #             placeholder.markdown(f"""
# # # #             <div class="msg-bot">
# # # #                 <div class="bot-icon">⚡</div>
# # # #                 <div class="bubble">{render_md(full_response)}▌</div>
# # # #             </div>
# # # #             """, unsafe_allow_html=True)

# # # #         placeholder.empty()
# # # #         messages.append({
# # # #             "role": "assistant",
# # # #             "content": full_response,
# # # #             "lang": langue_user,
# # # #             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
# # # #         })
# # # #         sauvegarder_conv(active_cid, messages)
# # # #         st.rerun()

# # # #!/usr/bin/env python3
# # # # -*- coding: utf-8 -*-
# # # """
# # # chatbot_groq.py — ChatBot Algérien avec GROQ (ultra rapide) + MongoDB
# # # Conversation naturelle en français, darija, arabe
# # # Analyse intelligente des commentaires télécom
# # # """

# # # import streamlit as st
# # # from groq import Groq
# # # from pymongo import MongoClient
# # # from datetime import datetime
# # # import re, json, os, uuid
# # # from langdetect import detect, LangDetectException
# # # from streamlit_mic_recorder import mic_recorder
# # # import speech_recognition as sr
# # # import io
# # # from pydub import AudioSegment
# # # import sys

# # # # ──────────────────────────────────────────────────────────────────
# # # # CONFIG
# # # # ──────────────────────────────────────────────────────────────────
# # # GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
# # # GROQ_MODEL = "llama-3.1-8b-instant"

# # # MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018/")
# # # DB_NAME = "telecom_algerie"

# # # groq_client = Groq(api_key=GROQ_API_KEY)

# # # # ──────────────────────────────────────────────────────────────────
# # # # IMPORT DES MODULES
# # # # ──────────────────────────────────────────────────────────────────
# # # # Ajouter le chemin parent pour importer les modules
# # # sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# # # from modules.graphiques import *
# # # from modules.export import *
# # # from modules.statistiques import *
# # # from modules.analyse_textuelle import *

# # # # ──────────────────────────────────────────────────────────────────
# # # # PAGE CONFIG
# # # # ──────────────────────────────────────────────────────────────────
# # # st.set_page_config(
# # #     page_title="ChatBot Algérien — Groq ⚡",
# # #     page_icon="🤖",
# # #     layout="wide",
# # #     initial_sidebar_state="expanded",
# # # )

# # # if "stop_generation" not in st.session_state:
# # #     st.session_state.stop_generation = False

# # # # ──────────────────────────────────────────────────────────────────
# # # # STYLE CSS
# # # # ──────────────────────────────────────────────────────────────────
# # # st.markdown("""
# # # <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap" rel="stylesheet">
# # # <style>
# # # html,body,[class*="css"]{
# # #     font-family:'Syne',sans-serif!important;
# # #     background:#0a0b0f!important;
# # #     color:#e8e6e0!important;
# # # }
# # # [data-testid="stAppViewContainer"]{background:#0a0b0f!important;}
# # # [data-testid="stSidebar"]{
# # #     background:#0d0e14!important;
# # #     border-right:1px solid #1a1d2e!important;
# # # }
# # # .msg-user{display:flex;justify-content:flex-end;margin:12px 0;}
# # # .msg-user .bubble{
# # #     background:#1a2540;border:1px solid #2a3560;
# # #     border-radius:18px 18px 4px 18px;
# # #     padding:12px 18px;max-width:70%;
# # #     font-size:15px;line-height:1.65;color:#c8d3f5;
# # # }
# # # .msg-bot{display:flex;gap:12px;margin:12px 0;align-items:flex-start;}
# # # .bot-icon{
# # #     width:36px;height:36px;min-width:36px;
# # #     background:linear-gradient(135deg,#f97316,#eab308);
# # #     border-radius:10px;display:flex;
# # #     align-items:center;justify-content:center;
# # #     font-size:18px;margin-top:2px;
# # # }
# # # .msg-bot .bubble{
# # #     background:#0f1117;border:1px solid #1e2130;
# # #     border-radius:4px 18px 18px 18px;
# # #     padding:14px 18px;max-width:75%;
# # #     font-size:15px;line-height:1.75;color:#e8e6e0;
# # # }
# # # .msg-bot .bubble strong{color:#f97316;}
# # # .msg-bot .bubble code{
# # #     background:#1a1d2e;border:1px solid #2a2d3e;
# # #     border-radius:4px;padding:1px 6px;
# # #     font-family:'IBM Plex Mono',monospace;
# # #     font-size:13px;color:#fbbf24;
# # # }
# # # .msg-meta{
# # #     font-size:11px;color:#374151;
# # #     font-family:'IBM Plex Mono',monospace;
# # #     margin-top:5px;padding-left:48px;
# # # }
# # # .stButton>button{
# # #     background:transparent!important;
# # #     border:1px solid #1e2130!important;
# # #     color:#9ca3af!important;border-radius:8px!important;
# # #     font-family:'IBM Plex Mono',monospace!important;
# # #     font-size:12px!important;width:100%;
# # #     text-align:left!important;padding:8px 12px!important;
# # #     transition:all .2s!important;
# # # }
# # # .stButton>button:hover{
# # #     border-color:#f97316!important;
# # #     color:#f97316!important;
# # #     background:rgba(249,115,22,.06)!important;
# # # }
# # # .badge{
# # #     display:inline-block;padding:1px 8px;
# # #     border-radius:20px;font-size:10px;font-weight:600;
# # #     font-family:'IBM Plex Mono',monospace;
# # # }
# # # .b-fr{background:rgba(59,130,246,.15);color:#60a5fa;border:1px solid rgba(59,130,246,.3);}
# # # .b-ar{background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.3);}
# # # .b-dz{background:rgba(249,115,22,.15);color:#fb923c;border:1px solid rgba(249,115,22,.3);}
# # # .sidebar-title{font-size:20px;font-weight:700;color:#e8e6e0;}
# # # .sidebar-sub{
# # #     font-size:10px;color:#4b5563;
# # #     font-family:'IBM Plex Mono',monospace;
# # #     letter-spacing:.08em;text-transform:uppercase;
# # #     margin-bottom:1.5rem;
# # # }
# # # .accent{color:#f97316;}
# # # .section-lbl{
# # #     font-size:10px;color:#374151;
# # #     font-family:'IBM Plex Mono',monospace;
# # #     letter-spacing:.1em;text-transform:uppercase;
# # #     padding:1rem 0 .4rem;
# # # }
# # # .typing{display:flex;gap:5px;align-items:center;padding:8px 0;}
# # # .typing span{
# # #     width:7px;height:7px;border-radius:50%;
# # #     background:#f97316;display:inline-block;
# # #     animation:bounce 1.2s infinite;
# # # }
# # # .typing span:nth-child(2){animation-delay:.2s;}
# # # .typing span:nth-child(3){animation-delay:.4s;}
# # # @keyframes bounce{
# # #     0%,60%,100%{transform:translateY(0);opacity:.4;}
# # #     30%{transform:translateY(-6px);opacity:1;}
# # # }
# # # .groq-badge{
# # #     display:inline-block;
# # #     background:linear-gradient(135deg,#f97316,#eab308);
# # #     color:#000;font-size:10px;font-weight:700;
# # #     padding:2px 8px;border-radius:20px;
# # #     font-family:'IBM Plex Mono',monospace;
# # #     margin-left:6px;
# # # }
# # # hr{border-color:#1a1d2e!important;margin:.75rem 0!important;}
# # # ::-webkit-scrollbar{width:4px;}
# # # ::-webkit-scrollbar-track{background:#0a0b0f;}
# # # ::-webkit-scrollbar-thumb{background:#1e2130;border-radius:10px;}
# # # .input-bar {
# # #     position: fixed;
# # #     bottom: 0; left: 0; right: 0;
# # #     background: #0d0e14;
# # #     border-top: 1px solid #1a1d2e;
# # #     padding: 14px 24px;
# # #     z-index: 999;
# # #     display: flex;
# # #     align-items: center;
# # #     gap: 10px;
# # # }
# # # .input-bar textarea {
# # #     flex: 1;
# # #     background: #0f1117 !important;
# # #     border: 1px solid #1e2130 !important;
# # #     border-radius: 12px !important;
# # #     color: #e8e6e0 !important;
# # #     padding: 12px 16px !important;
# # #     font-size: 14px !important;
# # #     font-family: 'Syne', sans-serif !important;
# # #     resize: none !important;
# # #     outline: none !important;
# # #     min-height: 48px;
# # #     max-height: 120px;
# # # }
# # # .input-bar textarea:focus {
# # #     border-color: #f97316 !important;
# # # }
# # # .icon-btn {
# # #     background: #0f1117;
# # #     border: 1px solid #1e2130;
# # #     border-radius: 10px;
# # #     width: 44px; height: 44px;
# # #     display: flex; align-items: center; justify-content: center;
# # #     font-size: 18px; cursor: pointer;
# # #     color: #9ca3af;
# # #     transition: all .2s;
# # #     flex-shrink: 0;
# # # }
# # # .icon-btn:hover {
# # #     border-color: #f97316;
# # #     color: #f97316;
# # #     background: rgba(249,115,22,.08);
# # # }
# # # .send-btn {
# # #     background: linear-gradient(135deg, #f97316, #eab308);
# # #     border: none;
# # #     border-radius: 10px;
# # #     width: 44px; height: 44px;
# # #     display: flex; align-items: center; justify-content: center;
# # #     font-size: 18px; cursor: pointer;
# # #     color: #000;
# # #     flex-shrink: 0;
# # #     transition: opacity .2s;
# # # }
# # # .send-btn:hover { opacity: 0.85; }
# # # .main > div { padding-bottom: 100px; }
# # # </style>
# # # """, unsafe_allow_html=True)

# # # # ──────────────────────────────────────────────────────────────────
# # # # MONGODB
# # # # ──────────────────────────────────────────────────────────────────
# # # @st.cache_resource(ttl=0)
# # # def get_db():
# # #     try:
# # #         c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
# # #         c.admin.command("ping")
# # #         return c[DB_NAME]
# # #     except Exception:
# # #         return None

# # # db = get_db()
# # # MONGO_OK = db is not None

# # # def check_groq() -> bool:
# # #     try:
# # #         groq_client.models.list()
# # #         return True
# # #     except Exception:
# # #         return False

# # # GROQ_OK = check_groq()

# # # # ──────────────────────────────────────────────────────────────────
# # # # CONTEXTE MONGODB
# # # # ──────────────────────────────────────────────────────────────────
# # # def get_context_mongo() -> str:
# # #     if not MONGO_OK:
# # #         return "Base MongoDB non disponible."
# # #     try:
# # #         lignes = []
# # #         cols = db.list_collection_names()
# # #         lignes.append(f"Collections disponibles : {', '.join(cols)}")

# # #         if "dataset_unifie" in cols:
# # #             col = db["dataset_unifie"]
# # #             total = col.count_documents({})
# # #             neg = col.count_documents({"label_final": "negatif"})
# # #             pos = col.count_documents({"label_final": "positif"})
# # #             neu = col.count_documents({"label_final": "neutre"})
# # #             conflits = col.count_documents({"conflit": True})
# # #             lignes.append(f"dataset_unifie : {total} commentaires")
# # #             lignes.append(f"Sentiments → négatif:{neg} | positif:{pos} | neutre:{neu}")
# # #             lignes.append(f"Conflits d'annotation : {conflits}")

# # #         return "\n".join(lignes)
# # #     except Exception as e:
# # #         return f"Erreur MongoDB : {e}"

# # # def repondre_question_specifique(question: str):
# # #     match = re.search(r'groupe[ _]?(\d+)', question, re.IGNORECASE)
# # #     if match and MONGO_OK:
# # #         group_id = f"groupe_{int(match.group(1)):04d}"
# # #         try:
# # #             doc = db["dataset_unifie"].find_one({"Group_ID": group_id})
# # #             if doc:
# # #                 texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# # #                 label = doc.get("label_final", "?")
# # #                 return f"📝 **{group_id}** :\n> {texte}\n\n🏷️ Sentiment : **{label}**"
# # #             return f"❌ Aucun commentaire trouvé pour **{group_id}**."
# # #         except Exception as e:
# # #             return f"Erreur : {e}"
# # #     return None

# # # def repondre_question_par_date(question: str) -> str or None:
# # #     match = re.search(r'(\d{2}/\d{2}/\d{4})', question)
# # #     if match and MONGO_OK:
# # #         date_cherchee = match.group(1)
# # #         try:
# # #             col = db["dataset_unifie"]
# # #             docs = list(col.find({"dates": {"$regex": date_cherchee}}).limit(5))
# # #             if docs:
# # #                 resultat = f"📝 **Commentaires du {date_cherchee}** :\n\n"
# # #                 for i, doc in enumerate(docs, 1):
# # #                     texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# # #                     label = doc.get("label_final", "?")
# # #                     resultat += f"{i}. {texte}\n   🏷️ Sentiment : {label}\n\n"
# # #                 return resultat
# # #             else:
# # #                 return f"❌ Aucun commentaire trouvé pour la date **{date_cherchee}**."
# # #         except Exception as e:
# # #             return f"Erreur recherche : {e}"
# # #     return None

# # # def detecter_langue(texte: str) -> str:
# # #     if re.search(r'[\u0600-\u06FF]', texte):
# # #         return "ar"
# # #     if re.search(r'\b[23789]\b|3likom|wach|chnou|bezzaf|machi|kayn|ndir|sahbi|slm|kifak|rahi|mazal', texte.lower()):
# # #         return "dz"
# # #     try:
# # #         return "fr" if detect(texte) == "fr" else "dz"
# # #     except LangDetectException:
# # #         return "fr"

# # # LANG_MAPPING = {"fr": "français", "ar": "arabe", "dz": "darija algérienne"}
# # # LANG_BADGE = {
# # #     "fr": '<span class="badge b-fr">FR</span>',
# # #     "ar": '<span class="badge b-ar">عر</span>',
# # #     "dz": '<span class="badge b-dz">DZ</span>',
# # # }

# # # def build_system_prompt(langue: str) -> str:
# # #     ctx = get_context_mongo()
# # #     return f"""Tu es un assistant analytique intelligent spécialisé dans l'analyse 
# # # de commentaires clients des opérateurs télécom algériens (Djezzy, Mobilis, Ooredoo).
# # # Tu comprends et réponds parfaitement en français, en darija algérienne et en arabe standard.

# # # === DONNÉES RÉELLES DE LA BASE MONGODB ===
# # # {ctx}
# # # ==========================================

# # # RÈGLES IMPORTANTES :
# # # - Réponds OBLIGATOIREMENT en : {LANG_MAPPING.get(langue, 'français')}
# # # - Si l'utilisateur parle darija → réponds en darija naturelle algérienne
# # # - Base tes analyses UNIQUEMENT sur les vraies données ci-dessus
# # # - Donne des chiffres précis, ne fabrique rien
# # # - Si une information manque, dis-le clairement
# # # - Formate tes réponses avec des listes et titres quand pertinent
# # # """

# # # # ──────────────────────────────────────────────────────────────────
# # # # TRAITEMENT DES QUESTIONS SPÉCIALES (graphiques, exports, etc.)
# # # # ──────────────────────────────────────────────────────────────────
# # # def traiter_question_speciale(question: str):
# # #     """Traite les questions spéciales (graphiques, exports, tableaux)"""
# # #     q = question.lower()
    
# # #     # Graphique évolution
# # #     if "graphique évolution" in q or "courbe" in q or "tendance graphique" in q or "évolution des sentiments" in q:
# # #         fig = graphique_evolution_sentiments()
# # #         if fig:
# # #             st.plotly_chart(fig, use_container_width=True)
# # #             return "📊 Voici le graphique d'évolution des sentiments."
# # #         return "❌ Pas assez de données pour générer le graphique."
    
# # #     # Camembert / répartition
# # #     if "camembert" in q or "répartition" in q or "part des sentiments" in q:
# # #         fig = graphique_repartition_sentiments()
# # #         if fig:
# # #             st.plotly_chart(fig, use_container_width=True)
# # #             return "🥧 Voici la répartition des sentiments."
# # #         return "❌ Pas assez de données."
    
# # #     # Top mots / nuage de mots
# # #     if "top mots" in q or "mots fréquents" in q:
# # #         if "nuage" in q:
# # #             nuage = generer_nuage_mots()
# # #             if nuage:
# # #                 st.markdown(nuage, unsafe_allow_html=True)
# # #                 return "☁️ Voici le nuage de mots des commentaires négatifs."
# # #         else:
# # #             fig = graphique_top_mots()
# # #             if fig:
# # #                 st.plotly_chart(fig, use_container_width=True)
# # #                 return "📊 Voici les mots les plus fréquents dans les commentaires négatifs."
# # #         return "❌ Pas assez de données."
    
# # #     # Export CSV
# # #     if "exporte" in q or "télécharge" in q or "csv" in q or "excel" in q or "json" in q:
# # #         if "csv" in q:
# # #             return exporter_csv(500)
# # #         elif "excel" in q:
# # #             return exporter_excel(500)
# # #         elif "json" in q:
# # #             return exporter_json(500)
# # #         return "❌ Format non reconnu. Utilise 'csv', 'excel' ou 'json'."
    
# # #     # Tableau des statistiques
# # #     if "tableau" in q or "statistiques tableau" in q or "stats tableau" in q:
# # #         tableau = generer_tableau_html()
# # #         if tableau:
# # #             st.markdown(tableau, unsafe_allow_html=True)
# # #             return "📋 Voici le tableau des statistiques."
# # #         return "❌ Pas assez de données."
    
# # #     return None

# # # # ──────────────────────────────────────────────────────────────────
# # # # APPEL GROQ — STREAMING
# # # # ──────────────────────────────────────────────────────────────────
# # # def appeler_groq_stream(messages_hist: list, question: str, langue: str):
# # #     msgs = [{"role": "system", "content": build_system_prompt(langue)}]
# # #     for m in messages_hist[-20:]:
# # #         msgs.append({"role": m["role"], "content": m["content"]})
# # #     msgs.append({"role": "user", "content": question})

# # #     try:
# # #         stream = groq_client.chat.completions.create(
# # #             model=GROQ_MODEL,
# # #             messages=msgs,
# # #             stream=True,
# # #             temperature=0.7,
# # #             max_tokens=512,
# # #             top_p=0.9,
# # #         )
# # #         for chunk in stream:
# # #             if st.session_state.stop_generation:
# # #                 st.session_state.stop_generation = False
# # #                 yield "\n\n⏹️ Génération arrêtée."
# # #                 break
# # #             delta = chunk.choices[0].delta.content
# # #             if delta:
# # #                 yield delta
# # #     except Exception as e:
# # #         yield f"\n⚠️ Erreur Groq : {e}\n"

# # # def render_md(txt: str) -> str:
# # #     txt = txt.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
# # #     txt = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', txt)
# # #     txt = re.sub(r'\*(.*?)\*', r'<em>\1</em>', txt)
# # #     txt = re.sub(r'`(.*?)`', r'<code>\1</code>', txt)
# # #     txt = txt.replace("\n\n","<br><br>").replace("\n","<br>")
# # #     txt = re.sub(r'<br>[-•] ','<br>• ', txt)
# # #     return txt

# # # # ──────────────────────────────────────────────────────────────────
# # # # SAUVEGARDE CONVERSATIONS
# # # # ──────────────────────────────────────────────────────────────────
# # # def charger_convs_mongo() -> dict:
# # #     if not MONGO_OK:
# # #         return {}
# # #     try:
# # #         docs = list(db.chat_sessions.find({},{"_id":0}).sort("updated_at",-1).limit(30))
# # #         return {d["conv_id"]: {
# # #             "title": d.get("title","Conversation"),
# # #             "messages": d.get("messages",[]),
# # #             "created_at": str(d.get("created_at","")),
# # #         } for d in docs}
# # #     except Exception:
# # #         return {}

# # # def sauvegarder_conv(cid, messages):
# # #     st.session_state.conversations[cid]["messages"] = messages
# # #     if MONGO_OK:
# # #         try:
# # #             db.chat_sessions.update_one(
# # #                 {"conv_id": cid},
# # #                 {"$set": {
# # #                     "messages": messages,
# # #                     "updated_at": datetime.now(),
# # #                     "title": st.session_state.conversations[cid].get("title",""),
# # #                 }},
# # #                 upsert=True,
# # #             )
# # #         except Exception:
# # #             pass

# # # def nouvelle_conversation():
# # #     cid = str(uuid.uuid4())[:8]
# # #     st.session_state.conversations[cid] = {
# # #         "title": "Nouvelle conversation",
# # #         "messages": [],
# # #         "created_at": datetime.now().isoformat(),
# # #     }
# # #     st.session_state.active_conv = cid
# # #     if MONGO_OK:
# # #         try:
# # #             db.chat_sessions.insert_one({
# # #                 "conv_id": cid,
# # #                 "title": "Nouvelle conversation",
# # #                 "messages": [],
# # #                 "created_at": datetime.now(),
# # #                 "updated_at": datetime.now(),
# # #             })
# # #         except Exception:
# # #             pass
# # #     return cid

# # # # ──────────────────────────────────────────────────────────────────
# # # # SESSION STATE
# # # # ──────────────────────────────────────────────────────────────────
# # # if "conversations" not in st.session_state:
# # #     st.session_state.conversations = charger_convs_mongo()
# # # if "active_conv" not in st.session_state:
# # #     st.session_state.active_conv = None

# # # if not st.session_state.active_conv or st.session_state.active_conv not in st.session_state.conversations:
# # #     if st.session_state.conversations:
# # #         st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
# # #     else:
# # #         nouvelle_conversation()

# # # # ──────────────────────────────────────────────────────────────────
# # # # SIDEBAR
# # # # ──────────────────────────────────────────────────────────────────
# # # with st.sidebar:
# # #     st.markdown(f"""
# # #     <div class="sidebar-title">Chat<span class="accent">Bot</span><span class="groq-badge">⚡ GROQ</span></div>
# # #     <div class="sidebar-sub">{GROQ_MODEL} · Télécom DZ</div>
# # #     """, unsafe_allow_html=True)

# # #     if st.button("＋ Nouvelle conversation"):
# # #         nouvelle_conversation()
# # #         st.rerun()

# # #     st.markdown("---")

# # #     col_m, col_g = st.columns(2)
# # #     with col_m:
# # #         if MONGO_OK:
# # #             st.success("MongoDB ✓")
# # #         else:
# # #             st.error("MongoDB ✗")
# # #     with col_g:
# # #         if GROQ_OK:
# # #             st.success("Groq ✓")
# # #         else:
# # #             st.error("Groq ✗")

# # #     st.markdown("---")
# # #     st.markdown('<div class="section-lbl">Conversations récentes</div>', unsafe_allow_html=True)

# # #     convs = st.session_state.conversations
# # #     for cid, conv in list(convs.items()):
# # #         title = conv.get("title","Conversation")[:35]
# # #         is_active = cid == st.session_state.active_conv
# # #         label = f"{'▶ ' if is_active else ''}{title}"
# # #         if st.button(label, key=f"btn_conv_{cid}"):
# # #             st.session_state.active_conv = cid
# # #             st.rerun()

# # #     st.markdown("---")

# # #     if st.button("🗑️ Supprimer conversation"):
# # #         cid = st.session_state.active_conv
# # #         if cid and cid in convs:
# # #             del st.session_state.conversations[cid]
# # #             if MONGO_OK:
# # #                 try:
# # #                     db.chat_sessions.delete_one({"conv_id": cid})
# # #                 except Exception:
# # #                     pass
# # #             st.session_state.active_conv = None
# # #             st.rerun()

# # #     st.markdown("---")
# # #     st.markdown('<div class="section-lbl">Questions suggérées</div>', unsafe_allow_html=True)

# # #     suggestions = [
# # #         "Donne moi les stats globales",
# # #         "Pourquoi ya des commentaires négatifs ?",
# # #         "wach kayn bezzaf chikayat ?",
# # #         "كم عدد التعليقات السلبية؟",
# # #         "Analyse les tendances",
# # #         "Exemples de commentaires négatifs",
# # #         "ما هي أكثر المشاكل شيوعاً؟",
# # #         "Compare positif vs négatif",
# # #         "Affiche le graphique d'évolution",
# # #         "Montre le camembert",
# # #         "Top mots négatifs",
# # #         "Exporte en CSV",
# # #         "Tableau des statistiques",
# # #     ]
# # #     for sug in suggestions:
# # #         if st.button(sug, key=f"sug_{sug}"):
# # #             st.session_state["pending_question"] = sug
# # #             st.rerun()

# # #     # Export
# # #     active_msgs = convs.get(st.session_state.active_conv, {}).get("messages", [])
# # #     if active_msgs:
# # #         st.markdown("---")
# # #         st.download_button(
# # #             "⬇️ Exporter JSON",
# # #             data=json.dumps(active_msgs, ensure_ascii=False, indent=2, default=str),
# # #             file_name=f"conv_{st.session_state.active_conv}.json",
# # #             mime="application/json",
# # #             use_container_width=True,
# # #         )

# # # # ──────────────────────────────────────────────────────────────────
# # # # ZONE PRINCIPALE — CHAT
# # # # ──────────────────────────────────────────────────────────────────
# # # active_cid = st.session_state.active_conv
# # # active_conv = st.session_state.conversations.get(active_cid, {})
# # # messages = active_conv.get("messages", [])

# # # col_title, col_info = st.columns([4, 1])
# # # with col_title:
# # #     st.markdown(f"""
# # #     <div style="padding:.5rem 0 1rem;">
# # #         <div style="font-size:18px;font-weight:600;color:#e8e6e0;">
# # #             {active_conv.get('title','Conversation')}
# # #         </div>
# # #         <div style="font-size:11px;color:#4b5563;font-family:'IBM Plex Mono',monospace;">
# # #             {len(messages)} messages · ⚡ {GROQ_MODEL} ·
# # #             {'🟢 MongoDB' if MONGO_OK else '🔴 MongoDB'} ·
# # #             {'🟢 Groq' if GROQ_OK else '🔴 Groq'}
# # #         </div>
# # #     </div>
# # #     """, unsafe_allow_html=True)

# # # # Affichage des messages
# # # if not messages:
# # #     st.markdown("""
# # #     <div style="text-align:center;padding:4rem 2rem;color:#374151;">
# # #         <div style="font-size:48px;margin-bottom:1rem;">⚡</div>
# # #         <div style="font-size:18px;font-weight:600;color:#6b7280;margin-bottom:.5rem;">
# # #             Comment puis-je vous aider ?
# # #         </div>
# # #         <div style="font-size:13px;color:#374151;font-family:'IBM Plex Mono',monospace;">
# # #             Parlez en français · darija · عربي · réponse en 1-2 sec
# # #         </div>
# # #     </div>
# # #     """, unsafe_allow_html=True)
# # # else:
# # #     for msg in messages:
# # #         role = msg["role"]
# # #         content = msg["content"]
# # #         lang = msg.get("lang", "fr")
# # #         ts = msg.get("timestamp", "")[:16]
# # #         badge = LANG_BADGE.get(lang, "")

# # #         if role == "user":
# # #             safe = content.replace("<","&lt;").replace(">","&gt;")
# # #             st.markdown(f"""
# # #             <div class="msg-user"><div class="bubble">{safe}</div></div>
# # #             <div class="msg-meta" style="text-align:right;padding-right:4px;">
# # #                 {badge} {ts}
# # #             </div>
# # #             """, unsafe_allow_html=True)
# # #         else:
# # #             st.markdown(f"""
# # #             <div class="msg-bot">
# # #                 <div class="bot-icon">⚡</div>
# # #                 <div>
# # #                     <div class="bubble">{render_md(content)}</div>
# # #                     <div class="msg-meta">{ts}</div>
# # #                 </div>
# # #             </div>
# # #             """, unsafe_allow_html=True)

# # # # ── INPUT BAR FIXE ────────────────────────────────────────────────
# # # pending = st.session_state.pop("pending_question", None)

# # # col_mic, col_file, col_input, col_send = st.columns([0.6, 0.6, 8, 0.8])

# # # with col_mic:
# # #     audio = mic_recorder(
# # #         start_prompt="🎤",
# # #         stop_prompt="⏹️",
# # #         just_once=True,
# # #         use_container_width=True,
# # #         key="mic_recorder"
# # #     )

# # # with col_file:
# # #     fichier = st.file_uploader("", type=["txt", "csv", "json", "pdf"], 
# # #                                 label_visibility="collapsed",
# # #                                 key="file_uploader")
# # #     if fichier:
# # #         contenu = fichier.read().decode("utf-8", errors="ignore")[:2000]
# # #         st.session_state["pending_question"] = f"Analyse ce fichier ({fichier.name}) :\n\n{contenu}"
# # #         st.rerun()

# # # with col_input:
# # #     question_input = st.text_area(
# # #         "", 
# # #         placeholder="Écris ton message en français, darija ou عربي…",
# # #         label_visibility="collapsed",
# # #         height=68,
# # #         key="chat_input"
# # #     )

# # # with col_send:
# # #     send = st.button("➤", use_container_width=True, key="send_btn")

# # # # Traitement micro
# # # if audio and 'bytes' in audio:
# # #     try:
# # #         audio_bytes = io.BytesIO(audio['bytes'])
# # #         sound = AudioSegment.from_file(audio_bytes)
# # #         sound.export("temp_audio.wav", format="wav")
# # #         recognizer = sr.Recognizer()
# # #         with sr.AudioFile("temp_audio.wav") as source:
# # #             audio_data = recognizer.record(source)
# # #             texte = recognizer.recognize_google(audio_data, language="fr-FR")
# # #             st.session_state["pending_question"] = texte
# # #             st.rerun()
# # #     except Exception as e:
# # #         st.warning(f"Micro : {e}")

# # # # Question finale
# # # question = None
# # # if send and question_input and question_input.strip():
# # #     question = question_input.strip()
# # # elif pending:
# # #     question = pending

# # # if question:
# # #     # 1. Vérifier les réponses directes (Group_ID, date)
# # #     rep_directe = repondre_question_specifique(question)
# # #     if not rep_directe:
# # #         rep_directe = repondre_question_par_date(question)
    
# # #     # 2. Vérifier les questions spéciales (graphiques, exports, tableaux)
# # #     if not rep_directe:
# # #         rep_speciale = traiter_question_speciale(question)
# # #     else:
# # #         rep_speciale = None

# # #     ts_now = datetime.now().strftime("%Y-%m-%d %H:%M")
# # #     lang_det = detecter_langue(question)

# # #     if rep_directe:
# # #         messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
# # #         messages.append({"role": "assistant", "content": rep_directe, "lang": "fr", "timestamp": ts_now})
# # #         sauvegarder_conv(active_cid, messages)
# # #         st.rerun()
# # #     elif rep_speciale:
# # #         messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
# # #         # Pour les graphiques, on a déjà affiché via st.plotly_chart
# # #         # Pour les exports, on a retourné un lien
# # #         if not ("graphique" in question.lower() or "camembert" in question.lower() or "tableau" in question.lower()):
# # #             st.markdown(rep_speciale)
# # #         messages.append({"role": "assistant", "content": rep_speciale, "lang": "fr", "timestamp": ts_now})
# # #         sauvegarder_conv(active_cid, messages)
# # #         st.rerun()
# # #     else:
# # #         messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
# # #         if len(messages) == 1:
# # #             titre = question[:40] + ("…" if len(question) > 40 else "")
# # #             st.session_state.conversations[active_cid]["title"] = titre
# # #             if MONGO_OK:
# # #                 try:
# # #                     db.chat_sessions.update_one({"conv_id": active_cid}, {"$set": {"title": titre}})
# # #                 except Exception:
# # #                     pass
# # #         sauvegarder_conv(active_cid, messages)
# # #         st.rerun()

# # # # ── Génération réponse GROQ ───────────────────────────────────────
# # # if messages and messages[-1]["role"] == "user":
# # #     st.session_state.stop_generation = False

# # #     if st.button("⏹️ Stop", key="stop_btn"):
# # #         st.session_state.stop_generation = True

# # #     if not GROQ_OK:
# # #         st.error("""
# # #         ⚠️ Clé Groq invalide ou manquante !
# # #         1. Va sur https://console.groq.com
# # #         2. Crée une clé API gratuite
# # #         """)
# # #     else:
# # #         placeholder = st.empty()
# # #         placeholder.markdown("""
# # #         <div class="msg-bot">
# # #             <div class="bot-icon">⚡</div>
# # #             <div class="bubble">
# # #                 <div class="typing"><span></span><span></span><span></span></div>
# # #             </div>
# # #         </div>
# # #         """, unsafe_allow_html=True)

# # #         full_response = ""
# # #         langue_user = messages[-1].get("lang", "fr")

# # #         for delta in appeler_groq_stream(messages[:-1], messages[-1]["content"], langue_user):
# # #             full_response += delta
# # #             placeholder.markdown(f"""
# # #             <div class="msg-bot">
# # #                 <div class="bot-icon">⚡</div>
# # #                 <div class="bubble">{render_md(full_response)}▌</div>
# # #             </div>
# # #             """, unsafe_allow_html=True)

# # #         placeholder.empty()
# # #         messages.append({
# # #             "role": "assistant",
# # #             "content": full_response,
# # #             "lang": langue_user,
# # #             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
# # #         })
# # #         sauvegarder_conv(active_cid, messages)
# # #         st.rerun()

# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # """
# # chatbot_groq.py — ChatBot Algérien avec GROQ (ultra rapide) + MongoDB
# # Conversation naturelle en français, darija, arabe
# # Analyse intelligente des commentaires télécom
# # """

# # import streamlit as st
# # from groq import Groq
# # from pymongo import MongoClient
# # from datetime import datetime
# # import re, json, os, uuid
# # from langdetect import detect, LangDetectException
# # from streamlit_mic_recorder import mic_recorder
# # import speech_recognition as sr
# # import io
# # from pydub import AudioSegment
# # import sys

# # # ──────────────────────────────────────────────────────────────────
# # # CONFIG
# # # ──────────────────────────────────────────────────────────────────
# # GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
# # GROQ_MODEL   = "llama-3.1-8b-instant"

# # MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018/")
# # DB_NAME   = "telecom_algerie"

# # groq_client = Groq(api_key=GROQ_API_KEY)

# # # ──────────────────────────────────────────────────────────────────
# # # IMPORT DES MODULES
# # # ──────────────────────────────────────────────────────────────────
# # sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# # from modules.graphiques        import *
# # from modules.export            import *
# # from modules.statistiques      import *
# # from modules.analyse_textuelle import *

# # # ──────────────────────────────────────────────────────────────────
# # # PAGE CONFIG
# # # ──────────────────────────────────────────────────────────────────
# # st.set_page_config(
# #     page_title="ChatBot Algérien — Groq ⚡",
# #     page_icon="🤖",
# #     layout="wide",
# #     initial_sidebar_state="expanded",
# # )

# # if "stop_generation" not in st.session_state:
# #     st.session_state.stop_generation = False

# # # ──────────────────────────────────────────────────────────────────
# # # STYLE CSS
# # # ──────────────────────────────────────────────────────────────────
# # st.markdown("""
# # <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap" rel="stylesheet">
# # <style>
# # html,body,[class*="css"]{
# #     font-family:'Syne',sans-serif!important;
# #     background:#0a0b0f!important;
# #     color:#e8e6e0!important;
# # }
# # [data-testid="stAppViewContainer"]{background:#0a0b0f!important;}
# # [data-testid="stSidebar"]{
# #     background:#0d0e14!important;
# #     border-right:1px solid #1a1d2e!important;
# # }
# # .msg-user{display:flex;justify-content:flex-end;margin:12px 0;}
# # .msg-user .bubble{
# #     background:#1a2540;border:1px solid #2a3560;
# #     border-radius:18px 18px 4px 18px;
# #     padding:12px 18px;max-width:70%;
# #     font-size:15px;line-height:1.65;color:#c8d3f5;
# # }
# # .msg-bot{display:flex;gap:12px;margin:12px 0;align-items:flex-start;}
# # .bot-icon{
# #     width:36px;height:36px;min-width:36px;
# #     background:linear-gradient(135deg,#f97316,#eab308);
# #     border-radius:10px;display:flex;
# #     align-items:center;justify-content:center;
# #     font-size:18px;margin-top:2px;
# # }
# # .msg-bot .bubble{
# #     background:#0f1117;border:1px solid #1e2130;
# #     border-radius:4px 18px 18px 18px;
# #     padding:14px 18px;max-width:75%;
# #     font-size:15px;line-height:1.75;color:#e8e6e0;
# # }
# # .msg-bot .bubble strong{color:#f97316;}
# # .msg-bot .bubble code{
# #     background:#1a1d2e;border:1px solid #2a2d3e;
# #     border-radius:4px;padding:1px 6px;
# #     font-family:'IBM Plex Mono',monospace;
# #     font-size:13px;color:#fbbf24;
# # }
# # .msg-meta{
# #     font-size:11px;color:#374151;
# #     font-family:'IBM Plex Mono',monospace;
# #     margin-top:5px;padding-left:48px;
# # }
# # .stButton>button{
# #     background:transparent!important;
# #     border:1px solid #1e2130!important;
# #     color:#9ca3af!important;border-radius:8px!important;
# #     font-family:'IBM Plex Mono',monospace!important;
# #     font-size:12px!important;width:100%;
# #     text-align:left!important;padding:8px 12px!important;
# #     transition:all .2s!important;
# # }
# # .stButton>button:hover{
# #     border-color:#f97316!important;
# #     color:#f97316!important;
# #     background:rgba(249,115,22,.06)!important;
# # }
# # .badge{
# #     display:inline-block;padding:1px 8px;
# #     border-radius:20px;font-size:10px;font-weight:600;
# #     font-family:'IBM Plex Mono',monospace;
# # }
# # .b-fr{background:rgba(59,130,246,.15);color:#60a5fa;border:1px solid rgba(59,130,246,.3);}
# # .b-ar{background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.3);}
# # .b-dz{background:rgba(249,115,22,.15);color:#fb923c;border:1px solid rgba(249,115,22,.3);}
# # .sidebar-title{font-size:20px;font-weight:700;color:#e8e6e0;}
# # .sidebar-sub{
# #     font-size:10px;color:#4b5563;
# #     font-family:'IBM Plex Mono',monospace;
# #     letter-spacing:.08em;text-transform:uppercase;
# #     margin-bottom:1.5rem;
# # }
# # .accent{color:#f97316;}
# # .section-lbl{
# #     font-size:10px;color:#374151;
# #     font-family:'IBM Plex Mono',monospace;
# #     letter-spacing:.1em;text-transform:uppercase;
# #     padding:1rem 0 .4rem;
# # }
# # .typing{display:flex;gap:5px;align-items:center;padding:8px 0;}
# # .typing span{
# #     width:7px;height:7px;border-radius:50%;
# #     background:#f97316;display:inline-block;
# #     animation:bounce 1.2s infinite;
# # }
# # .typing span:nth-child(2){animation-delay:.2s;}
# # .typing span:nth-child(3){animation-delay:.4s;}
# # @keyframes bounce{
# #     0%,60%,100%{transform:translateY(0);opacity:.4;}
# #     30%{transform:translateY(-6px);opacity:1;}
# # }
# # .groq-badge{
# #     display:inline-block;
# #     background:linear-gradient(135deg,#f97316,#eab308);
# #     color:#000;font-size:10px;font-weight:700;
# #     padding:2px 8px;border-radius:20px;
# #     font-family:'IBM Plex Mono',monospace;
# #     margin-left:6px;
# # }
# # hr{border-color:#1a1d2e!important;margin:.75rem 0!important;}
# # ::-webkit-scrollbar{width:4px;}
# # ::-webkit-scrollbar-track{background:#0a0b0f;}
# # ::-webkit-scrollbar-thumb{background:#1e2130;border-radius:10px;}
# # .input-bar {
# #     position: fixed;
# #     bottom: 0; left: 0; right: 0;
# #     background: #0d0e14;
# #     border-top: 1px solid #1a1d2e;
# #     padding: 14px 24px;
# #     z-index: 999;
# #     display: flex;
# #     align-items: center;
# #     gap: 10px;
# # }
# # .input-bar textarea {
# #     flex: 1;
# #     background: #0f1117 !important;
# #     border: 1px solid #1e2130 !important;
# #     border-radius: 12px !important;
# #     color: #e8e6e0 !important;
# #     padding: 12px 16px !important;
# #     font-size: 14px !important;
# #     font-family: 'Syne', sans-serif !important;
# #     resize: none !important;
# #     outline: none !important;
# #     min-height: 48px;
# #     max-height: 120px;
# # }
# # .input-bar textarea:focus {
# #     border-color: #f97316 !important;
# # }
# # .icon-btn {
# #     background: #0f1117;
# #     border: 1px solid #1e2130;
# #     border-radius: 10px;
# #     width: 44px; height: 44px;
# #     display: flex; align-items: center; justify-content: center;
# #     font-size: 18px; cursor: pointer;
# #     color: #9ca3af;
# #     transition: all .2s;
# #     flex-shrink: 0;
# # }
# # .icon-btn:hover {
# #     border-color: #f97316;
# #     color: #f97316;
# #     background: rgba(249,115,22,.08);
# # }
# # .send-btn {
# #     background: linear-gradient(135deg, #f97316, #eab308);
# #     border: none;
# #     border-radius: 10px;
# #     width: 44px; height: 44px;
# #     display: flex; align-items: center; justify-content: center;
# #     font-size: 18px; cursor: pointer;
# #     color: #000;
# #     flex-shrink: 0;
# #     transition: opacity .2s;
# # }
# # .send-btn:hover { opacity: 0.85; }
# # .main > div { padding-bottom: 100px; }
# # </style>
# # """, unsafe_allow_html=True)

# # # ──────────────────────────────────────────────────────────────────
# # # MONGODB
# # # ──────────────────────────────────────────────────────────────────
# # @st.cache_resource(ttl=0)
# # def get_db():
# #     try:
# #         c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
# #         c.admin.command("ping")
# #         return c[DB_NAME]
# #     except Exception:
# #         return None

# # db     = get_db()
# # MONGO_OK = db is not None

# # def check_groq() -> bool:
# #     try:
# #         groq_client.models.list()
# #         return True
# #     except Exception:
# #         return False

# # GROQ_OK = check_groq()

# # # ──────────────────────────────────────────────────────────────────
# # # CONTEXTE MONGODB
# # # ──────────────────────────────────────────────────────────────────
# # def get_context_mongo() -> str:
# #     if not MONGO_OK:
# #         return "Base MongoDB non disponible."
# #     try:
# #         lignes = []
# #         cols = db.list_collection_names()
# #         lignes.append(f"Collections disponibles : {', '.join(cols)}")

# #         if "dataset_unifie" in cols:
# #             col      = db["dataset_unifie"]
# #             total    = col.count_documents({})
# #             neg      = col.count_documents({"label_final": "negatif"})
# #             pos      = col.count_documents({"label_final": "positif"})
# #             neu      = col.count_documents({"label_final": "neutre"})
# #             conflits = col.count_documents({"conflit": True})
# #             lignes.append(f"dataset_unifie : {total} commentaires")
# #             lignes.append(f"Sentiments → négatif:{neg} | positif:{pos} | neutre:{neu}")
# #             lignes.append(f"Conflits d'annotation : {conflits}")

# #         return "\n".join(lignes)
# #     except Exception as e:
# #         return f"Erreur MongoDB : {e}"


# # def repondre_question_specifique(question: str):
# #     match = re.search(r'groupe[ _]?(\d+)', question, re.IGNORECASE)
# #     if match and MONGO_OK:
# #         group_id = f"groupe_{int(match.group(1)):04d}"
# #         try:
# #             doc = db["dataset_unifie"].find_one({"Group_ID": group_id})
# #             if doc:
# #                 texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# #                 label = doc.get("label_final", "?")
# #                 return f"📝 **{group_id}** :\n> {texte}\n\n🏷️ Sentiment : **{label}**"
# #             return f"❌ Aucun commentaire trouvé pour **{group_id}**."
# #         except Exception as e:
# #             return f"Erreur : {e}"
# #     return None


# # def repondre_question_par_date(question: str):
# #     match = re.search(r'(\d{2}/\d{2}/\d{4})', question)
# #     if match and MONGO_OK:
# #         date_cherchee = match.group(1)
# #         try:
# #             col  = db["dataset_unifie"]
# #             docs = list(col.find({"dates": {"$regex": date_cherchee}}).limit(5))
# #             if docs:
# #                 resultat = f"📝 **Commentaires du {date_cherchee}** :\n\n"
# #                 for i, doc in enumerate(docs, 1):
# #                     texte = doc.get("Commentaire_Client_Original", "Pas de texte")
# #                     label = doc.get("label_final", "?")
# #                     resultat += f"{i}. {texte}\n   🏷️ Sentiment : {label}\n\n"
# #                 return resultat
# #             return f"❌ Aucun commentaire trouvé pour la date **{date_cherchee}**."
# #         except Exception as e:
# #             return f"Erreur recherche : {e}"
# #     return None


# # def detecter_langue(texte: str) -> str:
# #     if re.search(r'[\u0600-\u06FF]', texte):
# #         return "ar"
# #     if re.search(
# #         r'\b[23789]\b|3likom|wach|chnou|bezzaf|machi|kayn|ndir|sahbi|slm|kifak|rahi|mazal',
# #         texte.lower()
# #     ):
# #         return "dz"
# #     try:
# #         return "fr" if detect(texte) == "fr" else "dz"
# #     except LangDetectException:
# #         return "fr"


# # LANG_MAPPING = {"fr": "français", "ar": "arabe", "dz": "darija algérienne"}
# # LANG_BADGE   = {
# #     "fr": '<span class="badge b-fr">FR</span>',
# #     "ar": '<span class="badge b-ar">عر</span>',
# #     "dz": '<span class="badge b-dz">DZ</span>',
# # }

# # # ──────────────────────────────────────────────────────────────────
# # # SYSTEM PROMPT  (FIX 1 — interdire la génération de SVG/graphiques)
# # # ──────────────────────────────────────────────────────────────────
# # def build_system_prompt(langue: str) -> str:
# #     ctx = get_context_mongo()
# #     return f"""Tu es un assistant analytique intelligent spécialisé dans l'analyse
# # de commentaires clients des opérateurs télécom algériens (Djezzy, Mobilis, Ooredoo).
# # Tu comprends et réponds parfaitement en français, en darija algérienne et en arabe standard.

# # === DONNÉES RÉELLES DE LA BASE MONGODB ===
# # {ctx}
# # ==========================================

# # RÈGLES IMPORTANTES :
# # - Réponds OBLIGATOIREMENT en : {LANG_MAPPING.get(langue, 'français')}
# # - Si l'utilisateur parle darija → réponds en darija naturelle algérienne
# # - Base tes analyses UNIQUEMENT sur les vraies données ci-dessus
# # - Donne des chiffres précis, ne fabrique rien
# # - Si une information manque, dis-le clairement
# # - Formate tes réponses avec des listes et titres quand pertinent
# # - NE génère JAMAIS de code SVG, HTML, XML, ni de graphiques dans ta réponse
# # - NE génère JAMAIS de visualisation, diagramme ou représentation graphique sous quelque forme que ce soit
# # - Si l'utilisateur demande un graphique ou une visualisation, réponds uniquement par
# #   une courte phrase confirmant que le système va afficher le résultat automatiquement
# # """

# # # ──────────────────────────────────────────────────────────────────
# # # QUESTIONS SPÉCIALES — graphiques, exports, tableaux
# # # (FIX 2 — ne jamais retourner None quand un mot-clé est détecté)
# # # ──────────────────────────────────────────────────────────────────

# # # Mots-clés graphiques — utilisés pour détecter ET pour bloquer le fallback GROQ
# # GRAPHIQUE_KEYWORDS = [
# #     "graphique", "courbe", "camembert", "diagramme", "visuali",
# #     "chart", "plot", "nuage de mots", "évolution", "evolution",
# #     "tendance", "top mots", "mots fréquents", "mots frequents",
# #     "répartition", "repartition", "part des sentiments", "tableau stat",
# # ]

# # def est_question_graphique(question: str) -> bool:
# #     """Retourne True si la question concerne un graphique ou une visualisation."""
# #     q = question.lower()
# #     return any(kw in q for kw in GRAPHIQUE_KEYWORDS)


# # def traiter_question_speciale(question: str):
# #     """
# #     Traite les questions spéciales (graphiques, exports, tableaux).
# #     Retourne TOUJOURS une chaîne (jamais None) quand un mot-clé est détecté,
# #     afin d'éviter que GROQ prenne le relais et génère du SVG brut.
# #     """
# #     q = question.lower()

# #   # ── Graphique évolution ──────────────────────────────────────
# #     if any(k in q for k in [
# #         "graphique évolution", "graphique evolution",
# #         "évolution", "evolution", "courbe", "tendance graphique",
# #     ]):
# #         try:
# #             fig = graphique_evolution_sentiments()
# #         except Exception as e:
# #             return f"❌ Erreur lors de la génération du graphique : {e}"

# #         if fig:
# #             st.plotly_chart(fig, use_container_width=True)
# #             return "📊 Voici le graphique d'évolution des sentiments dans le temps."
# #         return (
# #             "❌ Impossible de générer ce graphique. "
# #             "Vérifiez que le champ 'dates' est bien renseigné dans dataset_unifie."
# #         )

# #     # ── Top mots / nuage de mots ─────────────────────────────────
# #     if any(k in q for k in ["top mots", "mots fréquents", "mots frequents", "mots négatifs", "mots negatifs"]):
# #         if "nuage" in q:
# #             try:
# #                 nuage = generer_nuage_mots()
# #             except Exception as e:
# #                 return f"❌ Erreur nuage de mots : {e}"
# #             if nuage:
# #                 st.markdown(nuage, unsafe_allow_html=True)
# #                 return "☁️ Voici le nuage de mots des commentaires négatifs."
# #             return "❌ Pas assez de données pour générer le nuage de mots."
# #         else:
# #             try:
# #                 fig = graphique_top_mots()
# #             except Exception as e:
# #                 return f"❌ Erreur graphique mots : {e}"
# #             if fig:
# #                 st.plotly_chart(fig, use_container_width=True)
# #                 return "📊 Voici les mots les plus fréquents dans les commentaires négatifs."
# #             return "❌ Pas assez de données pour ce graphique."

# #     # ── Export CSV / Excel / JSON ────────────────────────────────
# #     if "exporte" in q or "télécharge" in q or "telecharge" in q or "csv" in q or "excel" in q or "json" in q:
# #         if "csv" in q:
# #             result = exporter_csv(500)
# #             return result if result else "❌ Erreur lors de l'export CSV."
# #         elif "excel" in q:
# #             result = exporter_excel(500)
# #             return result if result else "❌ Erreur lors de l'export Excel."
# #         elif "json" in q:
# #             result = exporter_json(500)
# #             return result if result else "❌ Erreur lors de l'export JSON."
# #         return "❌ Format non reconnu. Précise 'csv', 'excel' ou 'json'."

# #     # ── Tableau des statistiques ─────────────────────────────────
# #     if "tableau" in q and ("stat" in q or "statistique" in q):
# #         try:
# #             tableau = generer_tableau_html()
# #         except Exception as e:
# #             return f"❌ Erreur lors de la génération du tableau : {e}"

# #         if tableau:
# #             st.markdown(tableau, unsafe_allow_html=True)
# #             return "📋 Voici le tableau des statistiques."
# #         return "❌ Pas assez de données pour générer le tableau."

# #     # Aucun mot-clé reconnu → on laisse GROQ répondre
# #     return None

# # # ──────────────────────────────────────────────────────────────────
# # # APPEL GROQ — STREAMING
# # # ──────────────────────────────────────────────────────────────────
# # def appeler_groq_stream(messages_hist: list, question: str, langue: str):
# #     msgs = [{"role": "system", "content": build_system_prompt(langue)}]
# #     for m in messages_hist[-20:]:
# #         msgs.append({"role": m["role"], "content": m["content"]})
# #     msgs.append({"role": "user", "content": question})

# #     try:
# #         stream = groq_client.chat.completions.create(
# #             model=GROQ_MODEL,
# #             messages=msgs,
# #             stream=True,
# #             temperature=0.7,
# #             max_tokens=512,
# #             top_p=0.9,
# #         )
# #         for chunk in stream:
# #             if st.session_state.stop_generation:
# #                 st.session_state.stop_generation = False
# #                 yield "\n\n⏹️ Génération arrêtée."
# #                 break
# #             delta = chunk.choices[0].delta.content
# #             if delta:
# #                 yield delta
# #     except Exception as e:
# #         yield f"\n⚠️ Erreur Groq : {e}\n"


# # def render_md(txt: str) -> str:
# #     txt = txt.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
# #     txt = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', txt)
# #     txt = re.sub(r'\*(.*?)\*',     r'<em>\1</em>',         txt)
# #     txt = re.sub(r'`(.*?)`',       r'<code>\1</code>',     txt)
# #     txt = txt.replace("\n\n", "<br><br>").replace("\n", "<br>")
# #     txt = re.sub(r'<br>[-•] ', '<br>• ', txt)
# #     return txt

# # # ──────────────────────────────────────────────────────────────────
# # # SAUVEGARDE CONVERSATIONS
# # # ──────────────────────────────────────────────────────────────────
# # def charger_convs_mongo() -> dict:
# #     if not MONGO_OK:
# #         return {}
# #     try:
# #         docs = list(
# #             db.chat_sessions.find({}, {"_id": 0})
# #             .sort("updated_at", -1)
# #             .limit(30)
# #         )
# #         return {
# #             d["conv_id"]: {
# #                 "title":      d.get("title", "Conversation"),
# #                 "messages":   d.get("messages", []),
# #                 "created_at": str(d.get("created_at", "")),
# #             }
# #             for d in docs
# #         }
# #     except Exception:
# #         return {}


# # def sauvegarder_conv(cid, messages):
# #     st.session_state.conversations[cid]["messages"] = messages
# #     if MONGO_OK:
# #         try:
# #             db.chat_sessions.update_one(
# #                 {"conv_id": cid},
# #                 {"$set": {
# #                     "messages":   messages,
# #                     "updated_at": datetime.now(),
# #                     "title":      st.session_state.conversations[cid].get("title", ""),
# #                 }},
# #                 upsert=True,
# #             )
# #         except Exception:
# #             pass


# # def nouvelle_conversation():
# #     cid = str(uuid.uuid4())[:8]
# #     st.session_state.conversations[cid] = {
# #         "title":      "Nouvelle conversation",
# #         "messages":   [],
# #         "created_at": datetime.now().isoformat(),
# #     }
# #     st.session_state.active_conv = cid
# #     if MONGO_OK:
# #         try:
# #             db.chat_sessions.insert_one({
# #                 "conv_id":    cid,
# #                 "title":      "Nouvelle conversation",
# #                 "messages":   [],
# #                 "created_at": datetime.now(),
# #                 "updated_at": datetime.now(),
# #             })
# #         except Exception:
# #             pass
# #     return cid

# # # ──────────────────────────────────────────────────────────────────
# # # SESSION STATE
# # # ──────────────────────────────────────────────────────────────────
# # if "conversations" not in st.session_state:
# #     st.session_state.conversations = charger_convs_mongo()
# # if "active_conv" not in st.session_state:
# #     st.session_state.active_conv = None

# # if (
# #     not st.session_state.active_conv
# #     or st.session_state.active_conv not in st.session_state.conversations
# # ):
# #     if st.session_state.conversations:
# #         st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
# #     else:
# #         nouvelle_conversation()

# # # ──────────────────────────────────────────────────────────────────
# # # SIDEBAR
# # # ──────────────────────────────────────────────────────────────────
# # with st.sidebar:
# #     st.markdown(f"""
# #     <div class="sidebar-title">Chat<span class="accent">Bot</span>
# #     <span class="groq-badge">⚡ GROQ</span></div>
# #     <div class="sidebar-sub">{GROQ_MODEL} · Télécom DZ</div>
# #     """, unsafe_allow_html=True)

# #     if st.button("＋ Nouvelle conversation"):
# #         nouvelle_conversation()
# #         st.rerun()

# #     st.markdown("---")

# #     col_m, col_g = st.columns(2)
# #     with col_m:
# #         if MONGO_OK:
# #             st.success("MongoDB ✓")
# #         else:
# #             st.error("MongoDB ✗")
# #     with col_g:
# #         if GROQ_OK:
# #             st.success("Groq ✓")
# #         else:
# #             st.error("Groq ✗")

# #     st.markdown("---")
# #     st.markdown('<div class="section-lbl">Conversations récentes</div>', unsafe_allow_html=True)

# #     convs = st.session_state.conversations
# #     for cid, conv in list(convs.items()):
# #         title     = conv.get("title", "Conversation")[:35]
# #         is_active = cid == st.session_state.active_conv
# #         label     = f"{'▶ ' if is_active else ''}{title}"
# #         if st.button(label, key=f"btn_conv_{cid}"):
# #             st.session_state.active_conv = cid
# #             st.rerun()

# #     st.markdown("---")

# #     if st.button("🗑️ Supprimer conversation"):
# #         cid = st.session_state.active_conv
# #         if cid and cid in convs:
# #             del st.session_state.conversations[cid]
# #             if MONGO_OK:
# #                 try:
# #                     db.chat_sessions.delete_one({"conv_id": cid})
# #                 except Exception:
# #                     pass
# #             st.session_state.active_conv = None
# #             st.rerun()

# #     st.markdown("---")
# #     st.markdown('<div class="section-lbl">Questions suggérées</div>', unsafe_allow_html=True)

# #     suggestions = [
# #         "Donne moi les stats globales",
# #         "Pourquoi ya des commentaires négatifs ?",
# #         "wach kayn bezzaf chikayat ?",
# #         "كم عدد التعليقات السلبية؟",
# #         "Analyse les tendances",
# #         "Exemples de commentaires négatifs",
# #         "ما هي أكثر المشاكل شيوعاً؟",
# #         "Compare positif vs négatif",
# #         "Affiche le graphique évolution",
# #         "Montre le camembert",
# #         "Top mots négatifs",
# #         "Exporte en CSV",
# #         "Tableau des statistiques",
# #     ]
# #     for sug in suggestions:
# #         if st.button(sug, key=f"sug_{sug}"):
# #             st.session_state["pending_question"] = sug
# #             st.rerun()

# #     # Export de la conversation active
# #     active_msgs = convs.get(st.session_state.active_conv, {}).get("messages", [])
# #     if active_msgs:
# #         st.markdown("---")
# #         st.download_button(
# #             "⬇️ Exporter JSON",
# #             data=json.dumps(active_msgs, ensure_ascii=False, indent=2, default=str),
# #             file_name=f"conv_{st.session_state.active_conv}.json",
# #             mime="application/json",
# #             use_container_width=True,
# #         )

# # # ──────────────────────────────────────────────────────────────────
# # # ZONE PRINCIPALE — CHAT
# # # ──────────────────────────────────────────────────────────────────
# # active_cid  = st.session_state.active_conv
# # active_conv = st.session_state.conversations.get(active_cid, {})
# # messages    = active_conv.get("messages", [])

# # col_title, col_info = st.columns([4, 1])
# # with col_title:
# #     st.markdown(f"""
# #     <div style="padding:.5rem 0 1rem;">
# #         <div style="font-size:18px;font-weight:600;color:#e8e6e0;">
# #             {active_conv.get('title','Conversation')}
# #         </div>
# #         <div style="font-size:11px;color:#4b5563;font-family:'IBM Plex Mono',monospace;">
# #             {len(messages)} messages · ⚡ {GROQ_MODEL} ·
# #             {'🟢 MongoDB' if MONGO_OK else '🔴 MongoDB'} ·
# #             {'🟢 Groq' if GROQ_OK else '🔴 Groq'}
# #         </div>
# #     </div>
# #     """, unsafe_allow_html=True)

# # # Affichage des messages
# # if not messages:
# #     st.markdown("""
# #     <div style="text-align:center;padding:4rem 2rem;color:#374151;">
# #         <div style="font-size:48px;margin-bottom:1rem;">⚡</div>
# #         <div style="font-size:18px;font-weight:600;color:#6b7280;margin-bottom:.5rem;">
# #             Comment puis-je vous aider ?
# #         </div>
# #         <div style="font-size:13px;color:#374151;font-family:'IBM Plex Mono',monospace;">
# #             Parlez en français · darija · عربي · réponse en 1-2 sec
# #         </div>
# #     </div>
# #     """, unsafe_allow_html=True)
# # else:
# #     for msg in messages:
# #         role    = msg["role"]
# #         content = msg["content"]
# #         lang    = msg.get("lang", "fr")
# #         ts      = msg.get("timestamp", "")[:16]
# #         badge   = LANG_BADGE.get(lang, "")

# #         if role == "user":
# #             safe = content.replace("<", "&lt;").replace(">", "&gt;")
# #             st.markdown(f"""
# #             <div class="msg-user"><div class="bubble">{safe}</div></div>
# #             <div class="msg-meta" style="text-align:right;padding-right:4px;">
# #                 {badge} {ts}
# #             </div>
# #             """, unsafe_allow_html=True)
# #         else:
# #             st.markdown(f"""
# #             <div class="msg-bot">
# #                 <div class="bot-icon">⚡</div>
# #                 <div>
# #                     <div class="bubble">{render_md(content)}</div>
# #                     <div class="msg-meta">{ts}</div>
# #                 </div>
# #             </div>
# #             """, unsafe_allow_html=True)

# # # ── INPUT BAR FIXE ────────────────────────────────────────────────
# # pending = st.session_state.pop("pending_question", None)

# # col_mic, col_file, col_input, col_send = st.columns([0.6, 0.6, 8, 0.8])

# # with col_mic:
# #     audio = mic_recorder(
# #         start_prompt="🎤",
# #         stop_prompt="⏹️",
# #         just_once=True,
# #         use_container_width=True,
# #         key="mic_recorder",
# #     )

# # with col_file:
# #     fichier = st.file_uploader(
# #         "",
# #         type=["txt", "csv", "json", "pdf"],
# #         label_visibility="collapsed",
# #         key="file_uploader",
# #     )
# #     if fichier:
# #         contenu = fichier.read().decode("utf-8", errors="ignore")[:2000]
# #         st.session_state["pending_question"] = (
# #             f"Analyse ce fichier ({fichier.name}) :\n\n{contenu}"
# #         )
# #         st.rerun()

# # with col_input:
# #     question_input = st.text_area(
# #         "",
# #         placeholder="Écris ton message en français, darija ou عربي…",
# #         label_visibility="collapsed",
# #         height=68,
# #         key="chat_input",
# #     )

# # with col_send:
# #     send = st.button("➤", use_container_width=True, key="send_btn")

# # # Traitement micro
# # if audio and "bytes" in audio:
# #     try:
# #         audio_bytes = io.BytesIO(audio["bytes"])
# #         sound       = AudioSegment.from_file(audio_bytes)
# #         sound.export("temp_audio.wav", format="wav")
# #         recognizer = sr.Recognizer()
# #         with sr.AudioFile("temp_audio.wav") as source:
# #             audio_data = recognizer.record(source)
# #             texte      = recognizer.recognize_google(audio_data, language="fr-FR")
# #             st.session_state["pending_question"] = texte
# #             st.rerun()
# #     except Exception as e:
# #         st.warning(f"Micro : {e}")

# # # Question finale
# # question = None
# # if send and question_input and question_input.strip():
# #     question = question_input.strip()
# # elif pending:
# #     question = pending

# # # ──────────────────────────────────────────────────────────────────
# # # TRAITEMENT DE LA QUESTION
# # # ──────────────────────────────────────────────────────────────────
# # if question:
# #     ts_now   = datetime.now().strftime("%Y-%m-%d %H:%M")
# #     lang_det = detecter_langue(question)

# #     # 1. Réponses directes (Group_ID ou date)
# #     rep_directe = repondre_question_specifique(question)
# #     if not rep_directe:
# #         rep_directe = repondre_question_par_date(question)

# #     # 2. Questions spéciales (graphiques, exports, tableaux)
# #     rep_speciale = None
# #     if not rep_directe:
# #         rep_speciale = traiter_question_speciale(question)

# #     # ── Cas 1 : réponse directe (Group_ID / date) ────────────────
# #     if rep_directe:
# #         messages.append({"role": "user",      "content": question,    "lang": lang_det, "timestamp": ts_now})
# #         messages.append({"role": "assistant", "content": rep_directe, "lang": "fr",     "timestamp": ts_now})
# #         sauvegarder_conv(active_cid, messages)
# #         st.rerun()

# # # ── Cas 2 : question spéciale (graphique / export / tableau) ─
# #     elif rep_speciale is not None:
# #         messages.append({"role": "user",      "content": question,     "lang": lang_det, "timestamp": ts_now})
# #         messages.append({"role": "assistant", "content": rep_speciale, "lang": "fr",     "timestamp": ts_now})
# #         sauvegarder_conv(active_cid, messages)
# #         # PAS de st.rerun() ici → le graphique déjà rendu par st.plotly_chart()
# #         # reste visible. Le rerun efface le rendu Plotly avant qu'il s'affiche.

# #     # ── Cas 3 : question graphique non gérée → bloquer GROQ ──────
# #     #    (sécurité supplémentaire : si est_question_graphique détecte
# #     #     un mot-clé non capturé par traiter_question_speciale)
# #     elif est_question_graphique(question):
# #         msg_err = (
# #             "❌ Je ne peux pas générer de graphiques directement. "
# #             "Essayez une formulation plus précise, par exemple :\n"
# #             "• 'Affiche le graphique évolution'\n"
# #             "• 'Montre le camembert'\n"
# #             "• 'Top mots négatifs'"
# #         )
# #         messages.append({"role": "user",      "content": question, "lang": lang_det, "timestamp": ts_now})
# #         messages.append({"role": "assistant", "content": msg_err,  "lang": "fr",     "timestamp": ts_now})
# #         sauvegarder_conv(active_cid, messages)
# #         st.rerun()

# #     # ── Cas 4 : question ordinaire → GROQ ────────────────────────
# #     else:
# #         messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})

# #         # Titre de la conversation (premier message)
# #         if len(messages) == 1:
# #             titre = question[:40] + ("…" if len(question) > 40 else "")
# #             st.session_state.conversations[active_cid]["title"] = titre
# #             if MONGO_OK:
# #                 try:
# #                     db.chat_sessions.update_one(
# #                         {"conv_id": active_cid},
# #                         {"$set": {"title": titre}},
# #                     )
# #                 except Exception:
# #                     pass

# #         sauvegarder_conv(active_cid, messages)
# #         st.rerun()

# # # ──────────────────────────────────────────────────────────────────
# # # GÉNÉRATION RÉPONSE GROQ (streaming)
# # # ──────────────────────────────────────────────────────────────────
# # if messages and messages[-1]["role"] == "user":
# #     # Sécurité finale : ne pas appeler GROQ si la dernière question
# #     # concerne un graphique (évite tout SVG généré par le modèle)
# #     derniere_question = messages[-1]["content"]
# #     if est_question_graphique(derniere_question):
# #         # La réponse a déjà été ajoutée dans le bloc précédent (cas 3)
# #         # On ne fait rien ici
# #         pass
# #     elif not GROQ_OK:
# #         st.error("""
# #         ⚠️ Clé Groq invalide ou manquante !
# #         1. Va sur https://console.groq.com
# #         2. Crée une clé API gratuite
# #         """)
# #     else:
# #         st.session_state.stop_generation = False

# #         if st.button("⏹️ Stop", key="stop_btn"):
# #             st.session_state.stop_generation = True

# #         placeholder = st.empty()
# #         placeholder.markdown("""
# #         <div class="msg-bot">
# #             <div class="bot-icon">⚡</div>
# #             <div class="bubble">
# #                 <div class="typing"><span></span><span></span><span></span></div>
# #             </div>
# #         </div>
# #         """, unsafe_allow_html=True)

# #         full_response = ""
# #         langue_user   = messages[-1].get("lang", "fr")

# #         for delta in appeler_groq_stream(messages[:-1], messages[-1]["content"], langue_user):
# #             full_response += delta
# #             placeholder.markdown(f"""
# #             <div class="msg-bot">
# #                 <div class="bot-icon">⚡</div>
# #                 <div class="bubble">{render_md(full_response)}▌</div>
# #             </div>
# #             """, unsafe_allow_html=True)

# #         placeholder.empty()
# #         messages.append({
# #             "role":      "assistant",
# #             "content":   full_response,
# #             "lang":      langue_user,
# #             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
# #         })
# #         sauvegarder_conv(active_cid, messages)
# #         st.rerun()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# """
# chatbot_groq.py — ChatBot Algérien avec GROQ (ultra rapide) + MongoDB
# Conversation naturelle en français, darija, arabe
# Analyse intelligente des commentaires télécom
# """

# import streamlit as st
# from groq import Groq
# from pymongo import MongoClient
# from datetime import datetime
# import re, json, os, uuid
# from langdetect import detect, LangDetectException
# from streamlit_mic_recorder import mic_recorder
# import speech_recognition as sr
# import io
# from pydub import AudioSegment
# import sys

# # ──────────────────────────────────────────────────────────────────
# # CONFIG
# # ──────────────────────────────────────────────────────────────────
# GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "gsk_")
# GROQ_MODEL   = "llama-3.1-8b-instant"
# MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://localhost:27018/")
# DB_NAME      = "telecom_algerie"

# groq_client = Groq(api_key=GROQ_API_KEY)

# # ──────────────────────────────────────────────────────────────────
# # IMPORT DES MODULES
# # ──────────────────────────────────────────────────────────────────
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from modules.graphiques        import *
# from modules.export            import *
# from modules.statistiques      import *
# from modules.analyse_textuelle import *

# # ──────────────────────────────────────────────────────────────────
# # PAGE CONFIG
# # ──────────────────────────────────────────────────────────────────
# st.set_page_config(
#     page_title="ChatBot Algérien — Groq ⚡",
#     page_icon="🤖",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

# # ── Session state defaults ────────────────────────────────────────
# for k, v in [("stop_generation", False), ("theme", "dark"), ("input_key", 0)]:
#     if k not in st.session_state:
#         st.session_state[k] = v

# # ──────────────────────────────────────────────────────────────────
# # THÈME dark / light
# # ──────────────────────────────────────────────────────────────────
# THEMES = {
#     "dark": {
#         "bg":"#0a0b0f","bg2":"#0d0e14","bg3":"#0f1117",
#         "border":"#1a1d2e","border2":"#2a2d3e",
#         "text":"#e8e6e0","text2":"#9ca3af","text3":"#4b5563",
#         "accent":"#f97316","accent2":"#eab308",
#         "user_bg":"#1a2540","user_border":"#2a3560","user_text":"#c8d3f5",
#         "bot_bg":"#0f1117","bot_border":"#1e2130",
#         "btn_hover":"rgba(249,115,22,.08)",
#         "icon":"🌙","icon_lbl":"Mode clair",
#     },
#     "light": {
#         "bg":"#f5f4f0","bg2":"#ffffff","bg3":"#eeece8",
#         "border":"#e0ddd5","border2":"#c8c5bc",
#         "text":"#1a1814","text2":"#6b6760","text3":"#9ca3af",
#         "accent":"#e85d04","accent2":"#ca8a04",
#         "user_bg":"#e8f0fe","user_border":"#c5d5fb","user_text":"#1e3a8a",
#         "bot_bg":"#ffffff","bot_border":"#e5e3de",
#         "btn_hover":"rgba(232,93,4,.06)",
#         "icon":"☀️","icon_lbl":"Mode sombre",
#     },
# }
# T = THEMES[st.session_state.theme]

# # ──────────────────────────────────────────────────────────────────
# # CSS
# # ──────────────────────────────────────────────────────────────────
# st.markdown(f"""
# <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap" rel="stylesheet">
# <style>
# html,body,[class*="css"]{{
#     font-family:'Syne',sans-serif!important;
#     background:{T['bg']}!important;
#     color:{T['text']}!important;
# }}
# [data-testid="stAppViewContainer"]{{background:{T['bg']}!important;}}
# [data-testid="stSidebar"]{{
#     background:{T['bg2']}!important;
#     border-right:1px solid {T['border']}!important;
# }}
# /* messages */
# .msg-user{{display:flex;justify-content:flex-end;margin:10px 0;}}
# .msg-user .bubble{{
#     background:{T['user_bg']};border:1px solid {T['user_border']};
#     border-radius:18px 18px 4px 18px;
#     padding:11px 16px;max-width:70%;
#     font-size:14px;line-height:1.65;color:{T['user_text']};
# }}
# .msg-bot{{display:flex;gap:10px;margin:10px 0;align-items:flex-start;}}
# .bot-icon{{
#     width:34px;height:34px;min-width:34px;
#     background:linear-gradient(135deg,{T['accent']},{T['accent2']});
#     border-radius:9px;display:flex;align-items:center;
#     justify-content:center;font-size:17px;margin-top:2px;
# }}
# .msg-bot .bubble{{
#     background:{T['bot_bg']};border:1px solid {T['bot_border']};
#     border-radius:4px 18px 18px 18px;
#     padding:12px 16px;max-width:75%;
#     font-size:14px;line-height:1.75;color:{T['text']};
# }}
# .msg-bot .bubble strong{{color:{T['accent']};}}
# .msg-bot .bubble code{{
#     background:{T['bg3']};border:1px solid {T['border2']};
#     border-radius:4px;padding:1px 6px;
#     font-family:'IBM Plex Mono',monospace;
#     font-size:12px;color:{T['accent2']};
# }}
# .msg-meta{{
#     font-size:10px;color:{T['text3']};
#     font-family:'IBM Plex Mono',monospace;
#     margin-top:4px;padding-left:44px;
# }}
# /* badges langue */
# .badge{{display:inline-block;padding:1px 8px;border-radius:20px;
#     font-size:10px;font-weight:600;font-family:'IBM Plex Mono',monospace;}}
# .b-fr{{background:rgba(59,130,246,.15);color:#60a5fa;border:1px solid rgba(59,130,246,.3);}}
# .b-ar{{background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.3);}}
# .b-dz{{background:rgba(249,115,22,.15);color:#fb923c;border:1px solid rgba(249,115,22,.3);}}
# /* sidebar */
# .sidebar-title{{font-size:19px;font-weight:700;color:{T['text']};}}
# .sidebar-sub{{font-size:10px;color:{T['text3']};font-family:'IBM Plex Mono',monospace;
#     letter-spacing:.08em;text-transform:uppercase;margin-bottom:1.2rem;}}
# .accent{{color:{T['accent']};}}
# .groq-badge{{
#     display:inline-block;
#     background:linear-gradient(135deg,{T['accent']},{T['accent2']});
#     color:#000;font-size:10px;font-weight:700;
#     padding:2px 8px;border-radius:20px;
#     font-family:'IBM Plex Mono',monospace;margin-left:6px;
# }}
# .section-lbl{{font-size:10px;color:{T['text3']};font-family:'IBM Plex Mono',monospace;
#     letter-spacing:.1em;text-transform:uppercase;padding:.8rem 0 .3rem;}}
# /* boutons sidebar */
# .stButton>button{{
#     background:transparent!important;
#     border:1px solid {T['border']}!important;
#     color:{T['text2']}!important;border-radius:8px!important;
#     font-family:'IBM Plex Mono',monospace!important;
#     font-size:12px!important;width:100%;
#     text-align:left!important;padding:8px 12px!important;
#     transition:all .2s!important;
# }}
# .stButton>button:hover{{
#     border-color:{T['accent']}!important;
#     color:{T['accent']}!important;
#     background:{T['btn_hover']}!important;
# }}
# /* typing */
# .typing{{display:flex;gap:5px;align-items:center;padding:6px 0;}}
# .typing span{{width:7px;height:7px;border-radius:50%;
#     background:{T['accent']};display:inline-block;animation:bounce 1.2s infinite;}}
# .typing span:nth-child(2){{animation-delay:.2s;}}
# .typing span:nth-child(3){{animation-delay:.4s;}}
# @keyframes bounce{{
#     0%,60%,100%{{transform:translateY(0);opacity:.4;}}
#     30%{{transform:translateY(-6px);opacity:1;}}
# }}
# /* espace bas pour la barre fixe */
# .main>div{{padding-bottom:160px!important;}}
# hr{{border-color:{T['border']}!important;margin:.6rem 0!important;}}
# ::-webkit-scrollbar{{width:3px;}}
# ::-webkit-scrollbar-track{{background:transparent;}}
# ::-webkit-scrollbar-thumb{{background:{T['border2']};border-radius:4px;}}
# /* text_area dans la barre */
# textarea{{
#     background:{T['bg3']}!important;
#     border:1px solid {T['border']}!important;
#     border-radius:12px!important;
#     color:{T['text']}!important;
#     font-family:'Syne',sans-serif!important;
# }}
# textarea:focus{{border-color:{T['accent']}!important;}}
# </style>
# """, unsafe_allow_html=True)

# # ──────────────────────────────────────────────────────────────────
# # MONGODB + GROQ CHECKS
# # ──────────────────────────────────────────────────────────────────
# @st.cache_resource(ttl=0)
# def get_db():
#     try:
#         c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
#         c.admin.command("ping")
#         return c[DB_NAME]
#     except Exception:
#         return None

# db       = get_db()
# MONGO_OK = db is not None

# def check_groq() -> bool:
#     try:
#         groq_client.models.list()
#         return True
#     except Exception:
#         return False

# GROQ_OK = check_groq()

# # ──────────────────────────────────────────────────────────────────
# # CONTEXTE MONGODB
# # ──────────────────────────────────────────────────────────────────
# def get_context_mongo() -> str:
#     if not MONGO_OK:
#         return "Base MongoDB non disponible."
#     try:
#         lignes = []
#         cols = db.list_collection_names()
#         lignes.append(f"Collections disponibles : {', '.join(cols)}")
#         if "dataset_unifie" in cols:
#             col      = db["dataset_unifie"]
#             total    = col.count_documents({})
#             neg      = col.count_documents({"label_final": "negatif"})
#             pos      = col.count_documents({"label_final": "positif"})
#             neu      = col.count_documents({"label_final": "neutre"})
#             conflits = col.count_documents({"conflit": True})
#             lignes.append(f"dataset_unifie : {total} commentaires")
#             lignes.append(f"Sentiments → négatif:{neg} | positif:{pos} | neutre:{neu}")
#             lignes.append(f"Conflits d'annotation : {conflits}")
#         return "\n".join(lignes)
#     except Exception as e:
#         return f"Erreur MongoDB : {e}"


# def repondre_question_specifique(question: str):
#     match = re.search(r'groupe[ _]?(\d+)', question, re.IGNORECASE)
#     if match and MONGO_OK:
#         group_id = f"groupe_{int(match.group(1)):04d}"
#         try:
#             doc = db["dataset_unifie"].find_one({"Group_ID": group_id})
#             if doc:
#                 texte = doc.get("Commentaire_Client_Original","Pas de texte")
#                 label = doc.get("label_final","?")
#                 return f"📝 **{group_id}** :\n> {texte}\n\n🏷️ Sentiment : **{label}**"
#             return f"❌ Aucun commentaire trouvé pour **{group_id}**."
#         except Exception as e:
#             return f"Erreur : {e}"
#     return None


# def repondre_question_par_date(question: str):
#     match = re.search(r'(\d{2}/\d{2}/\d{4})', question)
#     if match and MONGO_OK:
#         date_cherchee = match.group(1)
#         try:
#             col  = db["dataset_unifie"]
#             docs = list(col.find({"dates":{"$regex":date_cherchee}}).limit(5))
#             if docs:
#                 resultat = f"📝 **Commentaires du {date_cherchee}** :\n\n"
#                 for i, doc in enumerate(docs, 1):
#                     texte = doc.get("Commentaire_Client_Original","Pas de texte")
#                     label = doc.get("label_final","?")
#                     resultat += f"{i}. {texte}\n   🏷️ Sentiment : {label}\n\n"
#                 return resultat
#             return f"❌ Aucun commentaire trouvé pour la date **{date_cherchee}**."
#         except Exception as e:
#             return f"Erreur recherche : {e}"
#     return None


# def detecter_langue(texte: str) -> str:
#     if re.search(r'[\u0600-\u06FF]', texte):
#         return "ar"
#     if re.search(
#         r'\b[23789]\b|3likom|wach|chnou|bezzaf|machi|kayn|ndir|sahbi|slm|kifak|rahi|mazal',
#         texte.lower()
#     ):
#         return "dz"
#     try:
#         return "fr" if detect(texte) == "fr" else "dz"
#     except LangDetectException:
#         return "fr"


# LANG_MAPPING = {"fr":"français","ar":"arabe","dz":"darija algérienne"}
# LANG_BADGE   = {
#     "fr":'<span class="badge b-fr">FR</span>',
#     "ar":'<span class="badge b-ar">عر</span>',
#     "dz":'<span class="badge b-dz">DZ</span>',
# }

# # ──────────────────────────────────────────────────────────────────
# # SYSTEM PROMPT
# # ──────────────────────────────────────────────────────────────────
# def build_system_prompt(langue: str) -> str:
#     ctx = get_context_mongo()
#     return f"""Tu es un assistant analytique intelligent spécialisé dans l'analyse
# de commentaires clients des opérateurs télécom algériens (Djezzy, Mobilis, Ooredoo).
# Tu comprends et réponds parfaitement en français, en darija algérienne et en arabe standard.

# === DONNÉES RÉELLES DE LA BASE MONGODB ===
# {ctx}
# ==========================================

# RÈGLES IMPORTANTES :
# - Réponds OBLIGATOIREMENT en : {LANG_MAPPING.get(langue,'français')}
# - Si l'utilisateur parle darija → réponds en darija naturelle algérienne
# - Base tes analyses UNIQUEMENT sur les vraies données ci-dessus
# - Donne des chiffres précis, ne fabrique rien
# - Si une information manque, dis-le clairement
# - Formate tes réponses avec des listes et titres quand pertinent
# - NE génère JAMAIS de code SVG, HTML, XML, ni de graphiques dans ta réponse
# - NE génère JAMAIS de visualisation ou diagramme sous quelque forme que ce soit
# - Si l'utilisateur demande un graphique, réponds juste que le système l'affiche automatiquement
# """

# # ──────────────────────────────────────────────────────────────────
# # QUESTIONS SPÉCIALES
# # ──────────────────────────────────────────────────────────────────
# GRAPHIQUE_KEYWORDS = [
#     "graphique","courbe","camembert","diagramme","visuali",
#     "chart","plot","nuage de mots","évolution","evolution",
#     "tendance","top mots","mots fréquents","mots frequents",
#     "répartition","repartition","part des sentiments",
#     "mots négatifs","mots negatifs",
# ]

# def est_question_graphique(question: str) -> bool:
#     q = question.lower()
#     return any(kw in q for kw in GRAPHIQUE_KEYWORDS)


# def traiter_question_speciale(question: str):
#     q = question.lower()

#     # Graphique évolution
#     if any(k in q for k in [
#         "graphique évolution","graphique evolution",
#         "évolution","evolution","courbe","tendance graphique",
#     ]):
#         try:
#             fig = graphique_evolution_sentiments()
#         except Exception as e:
#             return f"❌ Erreur graphique évolution : {e}"
#         if fig:
#             st.plotly_chart(fig, use_container_width=True)
#             return "📊 Voici le graphique d'évolution des sentiments dans le temps."
#         return "❌ Impossible de générer ce graphique. Vérifiez le champ 'dates' dans dataset_unifie."

#     # Camembert
#     if any(k in q for k in ["camembert","répartition","repartition","part des sentiments"]):
#         try:
#             fig = graphique_repartition_sentiments()
#         except Exception as e:
#             return f"❌ Erreur camembert : {e}"
#         if fig:
#             st.plotly_chart(fig, use_container_width=True)
#             return "🥧 Voici la répartition des sentiments."
#         return "❌ Pas assez de données pour la répartition."

#     # Top mots / nuage
#     if any(k in q for k in ["top mots","mots fréquents","mots frequents","mots négatifs","mots negatifs"]):
#         if "nuage" in q:
#             try:
#                 nuage = generer_nuage_mots()
#             except Exception as e:
#                 return f"❌ Erreur nuage de mots : {e}"
#             if nuage:
#                 st.markdown(nuage, unsafe_allow_html=True)
#                 return "☁️ Voici le nuage de mots des commentaires négatifs."
#             return "❌ Pas assez de données pour le nuage de mots."
#         else:
#             try:
#                 fig = graphique_top_mots()
#             except Exception as e:
#                 return f"❌ Erreur top mots : {e}"
#             if fig:
#                 st.plotly_chart(fig, use_container_width=True)
#                 return "📊 Voici les mots les plus fréquents dans les commentaires négatifs."
#             return "❌ Pas assez de données pour ce graphique."

#     # Export
#     if any(k in q for k in ["exporte","télécharge","telecharge","csv","excel","json"]):
#         if "csv"   in q: return exporter_csv(500)   or "❌ Erreur export CSV."
#         if "excel" in q: return exporter_excel(500) or "❌ Erreur export Excel."
#         if "json"  in q: return exporter_json(500)  or "❌ Erreur export JSON."
#         return "❌ Précise le format : csv, excel ou json."

#     # Tableau stats
#     if "tableau" in q and any(k in q for k in ["stat","statistique"]):
#         try:
#             tableau = generer_tableau_html()
#         except Exception as e:
#             return f"❌ Erreur tableau : {e}"
#         if tableau:
#             st.markdown(tableau, unsafe_allow_html=True)
#             return "📋 Voici le tableau des statistiques."
#         return "❌ Pas assez de données pour le tableau."

#     return None

# # ──────────────────────────────────────────────────────────────────
# # GROQ STREAMING
# # ──────────────────────────────────────────────────────────────────
# def appeler_groq_stream(messages_hist: list, question: str, langue: str):
#     msgs = [{"role":"system","content":build_system_prompt(langue)}]
#     for m in messages_hist[-20:]:
#         msgs.append({"role":m["role"],"content":m["content"]})
#     msgs.append({"role":"user","content":question})
#     try:
#         stream = groq_client.chat.completions.create(
#             model=GROQ_MODEL, messages=msgs, stream=True,
#             temperature=0.7, max_tokens=512, top_p=0.9,
#         )
#         for chunk in stream:
#             if st.session_state.stop_generation:
#                 st.session_state.stop_generation = False
#                 yield "\n\n⏹️ Génération arrêtée."
#                 break
#             delta = chunk.choices[0].delta.content
#             if delta:
#                 yield delta
#     except Exception as e:
#         yield f"\n⚠️ Erreur Groq : {e}\n"


# def render_md(txt: str) -> str:
#     txt = txt.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
#     txt = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', txt)
#     txt = re.sub(r'\*(.*?)\*',     r'<em>\1</em>',         txt)
#     txt = re.sub(r'`(.*?)`',       r'<code>\1</code>',     txt)
#     txt = txt.replace("\n\n","<br><br>").replace("\n","<br>")
#     txt = re.sub(r'<br>[-•] ','<br>• ',txt)
#     return txt

# # ──────────────────────────────────────────────────────────────────
# # CONVERSATIONS MONGO
# # ──────────────────────────────────────────────────────────────────
# def charger_convs_mongo() -> dict:
#     if not MONGO_OK:
#         return {}
#     try:
#         docs = list(db.chat_sessions.find({},{"_id":0}).sort("updated_at",-1).limit(30))
#         return {
#             d["conv_id"]:{
#                 "title":    d.get("title","Conversation"),
#                 "messages": d.get("messages",[]),
#                 "created_at": str(d.get("created_at","")),
#             }
#             for d in docs
#         }
#     except Exception:
#         return {}


# def sauvegarder_conv(cid, messages):
#     st.session_state.conversations[cid]["messages"] = messages
#     if MONGO_OK:
#         try:
#             db.chat_sessions.update_one(
#                 {"conv_id":cid},
#                 {"$set":{
#                     "messages":   messages,
#                     "updated_at": datetime.now(),
#                     "title":      st.session_state.conversations[cid].get("title",""),
#                 }},
#                 upsert=True,
#             )
#         except Exception:
#             pass


# def nouvelle_conversation():
#     cid = str(uuid.uuid4())[:8]
#     st.session_state.conversations[cid] = {
#         "title":"Nouvelle conversation",
#         "messages":[],
#         "created_at":datetime.now().isoformat(),
#     }
#     st.session_state.active_conv = cid
#     if MONGO_OK:
#         try:
#             db.chat_sessions.insert_one({
#                 "conv_id":cid,"title":"Nouvelle conversation",
#                 "messages":[],"created_at":datetime.now(),"updated_at":datetime.now(),
#             })
#         except Exception:
#             pass
#     return cid

# # ──────────────────────────────────────────────────────────────────
# # SESSION STATE — conversations
# # ──────────────────────────────────────────────────────────────────
# if "conversations" not in st.session_state:
#     st.session_state.conversations = charger_convs_mongo()
# if "active_conv" not in st.session_state:
#     st.session_state.active_conv = None

# if (
#     not st.session_state.active_conv
#     or st.session_state.active_conv not in st.session_state.conversations
# ):
#     if st.session_state.conversations:
#         st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
#     else:
#         nouvelle_conversation()

# # ──────────────────────────────────────────────────────────────────
# # SIDEBAR
# # ──────────────────────────────────────────────────────────────────
# with st.sidebar:
#     st.markdown(f"""
#     <div class="sidebar-title">
#         Chat<span class="accent">Bot</span>
#         <span class="groq-badge">⚡ GROQ</span>
#     </div>
#     <div class="sidebar-sub">{GROQ_MODEL} · Télécom DZ</div>
#     """, unsafe_allow_html=True)

#     if st.button("＋ Nouvelle conversation"):
#         nouvelle_conversation()
#         st.rerun()

#     st.markdown("---")

#     # Statuts
#     col_m, col_g = st.columns(2)
#     with col_m:
#         if MONGO_OK:
#             st.success("MongoDB ✓")
#         else:
#             st.error("MongoDB ✗")
#     with col_g:
#         if GROQ_OK:
#             st.success("Groq ✓")
#         else:
#             st.error("Groq ✗")

#     # ── Toggle dark / light ───────────────────────────────────────
#     st.markdown("---")
#     col_ic, col_btn = st.columns([1, 3])
#     with col_ic:
#         st.markdown(
#             f"<div style='font-size:20px;padding-top:4px;'>{T['icon']}</div>",
#             unsafe_allow_html=True,
#         )
#     with col_btn:
#         if st.button(T["icon_lbl"], key="theme_toggle_btn"):
#             st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"
#             st.rerun()

#     st.markdown("---")
#     st.markdown('<div class="section-lbl">Conversations récentes</div>', unsafe_allow_html=True)

#     convs = st.session_state.conversations
#     for cid, conv in list(convs.items()):
#         title     = conv.get("title","Conversation")[:35]
#         is_active = cid == st.session_state.active_conv
#         label     = f"{'▶ ' if is_active else ''}{title}"
#         if st.button(label, key=f"btn_conv_{cid}"):
#             st.session_state.active_conv = cid
#             st.rerun()

#     st.markdown("---")
#     if st.button("🗑️ Supprimer conversation"):
#         cid = st.session_state.active_conv
#         if cid and cid in convs:
#             del st.session_state.conversations[cid]
#             if MONGO_OK:
#                 try:
#                     db.chat_sessions.delete_one({"conv_id":cid})
#                 except Exception:
#                     pass
#             st.session_state.active_conv = None
#             st.rerun()

#     st.markdown("---")
#     st.markdown('<div class="section-lbl">Questions suggérées</div>', unsafe_allow_html=True)
#     suggestions = [
#         "Donne moi les stats globales",
#         "Pourquoi ya des commentaires négatifs ?",
#         "wach kayn bezzaf chikayat ?",
#         "كم عدد التعليقات السلبية؟",
#         "Analyse les tendances",
#         "Exemples de commentaires négatifs",
#         "ما هي أكثر المشاكل شيوعاً؟",
#         "Compare positif vs négatif",
#         "Affiche le graphique évolution",
#         "Montre le camembert",
#         "Top mots négatifs",
#         "Exporte en CSV",
#         "Tableau des statistiques",
#     ]
#     for sug in suggestions:
#         if st.button(sug, key=f"sug_{sug}"):
#             st.session_state["pending_question"] = sug
#             st.rerun()

#     active_msgs = convs.get(st.session_state.active_conv, {}).get("messages", [])
#     if active_msgs:
#         st.markdown("---")
#         st.download_button(
#             "⬇️ Exporter JSON",
#             data=json.dumps(active_msgs, ensure_ascii=False, indent=2, default=str),
#             file_name=f"conv_{st.session_state.active_conv}.json",
#             mime="application/json",
#             use_container_width=True,
#         )

# # ──────────────────────────────────────────────────────────────────
# # ZONE PRINCIPALE
# # ──────────────────────────────────────────────────────────────────
# active_cid  = st.session_state.active_conv
# active_conv = st.session_state.conversations.get(active_cid, {})
# messages    = active_conv.get("messages", [])

# # En-tête
# mongo_dot = f"<span style='color:#22c55e'>●</span> MongoDB" if MONGO_OK \
#             else f"<span style='color:#ef4444'>●</span> MongoDB"
# groq_dot  = f"<span style='color:#22c55e'>●</span> Groq" if GROQ_OK \
#             else f"<span style='color:#ef4444'>●</span> Groq"

# st.markdown(f"""
# <div style="padding:.4rem 0 .8rem;display:flex;align-items:center;justify-content:space-between;">
#     <div>
#         <div style="font-size:17px;font-weight:600;color:{T['text']};">
#             {active_conv.get('title','Conversation')}
#         </div>
#         <div style="font-size:10px;color:{T['text3']};font-family:'IBM Plex Mono',monospace;margin-top:2px;">
#             {len(messages)} messages · ⚡ {GROQ_MODEL}
#         </div>
#     </div>
#     <div style="font-size:11px;color:{T['text3']};font-family:'IBM Plex Mono',monospace;display:flex;gap:10px;">
#         {mongo_dot} &nbsp; {groq_dot}
#     </div>
# </div>
# """, unsafe_allow_html=True)

# # ── Messages ──────────────────────────────────────────────────────
# if not messages:
#     st.markdown(f"""
#     <div style="text-align:center;padding:3rem 2rem;color:{T['text3']};">
#         <div style="font-size:44px;margin-bottom:1rem;">⚡</div>
#         <div style="font-size:17px;font-weight:600;color:{T['text2']};margin-bottom:.4rem;">
#             Comment puis-je vous aider ?
#         </div>
#         <div style="font-size:12px;color:{T['text3']};font-family:'IBM Plex Mono',monospace;">
#             Parlez en français · darija · عربي · réponse en 1-2 sec
#         </div>
#     </div>
#     """, unsafe_allow_html=True)
# else:
#     for msg in messages:
#         role    = msg["role"]
#         content = msg["content"]
#         lang    = msg.get("lang","fr")
#         ts      = msg.get("timestamp","")[:16]
#         badge   = LANG_BADGE.get(lang,"")

#         if role == "user":
#             safe = content.replace("<","&lt;").replace(">","&gt;")
#             st.markdown(f"""
#             <div class="msg-user"><div class="bubble">{safe}</div></div>
#             <div class="msg-meta" style="text-align:right;padding-right:4px;">
#                 {badge} {ts}
#             </div>
#             """, unsafe_allow_html=True)
#         else:
#             st.markdown(f"""
#             <div class="msg-bot">
#                 <div class="bot-icon">⚡</div>
#                 <div>
#                     <div class="bubble">{render_md(content)}</div>
#                     <div class="msg-meta">{ts}</div>
#                 </div>
#             </div>
#             """, unsafe_allow_html=True)

# # ──────────────────────────────────────────────────────────────────
# # BARRE D'INPUT FIXE EN BAS
# # ──────────────────────────────────────────────────────────────────
# pending = st.session_state.pop("pending_question", None)

# # Conteneur fixe CSS
# st.markdown(f"""
# <div style="
#     position:fixed;bottom:0;left:0;right:0;
#     background:{T['bg2']};
#     border-top:1px solid {T['border']};
#     padding:10px 16px 8px;
#     z-index:9999;
# ">
#   <div id="input-placeholder"></div>
# </div>
# """, unsafe_allow_html=True)

# # Widgets Streamlit réels (seront stylisés pour paraître dans la barre)
# with st.container():
#     # Ligne 1 : micro + textarea + envoyer
#     c_mic, c_area, c_send, c_stop = st.columns([0.5, 8, 1, 0.7])

#     with c_mic:
#         audio = mic_recorder(
#             start_prompt="🎤",
#             stop_prompt="⏹️",
#             just_once=True,
#             use_container_width=True,
#             key="mic_recorder",
#         )

#     with c_area:
#         question_input = st.text_area(
#             "",
#             placeholder="Écris ton message en français, darija ou عربي…",
#             label_visibility="collapsed",
#             height=60,
#             key=f"chat_input_{st.session_state.input_key}",
#         )

#     with c_send:
#         send = st.button("➤", use_container_width=True, key="send_btn")

#     with c_stop:
#         if st.button("⏹", use_container_width=True, key="stop_btn"):
#             st.session_state.stop_generation = True

#     # Ligne 2 : file uploader + chips rapides
#     c_file, c_h1, c_h2, c_h3, c_h4 = st.columns([1.5, 2, 2, 2, 2])

#     with c_file:
#         fichier = st.file_uploader(
#             "📎",
#             type=["txt","csv","json","pdf"],
#             label_visibility="collapsed",
#             key=f"file_uploader_{st.session_state.input_key}",
#         )
#         if fichier:
#             contenu = fichier.read().decode("utf-8", errors="ignore")[:2000]
#             st.session_state["pending_question"] = f"Analyse ce fichier ({fichier.name}) :\n\n{contenu}"
#             st.session_state.input_key += 1
#             st.rerun()

#     hints = [
#         ("Analyse les tendances", c_h1),
#         ("wach kayn bezzaf ?", c_h2),
#         ("Compare pos/nég", c_h3),
#         ("Top mots négatifs", c_h4),
#     ]
#     for hint_txt, col in hints:
#         with col:
#             if st.button(hint_txt, key=f"hint_{hint_txt}"):
#                 st.session_state["pending_question"] = hint_txt
#                 st.rerun()

# # Traitement micro
# if audio and "bytes" in audio:
#     try:
#         audio_bytes = io.BytesIO(audio["bytes"])
#         sound       = AudioSegment.from_file(audio_bytes)
#         sound.export("temp_audio.wav", format="wav")
#         recognizer  = sr.Recognizer()
#         with sr.AudioFile("temp_audio.wav") as source:
#             audio_data = recognizer.record(source)
#             texte      = recognizer.recognize_google(audio_data, language="fr-FR")
#             st.session_state["pending_question"] = texte
#             st.rerun()
#     except Exception as e:
#         st.warning(f"Micro : {e}")

# # Question finale
# question = None
# if send and question_input and question_input.strip():
#     question = question_input.strip()
# elif pending:
#     question = pending

# # ──────────────────────────────────────────────────────────────────
# # TRAITEMENT DE LA QUESTION
# # ──────────────────────────────────────────────────────────────────
# if question:
#     ts_now   = datetime.now().strftime("%Y-%m-%d %H:%M")
#     lang_det = detecter_langue(question)

#     # Vider la barre d'input (clé dynamique)
#     st.session_state.input_key += 1

#     # Réponses directes
#     rep_directe = repondre_question_specifique(question)
#     if not rep_directe:
#         rep_directe = repondre_question_par_date(question)

#     # Questions spéciales
#     rep_speciale = None
#     if not rep_directe:
#         rep_speciale = traiter_question_speciale(question)

#     # Cas 1 : réponse directe
#     if rep_directe:
#         messages.append({"role":"user",      "content":question,    "lang":lang_det,"timestamp":ts_now})
#         messages.append({"role":"assistant", "content":rep_directe, "lang":"fr",    "timestamp":ts_now})
#         sauvegarder_conv(active_cid, messages)
#         st.rerun()

#     # Cas 2 : graphique / export / tableau
#     elif rep_speciale is not None:
#         messages.append({"role":"user",      "content":question,     "lang":lang_det,"timestamp":ts_now})
#         messages.append({"role":"assistant", "content":rep_speciale, "lang":"fr",    "timestamp":ts_now})
#         sauvegarder_conv(active_cid, messages)
#         # PAS de rerun → le graphique Plotly reste affiché

#     # Cas 3 : mot-clé graphique non géré → bloquer GROQ
#     elif est_question_graphique(question):
#         msg_err = (
#             "❌ Je ne peux pas générer de graphiques directement.\n"
#             "Essayez :\n"
#             "• 'Affiche le graphique évolution'\n"
#             "• 'Montre le camembert'\n"
#             "• 'Top mots négatifs'"
#         )
#         messages.append({"role":"user",      "content":question, "lang":lang_det,"timestamp":ts_now})
#         messages.append({"role":"assistant", "content":msg_err,  "lang":"fr",    "timestamp":ts_now})
#         sauvegarder_conv(active_cid, messages)
#         st.rerun()

#     # Cas 4 : GROQ
#     else:
#         messages.append({"role":"user","content":question,"lang":lang_det,"timestamp":ts_now})
#         if len(messages) == 1:
#             titre = question[:40] + ("…" if len(question) > 40 else "")
#             st.session_state.conversations[active_cid]["title"] = titre
#             if MONGO_OK:
#                 try:
#                     db.chat_sessions.update_one(
#                         {"conv_id":active_cid},{"$set":{"title":titre}}
#                     )
#                 except Exception:
#                     pass
#         sauvegarder_conv(active_cid, messages)
#         st.rerun()

# # ──────────────────────────────────────────────────────────────────
# # GÉNÉRATION GROQ (streaming)
# # ──────────────────────────────────────────────────────────────────
# if messages and messages[-1]["role"] == "user":
#     derniere_question = messages[-1]["content"]
#     if est_question_graphique(derniere_question):
#         pass  # bloqué
#     elif not GROQ_OK:
#         st.error("⚠️ Clé Groq invalide. Va sur https://console.groq.com pour en créer une.")
#     else:
#         placeholder = st.empty()
#         placeholder.markdown(f"""
#         <div class="msg-bot">
#             <div class="bot-icon">⚡</div>
#             <div class="bubble">
#                 <div class="typing"><span></span><span></span><span></span></div>
#             </div>
#         </div>
#         """, unsafe_allow_html=True)

#         full_response = ""
#         langue_user   = messages[-1].get("lang","fr")

#         for delta in appeler_groq_stream(messages[:-1], messages[-1]["content"], langue_user):
#             full_response += delta
#             placeholder.markdown(f"""
#             <div class="msg-bot">
#                 <div class="bot-icon">⚡</div>
#                 <div class="bubble">{render_md(full_response)}▌</div>
#             </div>
#             """, unsafe_allow_html=True)

#         placeholder.empty()
#         messages.append({
#             "role":      "assistant",
#             "content":   full_response,
#             "lang":      langue_user,
#             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
#         })
#         sauvegarder_conv(active_cid, messages)
#         st.rerun()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# chatbot_groq.py
import streamlit as st
from groq import Groq
from pymongo import MongoClient
from datetime import datetime
import re, json, os, uuid
from langdetect import detect, LangDetectException
from streamlit_mic_recorder import mic_recorder
import speech_recognition as sr
import io
from pydub import AudioSegment
import sys

# Import de la sidebar commune et du style global
from sidebar import render_sidebar
from style import load_fontawesome, MAIN_CSS   # votre fichier style.py

# Configuration de la page (UNE SEULE FOIS)
st.set_page_config(
    page_title="Assistant Télécom Algérie",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ========== SIDEBAR PRINCIPALE (MENU) ==========
render_sidebar()   # Affiche le menu, profil, logout

# ========== STYLE SUPPLÉMENTAIRE POUR LA COLONNE DROITE (simulée) ==========
RIGHT_SIDEBAR_CSS = """
<style>
    /* Ajustement pour la barre d'input fixe */
    .main .block-container {
        padding-bottom: 160px !important;
    }
    
    /* Colonne droite (simule une seconde sidebar) */
    .right-sidebar {
        background: #f8fafc;
        border-left: 1px solid #e2e8f0;
        padding: 1rem 1rem 1rem 1.5rem;
        height: 100%;
        border-radius: 0;
    }
    .right-sidebar .section-title {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #475569;
        margin: 1rem 0 0.5rem 0;
        font-weight: 600;
    }
    .right-sidebar hr {
        margin: 0.8rem 0;
    }
    .status-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 8px;
    }
    .status-ok { background-color: #10b981; }
    .status-ko { background-color: #ef4444; }
    
    /* Messages (inchangés) */
    .msg-user { display: flex; justify-content: flex-end; margin: 12px 0; }
    .msg-user .bubble {
        background: #e0f2fe; border: 1px solid #bae6fd;
        border-radius: 20px 20px 4px 20px; padding: 10px 16px;
        max-width: 75%; font-size: 0.9rem; color: #0c4a6e;
    }
    .msg-bot { display: flex; gap: 12px; margin: 12px 0; align-items: flex-start; }
    .bot-icon {
        width: 36px; height: 36px; background: linear-gradient(135deg, #0f3b5c, #2dd4bf);
        border-radius: 12px; display: flex; align-items: center; justify-content: center;
        color: white;
    }
    .msg-bot .bubble {
        background: white; border: 1px solid #e2e8f0;
        border-radius: 4px 20px 20px 20px; padding: 10px 16px;
        max-width: 75%; font-size: 0.9rem; color: #1e293b;
    }
    .badge { display: inline-block; padding: 1px 8px; border-radius: 30px; font-size: 0.7rem; font-weight: 600; }
    .badge-fr { background: #dbeafe; color: #1e40af; }
    .badge-ar { background: #fef3c7; color: #b45309; }
    .badge-dz { background: #ffedd5; color: #c2410c; }
    
    /* Barre d'input fixe */
    .fixed-input-bar {
        position: fixed; bottom: 0; left: 0; right: 0;
        background: white; border-top: 1px solid #e2e8f0;
        padding: 12px 20px; z-index: 1000; box-shadow: 0 -2px 10px rgba(0,0,0,0.03);
    }
    .typing span {
        display: inline-block; width: 8px; height: 8px;
        background: #2dd4bf; border-radius: 50%;
        animation: bounce 1.2s infinite; margin-right: 4px;
    }
    @keyframes bounce { 0%,60%,100% { transform: translateY(0); opacity: 0.4; } 30% { transform: translateY(-6px); opacity: 1; } }
    
    /* Boutons dans la colonne droite */
    .right-sidebar .stButton button {
        width: 100%;
        text-align: left;
        background: transparent;
        border: none;
        color: #1e293b;
        font-size: 0.8rem;
        padding: 6px 8px;
    }
    .right-sidebar .stButton button:hover {
        background: rgba(0,0,0,0.05);
        border-radius: 8px;
    }
</style>
"""
st.markdown(RIGHT_SIDEBAR_CSS, unsafe_allow_html=True)

# ========== CONFIGURATION GROQ & MONGODB ==========
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "gsk_")
GROQ_MODEL   = "llama-3.1-8b-instant"
MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://localhost:27018/")
DB_NAME      = "telecom_algerie"

groq_client = Groq(api_key=GROQ_API_KEY)

# Import de vos modules personnalisés (graphiques, export, stats, analyse)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from modules.graphiques        import *
from modules.export            import *
from modules.statistiques      import *
from modules.analyse_textuelle import *

# Initialisation session state
if "stop_generation" not in st.session_state:
    st.session_state.stop_generation = False
if "input_key" not in st.session_state:
    st.session_state.input_key = 0

# ========== FONCTIONS (MONGODB, GROQ, REPONSES, ETC.) ==========
# (Recopiez ici toutes vos fonctions existantes, sans modification)
# get_db, check_groq, get_context_mongo, repondre_question_specifique,
# repondre_question_par_date, detecter_langue, build_system_prompt,
# est_question_graphique, traiter_question_speciale, appeler_groq_stream,
# render_md, charger_convs_mongo, sauvegarder_conv, nouvelle_conversation, etc.

# Exemple : je les résume, mais vous devez les copier depuis votre code original.
# Assurez-vous qu'elles utilisent les variables globales (db, groq_client, etc.)

# ... (copiez ici tout le code de votre version originale, à partir de la ligne
# "MONGODB + GROQ CHECKS" jusqu'à la fin des définitions de fonctions)
# ...

# ========== VÉRIFICATION DES SERVICES ==========
@st.cache_resource(ttl=0)
def get_db():
    try:
        c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        c.admin.command("ping")
        return c[DB_NAME]
    except Exception:
        return None

db = get_db()
MONGO_OK = db is not None

def check_groq() -> bool:
    try:
        groq_client.models.list()
        return True
    except Exception:
        return False

GROQ_OK = check_groq()

# ... (imports, config, sidebar, etc.)

# ========== DÉFINITIONS DES FONCTIONS (TOUTES AVANT LEUR UTILISATION) ==========
# (Commencez par toutes vos fonctions, y compris charger_convs_mongo, sauvegarder_conv, nouvelle_conversation)

def charger_convs_mongo() -> dict:
    if not MONGO_OK:
        return {}
    try:
        docs = list(db.chat_sessions.find({}, {"_id": 0}).sort("updated_at", -1).limit(30))
        return {
            d["conv_id"]: {
                "title": d.get("title", "Conversation"),
                "messages": d.get("messages", []),
                "created_at": str(d.get("created_at", "")),
            }
            for d in docs
        }
    except Exception:
        return {}

def sauvegarder_conv(cid, messages):
    st.session_state.conversations[cid]["messages"] = messages
    if MONGO_OK:
        try:
            db.chat_sessions.update_one(
                {"conv_id": cid},
                {"$set": {
                    "messages": messages,
                    "updated_at": datetime.now(),
                    "title": st.session_state.conversations[cid].get("title", ""),
                }},
                upsert=True,
            )
        except Exception:
            pass

def nouvelle_conversation():
    cid = str(uuid.uuid4())[:8]
    st.session_state.conversations[cid] = {
        "title": "Nouvelle conversation",
        "messages": [],
        "created_at": datetime.now().isoformat(),
    }
    st.session_state.active_conv = cid
    if MONGO_OK:
        try:
            db.chat_sessions.insert_one({
                "conv_id": cid, "title": "Nouvelle conversation",
                "messages": [], "created_at": datetime.now(), "updated_at": datetime.now(),
            })
        except Exception:
            pass
    return cid

# ... (toutes les autres fonctions : get_context_mongo, repondre_question_specifique, etc.)

# ========== INITIALISATION SESSION STATE (après toutes les définitions) ==========
if "conversations" not in st.session_state:
    st.session_state.conversations = charger_convs_mongo()   # ← maintenant définie
if "active_conv" not in st.session_state:
    st.session_state.active_conv = None

if not st.session_state.active_conv or st.session_state.active_conv not in st.session_state.conversations:
    if st.session_state.conversations:
        st.session_state.active_conv = list(st.session_state.conversations.keys())[0]
    else:
        nouvelle_conversation()
# ========== MISE EN PAGE PRINCIPALE : CHAT (colonne gauche) + SIDEBAR DROITE ==========
col_chat, col_right = st.columns([3, 1])  # 3/4 pour le chat, 1/4 pour la seconde sidebar

# ------------------ COLONNE GAUCHE (CHAT) ------------------
with col_chat:
    active_cid = st.session_state.active_conv
    active_conv = st.session_state.conversations.get(active_cid, {})
    messages = active_conv.get("messages", [])

    # En-tête du chat
    st.markdown(f"""
    <div class="chat-header">
        <div>
            <div class="chat-title">
                <i class="fas fa-comments"></i> {active_conv.get('title', 'Conversation')}
            </div>
            <div class="chat-stats">{len(messages)} message(s) · Modèle {GROQ_MODEL}</div>
        </div>
        <div class="chat-stats">
            <i class="fas fa-microchip"></i> GROQ · {GROQ_MODEL}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Affichage des messages
    if not messages:
        st.markdown("""
        <div style="text-align: center; padding: 3rem 1rem; color: #64748b;">
            <i class="fas fa-robot" style="font-size: 3rem; margin-bottom: 1rem; color: #2dd4bf;"></i>
            <div style="font-size: 1.1rem; font-weight: 500;">Comment puis-je vous aider ?</div>
            <div style="font-size: 0.8rem; margin-top: 0.5rem;">
                Posez une question en français, darija ou arabe
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        for msg in messages:
            role = msg["role"]
            content = msg["content"]
            lang = msg.get("lang", "fr")
            ts = msg.get("timestamp", "")[:16]
            badge = LANG_BADGE.get(lang, "")

            if role == "user":
                safe = content.replace("<", "&lt;").replace(">", "&gt;")
                st.markdown(f"""
                <div class="msg-user"><div class="bubble">{safe}</div></div>
                <div class="msg-meta" style="text-align: right;">{badge} {ts}</div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="msg-bot">
                    <div class="bot-icon"><i class="fas fa-robot"></i></div>
                    <div>
                        <div class="bubble">{render_md(content)}</div>
                        <div class="msg-meta">{ts}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

# ------------------ COLONNE DROITE (SECONDE SIDEBAR) ------------------
with col_right:
    st.markdown('<div class="right-sidebar">', unsafe_allow_html=True)
    
    # Bouton Nouvelle conversation
    if st.button("➕ Nouvelle conversation", use_container_width=True):
        nouvelle_conversation()
        st.rerun()
    
    st.markdown("---")
    st.markdown('<div class="section-title">📊 STATUT DES SERVICES</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        if MONGO_OK:
            st.markdown('<span class="status-dot status-ok"></span> MongoDB', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-dot status-ko"></span> MongoDB', unsafe_allow_html=True)
    with col2:
        if GROQ_OK:
            st.markdown('<span class="status-dot status-ok"></span> Groq', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-dot status-ko"></span> Groq', unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown('<div class="section-title">💬 CONVERSATIONS RÉCENTES</div>', unsafe_allow_html=True)
    convs = st.session_state.conversations
    for cid, conv in list(convs.items()):
        title = conv.get("title", "Conversation")[:35]
        is_active = cid == st.session_state.active_conv
        label = f"{'▶ ' if is_active else ''}{title}"
        if st.button(label, key=f"btn_conv_{cid}", use_container_width=True):
            st.session_state.active_conv = cid
            st.rerun()
    
    if st.button("🗑️ Supprimer la conversation", use_container_width=True):
        cid = st.session_state.active_conv
        if cid and cid in convs:
            del st.session_state.conversations[cid]
            if MONGO_OK:
                try:
                    db.chat_sessions.delete_one({"conv_id": cid})
                except Exception:
                    pass
            st.session_state.active_conv = None
            st.rerun()
    
    st.markdown("---")
    st.markdown('<div class="section-title">❓ QUESTIONS SUGGÉRÉES</div>', unsafe_allow_html=True)
    suggestions = [
        "Donne moi les stats globales",
        "Pourquoi ya des commentaires négatifs ?",
        "wach kayn bezzaf chikayat ?",
        "كم عدد التعليقات السلبية؟",
        "Analyse les tendances",
        "Exemples de commentaires négatifs",
        "ما هي أكثر المشاكل شيوعاً؟",
        "Compare positif vs négatif",
        "Affiche le graphique évolution",
        "Montre le camembert",
        "Top mots négatifs",
        "Exporte en CSV",
        "Tableau des statistiques",
    ]
    for sug in suggestions:
        if st.button(sug, key=f"sug_{sug}", use_container_width=True):
            st.session_state["pending_question"] = sug
            st.rerun()
    
    # Export JSON de la conversation active
    active_msgs = convs.get(st.session_state.active_conv, {}).get("messages", [])
    if active_msgs:
        st.markdown("---")
        st.download_button(
            "📥 Exporter JSON",
            data=json.dumps(active_msgs, ensure_ascii=False, indent=2, default=str),
            file_name=f"conv_{st.session_state.active_conv}.json",
            mime="application/json",
            use_container_width=True,
        )
    
    st.markdown("---")
    st.markdown('<div class="footer">GROQ · LLaMA 3.1<br>Algérie Télécom</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)  # fin right-sidebar

# ========== BARRE D'INPUT FIXE (micro, zone texte, envoi, stop) ==========
# (Cette barre reste en bas, commune à toute la page, et non dans une colonne)
pending = st.session_state.pop("pending_question", None)

with st.container():
    st.markdown('<div class="fixed-input-bar">', unsafe_allow_html=True)
    cols = st.columns([0.5, 7, 1, 0.7])
    with cols[0]:
        audio = mic_recorder(
            start_prompt='<i class="fas fa-microphone"></i>',
            stop_prompt='<i class="fas fa-stop"></i>',
            just_once=True,
            use_container_width=True,
            key="mic_recorder",
        )
    with cols[1]:
        question_input = st.text_area(
            "",
            placeholder="Écrivez votre message en français, darija ou arabe...",
            label_visibility="collapsed",
            height=70,
            key=f"chat_input_{st.session_state.input_key}",
        )
    with cols[2]:
        send = st.button('<i class="fas fa-paper-plane"></i>', use_container_width=True, key="send_btn")
    with cols[3]:
        if st.button('<i class="fas fa-stop"></i>', use_container_width=True, key="stop_btn"):
            st.session_state.stop_generation = True

    # Chips (boutons rapides)
    chips_cols = st.columns([1, 2, 2, 2, 2])
    chips = [
        ("Analyser tendances", chips_cols[1]),
        ("Problèmes fréquents", chips_cols[2]),
        ("Comparer positif/négatif", chips_cols[3]),
        ("Exporter CSV", chips_cols[4]),
    ]
    for label, col in chips:
        with col:
            if st.button(label, key=f"chip_{label}"):
                st.session_state["pending_question"] = label
                st.rerun()

    # Upload de fichier
    fichier = st.file_uploader(
        '<i class="fas fa-paperclip"></i>',
        type=["txt", "csv", "json", "pdf"],
        label_visibility="collapsed",
        key=f"file_uploader_{st.session_state.input_key}",
    )
    if fichier:
        contenu = fichier.read().decode("utf-8", errors="ignore")[:2000]
        st.session_state["pending_question"] = f"Analyse ce fichier ({fichier.name}) :\n\n{contenu}"
        st.session_state.input_key += 1
        st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)

# ========== TRAITEMENT AUDIO ==========
if audio and "bytes" in audio:
    try:
        audio_bytes = io.BytesIO(audio["bytes"])
        sound = AudioSegment.from_file(audio_bytes)
        sound.export("temp_audio.wav", format="wav")
        recognizer = sr.Recognizer()
        with sr.AudioFile("temp_audio.wav") as source:
            audio_data = recognizer.record(source)
            texte = recognizer.recognize_google(audio_data, language="fr-FR")
            st.session_state["pending_question"] = texte
            st.rerun()
    except Exception as e:
        st.warning(f"Erreur microphone : {e}")

# ========== TRAITEMENT DE LA QUESTION ==========
question = None
if send and question_input and question_input.strip():
    question = question_input.strip()
elif pending:
    question = pending

if question:
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lang_det = detecter_langue(question)

    # Vider l'input
    st.session_state.input_key += 1

    # Réponses directes (groupe, date)
    rep_directe = repondre_question_specifique(question) or repondre_question_par_date(question)

    # Questions spéciales (graphiques, exports)
    rep_speciale = None
    if not rep_directe:
        rep_speciale = traiter_question_speciale(question)

    if rep_directe:
        messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
        messages.append({"role": "assistant", "content": rep_directe, "lang": "fr", "timestamp": ts_now})
        sauvegarder_conv(active_cid, messages)
        st.rerun()

    elif rep_speciale is not None:
        messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
        messages.append({"role": "assistant", "content": rep_speciale, "lang": "fr", "timestamp": ts_now})
        sauvegarder_conv(active_cid, messages)
        # Ne pas rerun pour garder les graphiques affichés

    elif est_question_graphique(question):
        msg_err = (
            "Je ne peux pas générer de graphiques directement.\n"
            "Essayez :\n"
            "• 'Affiche le graphique évolution'\n"
            "• 'Montre le camembert'\n"
            "• 'Top mots négatifs'"
        )
        messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
        messages.append({"role": "assistant", "content": msg_err, "lang": "fr", "timestamp": ts_now})
        sauvegarder_conv(active_cid, messages)
        st.rerun()

    else:
        messages.append({"role": "user", "content": question, "lang": lang_det, "timestamp": ts_now})
        if len(messages) == 1:
            titre = question[:40] + ("…" if len(question) > 40 else "")
            st.session_state.conversations[active_cid]["title"] = titre
            if MONGO_OK:
                try:
                    db.chat_sessions.update_one({"conv_id": active_cid}, {"$set": {"title": titre}})
                except Exception:
                    pass
        sauvegarder_conv(active_cid, messages)
        st.rerun()

# ========== GÉNÉRATION GROQ EN STREAMING ==========
if messages and messages[-1]["role"] == "user":
    derniere_question = messages[-1]["content"]
    if est_question_graphique(derniere_question):
        pass  # déjà bloqué
    elif not GROQ_OK:
        st.error("Clé Groq invalide. Veuillez configurer GROQ_API_KEY.")
    else:
        placeholder = st.empty()
        placeholder.markdown("""
        <div class="msg-bot">
            <div class="bot-icon"><i class="fas fa-robot"></i></div>
            <div class="bubble">
                <div class="typing"><span></span><span></span><span></span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        full_response = ""
        langue_user = messages[-1].get("lang", "fr")

        for delta in appeler_groq_stream(messages[:-1], messages[-1]["content"], langue_user):
            full_response += delta
            placeholder.markdown(f"""
            <div class="msg-bot">
                <div class="bot-icon"><i class="fas fa-robot"></i></div>
                <div class="bubble">{render_md(full_response)}▌</div>
            </div>
            """, unsafe_allow_html=True)

        placeholder.empty()
        messages.append({
            "role": "assistant",
            "content": full_response,
            "lang": langue_user,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        })
        sauvegarder_conv(active_cid, messages)
        st.rerun()