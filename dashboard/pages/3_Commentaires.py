# 📁 pages/3_Commentaires.py - Version améliorée
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from style import THEME_CSS, COLOR_MAP, page_header

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
client = MongoClient(MONGO_URI)
db = client["telecom_algerie"]
collection = db["dataset_unifie_sans_doublons"]

st.markdown(THEME_CSS, unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="filter-label">🔍 FILTRES</div>', unsafe_allow_html=True)
    
    # Sentiment
    st.markdown('<span class="filter-label">Sentiment</span>', unsafe_allow_html=True)
    sentiment_filter = st.multiselect(
        "Choisir", ["positif", "negatif", "neutre"],
        default=["positif", "negatif"], label_visibility="collapsed"
    )
    
    # Recherche textuelle
    st.markdown('<span class="filter-label" style="margin-top:0.5rem;">🔎 Recherche</span>', unsafe_allow_html=True)
    search_text = st.text_input("", placeholder="Mots-clés...", label_visibility="collapsed")
    
    # Limite résultats
    limit = st.slider("Nombre de résultats", 10, 200, 50, step=10)
    
    # Bouton reset
    if st.button("🔄 Réinitialiser", use_container_width=True):
        st.query_params.clear()
        st.rerun()

# ── Chargement ────────────────────────────────────────────────────────────────
query = {}
if sentiment_filter:
    query["label_final"] = {"$in": sentiment_filter}

data = list(collection.find(query).limit(limit))
df = pd.DataFrame(data) if data else pd.DataFrame()

# Filtre recherche
if search_text and not df.empty:
    mask = df['Commentaire_Client'].astype(str).str.contains(search_text, case=False, na=False)
    df = df[mask]
    st.info(f"🔍 {len(df)} résultat(s) pour « {search_text} »")

if df.empty:
    st.warning("⚠️ Aucun commentaire trouvé avec ces critères")
    st.stop()

# ── En-tête ───────────────────────────────────────────────────────────────────
st.markdown(page_header("💬", "Exploration des commentaires", 
                       f"{len(df)} commentaires trouvés"), 
           unsafe_allow_html=True)

# ── Affichage des Commentaires ────────────────────────────────────────────────
st.markdown('<div class="card">', unsafe_allow_html=True)

for _, row in df.iterrows():
    sentiment = row.get('label_final', 'inconnu')
    cls = "pos" if sentiment == "positif" else "neg" if sentiment == "negatif" else "neu"
    emoji = {"positif": "😊", "negatif": "😠", "neutre": "😐", "mixed": "🔄"}.get(sentiment, "📝")
    color = COLOR_MAP.get(sentiment, "#6b7280")
    
    comment = str(row.get('Commentaire_Client', ''))
    date = row.get('dates', '—')
    source = row.get('sources', '—')
    
    st.markdown(f"""
    <div class="comment-card {cls}">
        <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:0.5rem; flex-wrap:wrap;">
            <span class="comment-label" style="color:{color};">{emoji} {sentiment}</span>
            <span class="comment-meta">📅 {date} • 📱 {source}</span>
        </div>
        <div class="comment-text">{comment}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)

# ── Pagination & Export ───────────────────────────────────────────────────────
col_export, col_info = st.columns([1, 3])

with col_export:
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Export CSV",
        data=csv,
        file_name=f"commentaires_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        use_container_width=True
    )

with col_info:
    st.markdown(f"""
    <div style="font-size:0.8rem; color:#64748b; text-align:right;">
        Affichage: <strong>{len(df)}</strong> commentaires • 
        Total disponible: <strong>{collection.count_documents(query)}</strong>
    </div>
    """, unsafe_allow_html=True)