import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
import sys
import re
import html
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from sidebar import render_sidebar
from style import load_fontawesome, MAIN_CSS

# --- Connexion MongoDB ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
client = MongoClient(MONGO_URI)
db = client["telecom_algerie"]
collection = db["dataset_unifie_sans_doublons"]

# --- Styles globaux ---
st.markdown(MAIN_CSS, unsafe_allow_html=True)
st.markdown("""
<style>
.main .block-container { padding-top: 1rem; padding-bottom: 2rem; }
.filter-label {
    font-size: 0.75rem; font-weight: 600; color: #1e293b;
    margin-bottom: 0.25rem; display: block; letter-spacing: 0.5px;
}
.comment-card {
    background: white; border-radius: 16px; padding: 1.2rem; margin-bottom: 1rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.03);
    border-left: 5px solid; transition: all 0.2s ease;
}
.comment-card:hover { box-shadow: 0 8px 20px rgba(0,0,0,0.08); transform: translateY(-2px); }
.comment-card.pos { border-left-color: #10b981; }
.comment-card.neg { border-left-color: #ef4444; }
.comment-card.neu { border-left-color: #f59e0b; }
.comment-label {
    font-weight: 600; font-size: 0.75rem; background: #f1f5f9;
    padding: 0.2rem 0.7rem; border-radius: 30px; display: inline-flex; align-items: center; gap: 6px;
}
.comment-meta {
    font-size: 0.7rem; color: #64748b; background: #f8fafc;
    padding: 0.2rem 0.6rem; border-radius: 20px;
}
.comment-text { margin-top: 0.75rem; font-size: 0.9rem; color: #1e293b; line-height: 1.5; }
.section-title-custom {
    font-size: 1.3rem; font-weight: 600; color: #0f3b5c;
    margin: 0 0 1.2rem 0; border-left: 4px solid #2dd4bf; padding-left: 12px;
}
/* Message personnalisé avec icône */
.custom-info {
    background-color: #eef2ff; border-left: 4px solid #3b82f6;
    padding: 0.75rem 1rem; border-radius: 8px; margin-bottom: 1rem;
    font-size: 0.9rem; color: #1e3a8a;
}
.custom-warning {
    background-color: #fffbeb; border-left: 4px solid #f59e0b;
    padding: 0.75rem 1rem; border-radius: 8px; margin-bottom: 1rem;
    font-size: 0.9rem; color: #92400e;
}
</style>
""", unsafe_allow_html=True)

def markdown_to_html(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = html.escape(text)
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = re.sub(r'\*(.+?)\*', r'<em>\1</em>', text)
    lines = text.split('\n')
    in_list = False
    new_lines = []
    for line in lines:
        if re.match(r'^[\-\*]\s+', line.strip()):
            if not in_list:
                new_lines.append('<ul>')
                in_list = True
            content = re.sub(r'^[\-\*]\s+', '', line.strip())
            new_lines.append(f'<li>{content}</li>')
        else:
            if in_list:
                new_lines.append('</ul>')
                in_list = False
            new_lines.append(line)
    if in_list:
        new_lines.append('</ul>')
    text = '\n'.join(new_lines)
    text = text.replace('\n', '<br>')
    return text

# --- Sidebar (identique au dashboard) ---
render_sidebar()

# --- Contenu principal ---
st.markdown('<div class="section-title-custom"><i class="fas fa-comments"></i> Exploration des commentaires</div>', unsafe_allow_html=True)

# --- Filtres dans la sidebar ---
with st.sidebar:
    st.markdown("---")
    st.markdown('<div class="filter-label"><i class="fas fa-filter"></i> FILTRES AVANCÉS</div>', unsafe_allow_html=True)
    
    sentiment_filter = st.multiselect(
        "Sentiment",
        ["positif", "negatif", "neutre"],
        default=["positif", "negatif"],
        key="sentiment_filter_comments"
    )
    
    search_text = st.text_input("Recherche dans les commentaires", placeholder="Mots-clés...", key="search_comments")
    
    limit = st.slider("Nombre de résultats", 10, 200, 50, step=10, key="limit_comments")
    
    if st.button('<i class="fas fa-sync-alt"></i> Réinitialiser', use_container_width=True, key="reset_comments"):
        st.session_state["sentiment_filter_comments"] = ["positif", "negatif"]
        st.session_state["search_comments"] = ""
        st.session_state["limit_comments"] = 50
        st.rerun()

# --- Chargement des données ---
query = {}
if sentiment_filter:
    query["label_final"] = {"$in": sentiment_filter}

cursor = collection.find(query)
total_matching = collection.count_documents(query)
data = list(cursor.limit(limit))
df = pd.DataFrame(data) if data else pd.DataFrame()

if search_text and not df.empty:
    mask = df['Commentaire_Client'].astype(str).str.contains(search_text, case=False, na=False)
    df = df[mask]
    st.markdown(f"""
    <div class="custom-info">
        <i class="fas fa-search"></i> {len(df)} résultat(s) pour « {search_text} »
    </div>
    """, unsafe_allow_html=True)

if df.empty:
    st.markdown("""
    <div class="custom-warning">
        <i class="fas fa-exclamation-triangle"></i> Aucun commentaire trouvé avec ces critères
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# --- En-tête avec compteur et export ---
col1, col2 = st.columns([3, 1])
with col1:
    st.markdown(f'### <i class="fas fa-file-alt"></i> {len(df)} commentaires affichés', unsafe_allow_html=True)
with col2:
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label='<i class="fas fa-download"></i> Exporter CSV',
        data=csv,
        file_name=f"commentaires_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True
    )

# --- Mapping sentiments ---
sentiment_icon = {
    "positif": "far fa-smile-wink",
    "negatif": "far fa-frown",
    "neutre": "far fa-meh",
    "mixed": "fas fa-random"
}
COLOR_MAP = {
    "positif": "#10b981",
    "negatif": "#ef4444",
    "neutre": "#f59e0b",
    "mixed": "#8b5cf6"
}

# --- Affichage des commentaires ---
for _, row in df.iterrows():
    sentiment = row.get('label_final', 'neutre')
    cls = "pos" if sentiment == "positif" else "neg" if sentiment == "negatif" else "neu"
    icon = sentiment_icon.get(sentiment, "far fa-question-circle")
    color = COLOR_MAP.get(sentiment, "#6b7280")
    
    raw_comment = str(row.get('Commentaire_Client', ''))
    cleaned_comment = re.sub(r'^[\-\*]\s+', '', raw_comment.strip(), flags=re.MULTILINE)
    comment_html = markdown_to_html(cleaned_comment)
    
    date = row.get('dates')
    if not date or pd.isna(date):
        date = "Date inconnue"
    source = row.get('sources')
    if not source or pd.isna(source):
        source = "Source inconnue"
    
    st.markdown(f"""
    <div class="comment-card {cls}">
        <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px;">
            <div class="comment-label" style="color:{color};">
                <i class="{icon}"></i> {sentiment.capitalize()}
            </div>
            <div class="comment-meta">
                <i class="far fa-calendar-alt"></i> {date} &nbsp;|&nbsp;
                <i class="fas fa-tower-cell"></i> {source}
            </div>
        </div>
        <div class="comment-text">{comment_html}</div>
    </div>
    """, unsafe_allow_html=True)

# --- Pied de page ---
st.caption(f'<i class="fas fa-database"></i> Total disponible : {total_matching} commentaires | <i class="far fa-clock"></i> Dernière mise à jour : {datetime.now().strftime("%d/%m/%Y %H:%M")}', unsafe_allow_html=True)