# pages/3_Commentaires.py
import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime
import re

# ============================================================
# CONFIGURATION DE LA PAGE
# ============================================================
st.set_page_config(
    page_title="Télécom Algérie - Commentaires",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# SESSION STATE
# ============================================================
if "authenticated" not in st.session_state:
    st.session_state.authenticated = True  # Pour test, à enlever après
if "theme" not in st.session_state:
    st.session_state.theme = "light"

# ============================================================
# THÈMES
# ============================================================
THEMES = {
    "dark": {
        "bg": "#0a0b0f", "bg2": "#0d0e14", "bg3": "#0f1117",
        "border": "#1a1d2e", "border2": "#2a2d3e",
        "text": "#e8e6e0", "text2": "#9ca3af", "text3": "#4b5563",
        "accent": "#10B981", "accent2": "#3B82F6", "card_bg": "#0d0e14",
    },
    "light": {
        "bg": "#f0f4f8", "bg2": "#ffffff", "bg3": "#f8fafc",
        "border": "#e2e8f0", "border2": "#cbd5e1",
        "text": "#1e293b", "text2": "#475569", "text3": "#94a3b8",
        "accent": "#10B981", "accent2": "#3B82F6", "card_bg": "#ffffff",
    }
}

T = THEMES[st.session_state.theme]

# ============================================================
# CONNEXION MONGODB
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
client = MongoClient(MONGO_URI)
db = client["telecom_algerie"]
collection = db["dataset_unifie"]

# ============================================================
# STYLE CSS
# ============================================================
st.markdown(f"""
<style>
    .stApp {{ background-color: {T['bg']}; }}
    
    /* Cards */
    .card {{
        background: {T['card_bg']};
        border: 1px solid {T['border']};
        border-radius: 16px;
        padding: 1.2rem;
        margin-bottom: 1rem;
    }}
    .card-title {{
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: {T['text3']};
        margin-bottom: 1rem;
        font-weight: 600;
    }}
    
    /* Stats cards */
    .stat-card {{
        background: linear-gradient(135deg, {T['bg2']}, {T['bg']});
        border: 1px solid {T['border']};
        border-radius: 20px;
        padding: 1rem;
        text-align: center;
    }}
    .stat-value {{
        font-size: 1.8rem;
        font-weight: 700;
        color: {T['text']};
    }}
    .stat-label {{
        font-size: 0.7rem;
        color: {T['text3']};
        text-transform: uppercase;
    }}
    
    /* Comment cards */
    .comment-card {{
        background: {T['bg3']};
        border-left: 3px solid;
        border-radius: 12px;
        padding: 1rem;
        margin: 0.75rem 0;
        transition: all 0.2s;
    }}
    .comment-card:hover {{
        transform: translateX(5px);
    }}
    .comment-header {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.5rem;
        flex-wrap: wrap;
        gap: 0.5rem;
    }}
    .comment-sentiment {{
        font-weight: 600;
        font-size: 0.7rem;
        text-transform: uppercase;
        padding: 0.2rem 0.6rem;
        border-radius: 20px;
        display: inline-block;
    }}
    .comment-date {{
        font-size: 0.65rem;
        color: {T['text3']};
    }}
    .comment-text {{
        color: {T['text2']};
        font-size: 0.85rem;
        margin: 0.75rem 0;
        line-height: 1.5;
    }}
    .comment-footer {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 0.65rem;
        color: {T['text3']};
    }}
    
    /* Filter section */
    .filter-section {{
        background: {T['bg2']};
        border: 1px solid {T['border']};
        border-radius: 16px;
        padding: 1rem;
        margin-bottom: 1rem;
    }}
    .filter-label {{
        font-size: 0.7rem;
        color: {T['text3']};
        text-transform: uppercase;
        margin-bottom: 0.5rem;
        display: block;
    }}
    
    /* Sidebar */
    [data-testid="stSidebar"] {{
        background: {T['bg2']};
        border-right: 1px solid {T['border']};
    }}
    [data-testid="stSidebar"] * {{
        color: {T['text']} !important;
    }}
    
    /* Search input */
    .stTextInput input {{
        background: {T['bg3']} !important;
        border: 1px solid {T['border']} !important;
        border-radius: 12px !important;
        color: {T['text']} !important;
    }}
    
    /* Selectbox */
    .stSelectbox label, .stMultiSelect label {{
        color: {T['text3']} !important;
    }}
    
    /* Pagination */
    .pagination {{
        display: flex;
        justify-content: center;
        gap: 0.5rem;
        margin-top: 1rem;
    }}
    
    /* Hide Streamlit elements */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}
    
    h1, h2, h3, h4, h5, h6 {{ color: {T['text']} !important; }}
    hr {{ border-color: {T['border']} !important; }}
</style>
""", unsafe_allow_html=True)

# ============================================================
# FONCTIONS
# ============================================================
@st.cache_data(ttl=30)
def get_all_comments():
    """Récupère tous les commentaires"""
    data = list(collection.find())
    for doc in data:
        doc["_id"] = str(doc["_id"])
    return data

@st.cache_data(ttl=30)
def get_stats():
    """Récupère les statistiques"""
    total = collection.count_documents({})
    positif = collection.count_documents({"label_final": "positif"})
    negatif = collection.count_documents({"label_final": "negatif"})
    neutre = collection.count_documents({"label_final": "neutre"})
    return {"total": total, "positif": positif, "negatif": negatif, "neutre": neutre}

def filter_comments(comments, sentiment_filter, search_text, source_filter):
    """Filtre les commentaires"""
    filtered = comments
    
    # Filtre sentiment
    if sentiment_filter and "Tous" not in sentiment_filter:
        filtered = [c for c in filtered if c.get("label_final") in sentiment_filter]
    
    # Filtre source
    if source_filter:
        filtered = [c for c in filtered if c.get("sources") in source_filter]
    
    # Recherche textuelle
    if search_text:
        search_lower = search_text.lower()
        filtered = [c for c in filtered if search_lower in c.get("Commentaire_Client", "").lower()]
    
    return filtered

# ============================================================
# SIDEBAR - FILTRES
# ============================================================
with st.sidebar:
    st.markdown(f"""
    <div style="margin-bottom: 2rem;">
        <h2 style="margin: 0;">💬 Commentaires</h2>
        <div style="font-size: 0.7rem; color: {T['text3']};">Exploration des avis clients</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Filtres
    st.markdown('<div class="filter-label">🔍 FILTRES</div>', unsafe_allow_html=True)
    
    # Filtre sentiment
    sentiment_options = ["Tous", "positif", "negatif", "neutre"]
    selected_sentiments = st.multiselect(
        "Sentiment",
        sentiment_options,
        default=["Tous"]
    )
    
    # Filtre source
    source_options = ["Facebook", "Twitter", "Forum", "Test_manuel"]
    selected_sources = st.multiselect(
        "Source",
        source_options,
        default=[]
    )
    
    # Limite par page
    limit = st.slider("Commentaires par page", 10, 100, 20, step=10)
    
    st.markdown("---")
    
    # Boutons thème
    st.markdown('<div class="filter-label">🎨 THÈME</div>', unsafe_allow_html=True)
    col_dark, col_light = st.columns(2)
    with col_dark:
        if st.button("🌙 Dark", key="btn_dark", use_container_width=True):
            st.session_state.theme = "dark"
            st.rerun()
    with col_light:
        if st.button("☀️ Light", key="btn_light", use_container_width=True):
            st.session_state.theme = "light"
            st.rerun()
    
    st.markdown("---")
    
    # Bouton retour dashboard
    if st.button("🏠 Retour au Dashboard", use_container_width=True):
        st.switch_page("app.py")

# ============================================================
# CHARGEMENT DES DONNÉES
# ============================================================
stats = get_stats()
all_comments = get_all_comments()

# ============================================================
# EN-TÊTE
# ============================================================
st.markdown(f"""
<div style="margin-bottom: 1.5rem;">
    <h1 style="margin: 0 0 0.2rem 0;">💬 Exploration des commentaires</h1>
    <div style="color: {T['text3']};">Analyse détaillée des avis clients</div>
</div>
""", unsafe_allow_html=True)

# ============================================================
# STATISTIQUES RAPIDES
# ============================================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-value" style="color: {T['accent']};">{stats['total']:,}</div>
        <div class="stat-label">Total commentaires</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-value" style="color: {T['accent']};">{stats['positif']:,}</div>
        <div class="stat-label">😊 Positifs</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-value" style="color: #EF4444;">{stats['negatif']:,}</div>
        <div class="stat-label">😠 Négatifs</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-value" style="color: {T['accent2']};">{stats['neutre']:,}</div>
        <div class="stat-label">😐 Neutres</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ============================================================
# RECHERCHE
# ============================================================
st.markdown('<div class="filter-section">', unsafe_allow_html=True)
st.markdown('<div class="filter-label">🔎 RECHERCHE DANS LES COMMENTAIRES</div>', unsafe_allow_html=True)

search_text = st.text_input("", placeholder="Rechercher un mot-clé dans les commentaires...", label_visibility="collapsed")

st.markdown('</div>', unsafe_allow_html=True)

# ============================================================
# FILTRAGE ET AFFICHAGE
# ============================================================
# Appliquer les filtres
filtered_comments = filter_comments(all_comments, selected_sentiments, search_text, selected_sources)
total_filtered = len(filtered_comments)

# Pagination
if "page" not in st.session_state:
    st.session_state.page = 0

total_pages = max(1, (total_filtered + limit - 1) // limit)
start_idx = st.session_state.page * limit
end_idx = min(start_idx + limit, total_filtered)
page_comments = filtered_comments[start_idx:end_idx]

# En-tête des résultats
st.markdown(f"""
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
    <div style="color: {T['text2']};">
        📊 <strong>{total_filtered}</strong> commentaire(s) trouvé(s)
    </div>
    <div style="color: {T['text3']}; font-size: 0.7rem;">
        Page {st.session_state.page + 1} / {total_pages}
    </div>
</div>
""", unsafe_allow_html=True)

# ============================================================
# AFFICHAGE DES COMMENTAIRES
# ============================================================
if not page_comments:
    st.markdown(f"""
    <div style="text-align: center; padding: 3rem; color: {T['text3']};">
        <div style="font-size: 3rem; margin-bottom: 1rem;">💬</div>
        <div>Aucun commentaire ne correspond à vos critères</div>
        <div style="font-size: 0.8rem;">Essayez de modifier vos filtres</div>
    </div>
    """, unsafe_allow_html=True)
else:
    for comment in page_comments:
        sentiment = comment.get("label_final", "neutre")
        comment_text = comment.get("Commentaire_Client", "")
        source = comment.get("sources", "inconnue")
        date = comment.get("dates", "Date inconnue")
        
        # Couleurs par sentiment
        if sentiment == "positif":
            sentiment_color = T['accent']
            sentiment_bg = f"rgba(16, 185, 129, 0.15)"
            sentiment_icon = "😊"
        elif sentiment == "negatif":
            sentiment_color = "#EF4444"
            sentiment_bg = "rgba(239, 68, 68, 0.15)"
            sentiment_icon = "😠"
        else:
            sentiment_color = T['accent2']
            sentiment_bg = f"rgba(59, 130, 246, 0.15)"
            sentiment_icon = "😐"
        
        # Troncature du texte
        if len(comment_text) > 500:
            comment_text = comment_text[:500] + "..."
        
        st.markdown(f"""
        <div class="comment-card" style="border-left-color: {sentiment_color};">
            <div class="comment-header">
                <div>
                    <span class="comment-sentiment" style="background: {sentiment_bg}; color: {sentiment_color};">
                        {sentiment_icon} {sentiment.upper()}
                    </span>
                </div>
                <div class="comment-date">📅 {date}</div>
            </div>
            <div class="comment-text">{comment_text}</div>
            <div class="comment-footer">
                <span>📱 {source}</span>
                <span>🆔 {comment.get("_id", "")[:8]}...</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

# ============================================================
# PAGINATION
# ============================================================
if total_pages > 1:
    st.markdown('<div class="pagination">', unsafe_allow_html=True)
    
    col_prev, col_page_info, col_next = st.columns([1, 2, 1])
    
    with col_prev:
        if st.button("◀ Précédent", use_container_width=True, disabled=(st.session_state.page == 0)):
            if st.session_state.page > 0:
                st.session_state.page -= 1
                st.rerun()
    
    with col_page_info:
        st.markdown(f"""
        <div style="text-align: center; padding: 0.5rem; color: {T['text2']};">
            Page {st.session_state.page + 1} / {total_pages}
        </div>
        """, unsafe_allow_html=True)
    
    with col_next:
        if st.button("Suivant ▶", use_container_width=True, disabled=(st.session_state.page >= total_pages - 1)):
            if st.session_state.page < total_pages - 1:
                st.session_state.page += 1
                st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================
# EXPORT
# ============================================================
with st.expander("📥 Exporter les données"):
    st.markdown(f"""
    <div style="padding: 0.5rem;">
        <p style="color: {T['text2']};">Exportez les commentaires filtrés au format CSV</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Convertir en DataFrame
    export_df = pd.DataFrame(filtered_comments)
    
    # Sélectionner les colonnes à exporter
    export_columns = ['Commentaire_Client', 'label_final', 'sources', 'dates']
    existing_columns = [c for c in export_columns if c in export_df.columns]
    
    if existing_columns:
        csv = export_df[existing_columns].to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Télécharger CSV",
            data=csv,
            file_name=f"commentaires_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )