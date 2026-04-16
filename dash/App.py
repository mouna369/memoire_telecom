# # # # # dashboard_streamlit.py
# # # # import streamlit as st
# # # # import pandas as pd
# # # # import plotly.express as px
# # # # import plotly.graph_objects as go
# # # # from pymongo import MongoClient
# # # # from datetime import datetime

# # # # # ============================================================
# # # # # CONFIGURATION DE LA PAGE
# # # # # ============================================================
# # # # st.set_page_config(
# # # #     page_title="Télécom Algérie - Dashboard",
# # # #     page_icon="📊",
# # # #     layout="wide",
# # # #     initial_sidebar_state="expanded"
# # # # )

# # # # # ============================================================
# # # # # CONNEXION MONGODB
# # # # # ============================================================
# # # # MONGO_URI = "mongodb://localhost:27018/"
# # # # client = MongoClient(MONGO_URI)
# # # # db = client["telecom_algerie"]
# # # # collection = db["dataset_unifie"]

# # # # # ============================================================
# # # # # STYLE CSS PERSONNALISÉ
# # # # # ============================================================
# # # # st.markdown("""
# # # # <style>
# # # #     /* Global */
# # # #     .stApp {
# # # #         background-color: #0a0b0f;
# # # #     }
    
# # # #     /* Cards */
# # # #     .card {
# # # #         background: #0d0e14;
# # # #         border: 1px solid #1a1d2e;
# # # #         border-radius: 16px;
# # # #         padding: 1.2rem;
# # # #         margin-bottom: 1rem;
# # # #         transition: all 0.2s;
# # # #     }
# # # #     .card:hover {
# # # #         border-color: #f97316;
# # # #         transform: translateY(-2px);
# # # #     }
# # # #     .card-title {
# # # #         font-size: 0.7rem;
# # # #         text-transform: uppercase;
# # # #         letter-spacing: 0.08em;
# # # #         color: #4b5563;
# # # #         margin-bottom: 0.8rem;
# # # #         font-weight: 600;
# # # #     }
    
# # # #     /* KPI Cards */
# # # #     .kpi-card {
# # # #         background: linear-gradient(135deg, #0d0e14, #0a0b0f);
# # # #         border: 1px solid #1a1d2e;
# # # #         border-radius: 20px;
# # # #         padding: 1rem;
# # # #         text-align: center;
# # # #         transition: all 0.2s;
# # # #     }
# # # #     .kpi-card:hover {
# # # #         border-color: #f97316;
# # # #     }
# # # #     .kpi-value {
# # # #         font-size: 1.8rem;
# # # #         font-weight: 700;
# # # #         color: #e8e6e0;
# # # #     }
# # # #     .kpi-label {
# # # #         font-size: 0.7rem;
# # # #         color: #4b5563;
# # # #         text-transform: uppercase;
# # # #         letter-spacing: 0.05em;
# # # #     }
    
# # # #     /* Comment cards */
# # # #     .comment-card {
# # # #         background: #0f1117;
# # # #         border-left: 3px solid;
# # # #         border-radius: 12px;
# # # #         padding: 0.8rem 1rem;
# # # #         margin: 0.75rem 0;
# # # #         transition: all 0.2s;
# # # #     }
# # # #     .comment-card:hover {
# # # #         transform: translateX(5px);
# # # #     }
# # # #     .comment-sentiment {
# # # #         font-weight: 600;
# # # #         font-size: 0.7rem;
# # # #         text-transform: uppercase;
# # # #         margin-bottom: 0.3rem;
# # # #     }
# # # #     .comment-text {
# # # #         color: #9ca3af;
# # # #         font-size: 0.85rem;
# # # #         margin: 0.5rem 0;
# # # #         line-height: 1.5;
# # # #     }
# # # #     .comment-meta {
# # # #         font-size: 0.65rem;
# # # #         color: #4b5563;
# # # #     }
    
# # # #     /* Filters */
# # # #     .filter-label {
# # # #         font-size: 0.7rem;
# # # #         color: #4b5563;
# # # #         text-transform: uppercase;
# # # #         letter-spacing: 0.05em;
# # # #         margin-bottom: 0.5rem;
# # # #         display: block;
# # # #     }
    
# # # #     /* Sidebar */
# # # #     [data-testid="stSidebar"] {
# # # #         background: #0d0e14;
# # # #         border-right: 1px solid #1a1d2e;
# # # #     }
# # # #     [data-testid="stSidebar"] * {
# # # #         color: #e8e6e0 !important;
# # # #     }
    
# # # #     /* Hide Streamlit branding */
# # # #     #MainMenu {visibility: hidden;}
# # # #     footer {visibility: hidden;}
# # # #     header {visibility: hidden;}
# # # # </style>
# # # # """, unsafe_allow_html=True)

# # # # # ============================================================
# # # # # FONCTIONS UTILITAIRES
# # # # # ============================================================
# # # # @st.cache_data(ttl=30)
# # # # def get_stats(sentiment_filter=None):
# # # #     """Récupère les statistiques"""
# # # #     query = {}
# # # #     if sentiment_filter and "Tous" not in sentiment_filter:
# # # #         query["label_final"] = {"$in": sentiment_filter}
    
# # # #     total = collection.count_documents(query)
# # # #     positif = collection.count_documents({**query, "label_final": "positif"})
# # # #     negatif = collection.count_documents({**query, "label_final": "negatif"})
# # # #     neutre = collection.count_documents({**query, "label_final": "neutre"})
    
# # # #     return {
# # # #         "total": total,
# # # #         "positif": positif,
# # # #         "negatif": negatif,
# # # #         "neutre": neutre,
# # # #         "taux_pos": round(positif/total*100, 1) if total else 0,
# # # #         "taux_neg": round(negatif/total*100, 1) if total else 0
# # # #     }

# # # # @st.cache_data(ttl=30)
# # # # def get_comments(limit=20, sentiment_filter=None):
# # # #     """Récupère les commentaires"""
# # # #     query = {}
# # # #     if sentiment_filter and "Tous" not in sentiment_filter:
# # # #         query["label_final"] = {"$in": sentiment_filter}
    
# # # #     data = list(collection.find(query).limit(limit))
# # # #     for doc in data:
# # # #         doc["_id"] = str(doc["_id"])
# # # #     return data

# # # # # Couleurs
# # # # COLOR_MAP = {
# # # #     'positif': '#10B981',
# # # #     'negatif': '#EF4444',
# # # #     'neutre': '#6B7280',
# # # # }

# # # # # ============================================================
# # # # # SIDEBAR - FILTRES
# # # # # ============================================================
# # # # with st.sidebar:
# # # #     st.markdown("""
# # # #     <div style="margin-bottom: 2rem;">
# # # #         <h2 style="margin: 0 0 0.2rem 0; color: #e8e6e0;">📡 TélécomDZ</h2>
# # # #         <div style="font-size: 0.7rem; color: #4b5563; text-transform: uppercase;">Analyse des sentiments</div>
# # # #     </div>
# # # #     """, unsafe_allow_html=True)
    
# # # #     st.markdown("---")
# # # #     st.markdown('<div class="filter-label">🔍 FILTRES</div>', unsafe_allow_html=True)
    
# # # #     # Filtre sentiment
# # # #     sentiment_options = ["Tous", "positif", "negatif", "neutre"]
# # # #     selected_sentiments = st.multiselect(
# # # #         "Sentiment",
# # # #         sentiment_options,
# # # #         default=["Tous"]
# # # #     )
    
# # # #     if "Tous" in selected_sentiments:
# # # #         sentiment_filter = None
# # # #     else:
# # # #         sentiment_filter = selected_sentiments if selected_sentiments else None
    
# # # #     st.markdown("---")
# # # #     st.markdown('<div class="filter-label">📊 Statistiques</div>', unsafe_allow_html=True)
    
# # # #     # Afficher un résumé des stats
# # # #     stats_summary = get_stats(sentiment_filter)
# # # #     st.markdown(f"""
# # # #     <div style="font-size: 0.8rem; color: #9ca3af; line-height: 1.6;">
# # # #         Total: {stats_summary['total']:,}<br>
# # # #         Positifs: {stats_summary['positif']:,}<br>
# # # #         Négatifs: {stats_summary['negatif']:,}<br>
# # # #         Neutres: {stats_summary['neutre']:,}
# # # #     </div>
# # # #     """, unsafe_allow_html=True)

# # # # # ============================================================
# # # # # CHARGEMENT DES DONNÉES
# # # # # ============================================================
# # # # stats = get_stats(sentiment_filter)
# # # # comments = get_comments(15, sentiment_filter)

# # # # # ============================================================
# # # # # EN-TÊTE
# # # # # ============================================================
# # # # st.markdown("""
# # # # <div style="margin-bottom: 2rem;">
# # # #     <h1 style="margin: 0 0 0.2rem 0;">📊 Vue d'ensemble</h1>
# # # #     <div style="color: #4b5563;">Analyse globale des sentiments clients · Télécom Algérie</div>
# # # #     <div style="font-size: 0.7rem; color: #4b5563; margin-top: 0.5rem;">🔄 Dernière mise à jour: {}</div>
# # # # </div>
# # # # """.format(datetime.now().strftime('%H:%M:%S')), unsafe_allow_html=True)

# # # # # ============================================================
# # # # # KPI CARDS
# # # # # ============================================================
# # # # col1, col2, col3, col4 = st.columns(4)

# # # # with col1:
# # # #     st.markdown(f"""
# # # #     <div class="kpi-card">
# # # #         <div class="kpi-label">📝 Total commentaires</div>
# # # #         <div class="kpi-value">{stats['total']:,}</div>
# # # #     </div>
# # # #     """, unsafe_allow_html=True)

# # # # with col2:
# # # #     st.markdown(f"""
# # # #     <div class="kpi-card">
# # # #         <div class="kpi-label">😊 Positifs</div>
# # # #         <div class="kpi-value">{stats['positif']:,} ({stats['taux_pos']}%)</div>
# # # #     </div>
# # # #     """, unsafe_allow_html=True)

# # # # with col3:
# # # #     st.markdown(f"""
# # # #     <div class="kpi-card">
# # # #         <div class="kpi-label">😠 Négatifs</div>
# # # #         <div class="kpi-value">{stats['negatif']:,} ({stats['taux_neg']}%)</div>
# # # #     </div>
# # # #     """, unsafe_allow_html=True)

# # # # with col4:
# # # #     st.markdown(f"""
# # # #     <div class="kpi-card">
# # # #         <div class="kpi-label">😐 Neutres</div>
# # # #         <div class="kpi-value">{stats['neutre']:,}</div>
# # # #     </div>
# # # #     """, unsafe_allow_html=True)

# # # # st.markdown("---")

# # # # # ============================================================
# # # # # GRAPHIQUES
# # # # # ============================================================
# # # # col_chart1, col_chart2 = st.columns(2)

# # # # with col_chart1:
# # # #     st.markdown('<div class="card">', unsafe_allow_html=True)
# # # #     st.markdown('<div class="card-title">Distribution des sentiments</div>', unsafe_allow_html=True)
    
# # # #     fig_bar = px.bar(
# # # #         x=['Positifs', 'Négatifs', 'Neutres'],
# # # #         y=[stats['positif'], stats['negatif'], stats['neutre']],
# # # #         color=['Positifs', 'Négatifs', 'Neutres'],
# # # #         color_discrete_map={
# # # #             'Positifs': COLOR_MAP['positif'],
# # # #             'Négatifs': COLOR_MAP['negatif'],
# # # #             'Neutres': COLOR_MAP['neutre']
# # # #         },
# # # #         text_auto=True
# # # #     )
# # # #     fig_bar.update_layout(
# # # #         plot_bgcolor='rgba(0,0,0,0)',
# # # #         paper_bgcolor='rgba(0,0,0,0)',
# # # #         font=dict(color='#e8e6e0'),
# # # #         showlegend=False,
# # # #         height=350,
# # # #         xaxis=dict(gridcolor='#2a2d3e'),
# # # #         yaxis=dict(gridcolor='#2a2d3e')
# # # #     )
# # # #     fig_bar.update_traces(textfont_color='#e8e6e0')
# # # #     st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False})
# # # #     st.markdown('</div>', unsafe_allow_html=True)

# # # # with col_chart2:
# # # #     st.markdown('<div class="card">', unsafe_allow_html=True)
# # # #     st.markdown('<div class="card-title">Répartition</div>', unsafe_allow_html=True)
    
# # # #     fig_pie = px.pie(
# # # #         values=[stats['positif'], stats['negatif'], stats['neutre']],
# # # #         names=['Positifs', 'Négatifs', 'Neutres'],
# # # #         color=['Positifs', 'Négatifs', 'Neutres'],
# # # #         color_discrete_map={
# # # #             'Positifs': COLOR_MAP['positif'],
# # # #             'Négatifs': COLOR_MAP['negatif'],
# # # #             'Neutres': COLOR_MAP['neutre']
# # # #         },
# # # #         hole=0.5
# # # #     )
# # # #     fig_pie.update_layout(
# # # #         plot_bgcolor='rgba(0,0,0,0)',
# # # #         paper_bgcolor='rgba(0,0,0,0)',
# # # #         font=dict(color='#e8e6e0'),
# # # #         showlegend=True,
# # # #         height=350
# # # #     )
# # # #     fig_pie.update_traces(textinfo="percent", textfont_color='#e8e6e0')
# # # #     st.plotly_chart(fig_pie, use_container_width=True, config={'displayModeBar': False})
# # # #     st.markdown('</div>', unsafe_allow_html=True)

# # # # # ============================================================
# # # # # DERNIERS COMMENTAIRES
# # # # # ============================================================
# # # # st.markdown('<div class="card">', unsafe_allow_html=True)
# # # # st.markdown('<div class="card-title">📝 Derniers commentaires</div>', unsafe_allow_html=True)

# # # # for comment in comments:
# # # #     sentiment = comment.get('label_final', 'neutre')
# # # #     comment_text = comment.get('Commentaire_Client', '')[:200]
# # # #     if len(comment_text) > 150:
# # # #         comment_text = comment_text[:150] + "..."
# # # #     source = comment.get('sources', 'inconnue')
# # # #     date = comment.get('dates', 'Date inconnue')
    
# # # #     st.markdown(f"""
# # # #     <div class="comment-card" style="border-left-color: {COLOR_MAP.get(sentiment, '#4b5563')};">
# # # #         <div class="comment-sentiment" style="color: {COLOR_MAP.get(sentiment, '#4b5563')};">{sentiment.upper()}</div>
# # # #         <div class="comment-text">{comment_text}</div>
# # # #         <div class="comment-meta">📱 {source} | 📅 {date}</div>
# # # #     </div>
# # # #     """, unsafe_allow_html=True)

# # # # st.markdown('</div>', unsafe_allow_html=True)

# # # # # ============================================================
# # # # # BOUTON RAFRAÎCHIR
# # # # # ============================================================
# # # # if st.button("🔄 Rafraîchir les données", use_container_width=True):
# # # #     st.cache_data.clear()
# # # #     st.rerun()

# # # # dashboard_streamlit.py
# # # import streamlit as st
# # # import pandas as pd
# # # import plotly.express as px
# # # import plotly.graph_objects as go
# # # from pymongo import MongoClient
# # # from datetime import datetime

# # # # ============================================================
# # # # CONFIGURATION DE LA PAGE
# # # # ============================================================
# # # st.set_page_config(
# # #     page_title="Télécom Algérie - Dashboard",
# # #     page_icon="📊",
# # #     layout="wide",
# # #     initial_sidebar_state="expanded"
# # # )

# # # # ============================================================
# # # # SESSION STATE POUR LE THÈME
# # # # ============================================================
# # # if "theme" not in st.session_state:
# # #     st.session_state.theme = "dark"

# # # # ============================================================
# # # # THÈMES
# # # # ============================================================
# # # THEMES = {
# # #     "dark": {
# # #         "bg": "#0a0b0f",
# # #         "bg2": "#0d0e14",
# # #         "bg3": "#0f1117",
# # #         "border": "#1a1d2e",
# # #         "border2": "#2a2d3e",
# # #         "text": "#e8e6e0",
# # #         "text2": "#9ca3af",
# # #         "text3": "#4b5563",
# # #         "accent": "#10B981",  # Vert
# # #         "accent2": "#3B82F6", # Bleu
# # #         "accent3": "#FFFFFF", # Blanc
# # #         "card_bg": "#0d0e14",
# # #         "icon": "🌙",
# # #         "icon_lbl": "Mode clair"
# # #     },
# # #     "light": {
# # #         "bg": "#f0f4f8",
# # #         "bg2": "#ffffff",
# # #         "bg3": "#f8fafc",
# # #         "border": "#e2e8f0",
# # #         "border2": "#cbd5e1",
# # #         "text": "#1e293b",
# # #         "text2": "#475569",
# # #         "text3": "#94a3b8",
# # #         "accent": "#10B981",  # Vert
# # #         "accent2": "#3B82F6", # Bleu
# # #         "accent3": "#FFFFFF", # Blanc
# # #         "card_bg": "#ffffff",
# # #         "icon": "☀️",
# # #         "icon_lbl": "Mode sombre"
# # #     }
# # # }

# # # T = THEMES[st.session_state.theme]

# # # # ============================================================
# # # # CONNEXION MONGODB
# # # # ============================================================
# # # MONGO_URI = "mongodb://localhost:27018/"
# # # client = MongoClient(MONGO_URI)
# # # db = client["telecom_algerie"]
# # # collection = db["dataset_unifie"]

# # # # ============================================================
# # # # STYLE CSS PERSONNALISÉ
# # # # ============================================================
# # # st.markdown(f"""
# # # <style>
# # #     /* Global */
# # #     .stApp {{
# # #         background-color: {T['bg']};
# # #     }}
    
# # #     /* Cards */
# # #     .card {{
# # #         background: {T['card_bg']};
# # #         border: 1px solid {T['border']};
# # #         border-radius: 16px;
# # #         padding: 1.2rem;
# # #         margin-bottom: 1rem;
# # #         transition: all 0.2s;
# # #     }}
# # #     .card:hover {{
# # #         border-color: {T['accent']};
# # #         transform: translateY(-2px);
# # #     }}
# # #     .card-title {{
# # #         font-size: 0.7rem;
# # #         text-transform: uppercase;
# # #         letter-spacing: 0.08em;
# # #         color: {T['text3']};
# # #         margin-bottom: 0.8rem;
# # #         font-weight: 600;
# # #     }}
    
# # #     /* KPI Cards */
# # #     .kpi-card {{
# # #         background: linear-gradient(135deg, {T['bg2']}, {T['bg']});
# # #         border: 1px solid {T['border']};
# # #         border-radius: 20px;
# # #         padding: 1rem;
# # #         text-align: center;
# # #         transition: all 0.2s;
# # #     }}
# # #     .kpi-card:hover {{
# # #         border-color: {T['accent']};
# # #         transform: translateY(-2px);
# # #     }}
# # #     .kpi-value {{
# # #         font-size: 1.8rem;
# # #         font-weight: 700;
# # #         color: {T['text']};
# # #     }}
# # #     .kpi-label {{
# # #         font-size: 0.7rem;
# # #         color: {T['text3']};
# # #         text-transform: uppercase;
# # #         letter-spacing: 0.05em;
# # #     }}
    
# # #     /* Comment cards */
# # #     .comment-card {{
# # #         background: {T['bg3']};
# # #         border-left: 3px solid;
# # #         border-radius: 12px;
# # #         padding: 0.8rem 1rem;
# # #         margin: 0.75rem 0;
# # #         transition: all 0.2s;
# # #     }}
# # #     .comment-card:hover {{
# # #         transform: translateX(5px);
# # #     }}
# # #     .comment-sentiment {{
# # #         font-weight: 600;
# # #         font-size: 0.7rem;
# # #         text-transform: uppercase;
# # #         margin-bottom: 0.3rem;
# # #     }}
# # #     .comment-text {{
# # #         color: {T['text2']};
# # #         font-size: 0.85rem;
# # #         margin: 0.5rem 0;
# # #         line-height: 1.5;
# # #     }}
# # #     .comment-meta {{
# # #         font-size: 0.65rem;
# # #         color: {T['text3']};
# # #     }}
    
# # #     /* Filters */
# # #     .filter-label {{
# # #         font-size: 0.7rem;
# # #         color: {T['text3']};
# # #         text-transform: uppercase;
# # #         letter-spacing: 0.05em;
# # #         margin-bottom: 0.5rem;
# # #         display: block;
# # #     }}
    
# # #     /* Sidebar */
# # #     [data-testid="stSidebar"] {{
# # #         background: {T['bg2']};
# # #         border-right: 1px solid {T['border']};
# # #     }}
# # #     [data-testid="stSidebar"] * {{
# # #         color: {T['text']} !important;
# # #     }}
    
# # #     /* Buttons */
# # #     .stButton > button {{
# # #         background: linear-gradient(135deg, {T['accent']}, {T['accent2']}) !important;
# # #         color: white !important;
# # #         border: none !important;
# # #         border-radius: 10px !important;
# # #         padding: 0.5rem 1rem !important;
# # #         font-weight: 600 !important;
# # #         transition: all 0.2s !important;
# # #     }}
# # #     .stButton > button:hover {{
# # #         opacity: 0.9 !important;
# # #         transform: translateY(-1px) !important;
# # #     }}
    
# # #     /* Selectbox */
# # #     .stSelectbox label, .stMultiSelect label {{
# # #         color: {T['text3']} !important;
# # #     }}
    
# # #     /* Hide Streamlit branding */
# # #     #MainMenu {{visibility: hidden;}}
# # #     footer {{visibility: hidden;}}
# # #     header {{visibility: hidden;}}
    
# # #     /* Headers */
# # #     h1, h2, h3, h4, h5, h6 {{
# # #         color: {T['text']} !important;
# # #     }}
    
# # #     /* Divider */
# # #     hr {{
# # #         border-color: {T['border']} !important;
# # #     }}
# # # </style>
# # # """, unsafe_allow_html=True)

# # # # ============================================================
# # # # FONCTIONS UTILITAIRES
# # # # ============================================================
# # # @st.cache_data(ttl=30)
# # # def get_stats(sentiment_filter=None):
# # #     """Récupère les statistiques"""
# # #     query = {}
# # #     if sentiment_filter and "Tous" not in sentiment_filter:
# # #         query["label_final"] = {"$in": sentiment_filter}
    
# # #     total = collection.count_documents(query)
# # #     positif = collection.count_documents({**query, "label_final": "positif"})
# # #     negatif = collection.count_documents({**query, "label_final": "negatif"})
# # #     neutre = collection.count_documents({**query, "label_final": "neutre"})
    
# # #     return {
# # #         "total": total,
# # #         "positif": positif,
# # #         "negatif": negatif,
# # #         "neutre": neutre,
# # #         "taux_pos": round(positif/total*100, 1) if total else 0,
# # #         "taux_neg": round(negatif/total*100, 1) if total else 0
# # #     }

# # # @st.cache_data(ttl=30)
# # # def get_comments(limit=20, sentiment_filter=None):
# # #     """Récupère les commentaires"""
# # #     query = {}
# # #     if sentiment_filter and "Tous" not in sentiment_filter:
# # #         query["label_final"] = {"$in": sentiment_filter}
    
# # #     data = list(collection.find(query).limit(limit))
# # #     for doc in data:
# # #         doc["_id"] = str(doc["_id"])
# # #     return data

# # # # Couleurs (Vert, Bleu, Blanc)
# # # COLOR_MAP = {
# # #     'positif': T['accent'],   # Vert
# # #     'negatif': '#EF4444',     # Rouge (garde rouge pour négatif)
# # #     'neutre': T['accent2'],   # Bleu
# # # }

# # # # ============================================================
# # # # SIDEBAR - FILTRES
# # # # ============================================================
# # # with st.sidebar:
# # #     st.markdown(f"""
# # #     <div style="margin-bottom: 2rem;">
# # #         <h2 style="margin: 0 0 0.2rem 0; color: {T['text']};">📡 TélécomDZ</h2>
# # #         <div style="font-size: 0.7rem; color: {T['text3']}; text-transform: uppercase;">Analyse des sentiments</div>
# # #     </div>
# # #     """, unsafe_allow_html=True)
    
# # #     # Bouton toggle thème
# # #     col_theme1, col_theme2 = st.columns([1, 3])
# # #     with col_theme1:
# # #         st.markdown(f"<div style='font-size: 20px;'>{T['icon']}</div>", unsafe_allow_html=True)
# # #     with col_theme2:
# # #         if st.button(T['icon_lbl'], key="theme_toggle", use_container_width=True):
# # #             st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"
# # #             st.rerun()
    
# # #     st.markdown("---")
# # #     st.markdown('<div class="filter-label">🔍 FILTRES</div>', unsafe_allow_html=True)
    
# # #     # Filtre sentiment
# # #     sentiment_options = ["Tous", "positif", "negatif", "neutre"]
# # #     selected_sentiments = st.multiselect(
# # #         "Sentiment",
# # #         sentiment_options,
# # #         default=["Tous"]
# # #     )
    
# # #     if "Tous" in selected_sentiments:
# # #         sentiment_filter = None
# # #     else:
# # #         sentiment_filter = selected_sentiments if selected_sentiments else None
    
# # #     st.markdown("---")
# # #     st.markdown('<div class="filter-label">📊 Statistiques</div>', unsafe_allow_html=True)
    
# # #     # Afficher un résumé des stats
# # #     stats_summary = get_stats(sentiment_filter)
# # #     st.markdown(f"""
# # #     <div style="font-size: 0.8rem; color: {T['text2']}; line-height: 1.6;">
# # #         <span style="color: {T['accent']};">●</span> Total: {stats_summary['total']:,}<br>
# # #         <span style="color: {T['accent']};">●</span> Positifs: {stats_summary['positif']:,}<br>
# # #         <span style="color: #EF4444;">●</span> Négatifs: {stats_summary['negatif']:,}<br>
# # #         <span style="color: {T['accent2']};">●</span> Neutres: {stats_summary['neutre']:,}
# # #     </div>
# # #     """, unsafe_allow_html=True)

# # # # ============================================================
# # # # CHARGEMENT DES DONNÉES
# # # # ============================================================
# # # stats = get_stats(sentiment_filter)
# # # comments = get_comments(15, sentiment_filter)

# # # # ============================================================
# # # # EN-TÊTE
# # # # ============================================================
# # # st.markdown(f"""
# # # <div style="margin-bottom: 2rem;">
# # #     <h1 style="margin: 0 0 0.2rem 0;">📊 Vue d'ensemble</h1>
# # #     <div style="color: {T['text3']};">Analyse globale des sentiments clients · Télécom Algérie</div>
# # #     <div style="font-size: 0.7rem; color: {T['text3']}; margin-top: 0.5rem;">🔄 Dernière mise à jour: {datetime.now().strftime('%H:%M:%S')}</div>
# # # </div>
# # # """, unsafe_allow_html=True)

# # # # ============================================================
# # # # KPI CARDS
# # # # ============================================================
# # # col1, col2, col3, col4 = st.columns(4)

# # # with col1:
# # #     st.markdown(f"""
# # #     <div class="kpi-card">
# # #         <div class="kpi-label">📝 Total commentaires</div>
# # #         <div class="kpi-value" style="color: {T['accent']};">{stats['total']:,}</div>
# # #     </div>
# # #     """, unsafe_allow_html=True)

# # # with col2:
# # #     st.markdown(f"""
# # #     <div class="kpi-card">
# # #         <div class="kpi-label">😊 Positifs</div>
# # #         <div class="kpi-value" style="color: {T['accent']};">{stats['positif']:,} ({stats['taux_pos']}%)</div>
# # #     </div>
# # #     """, unsafe_allow_html=True)

# # # with col3:
# # #     st.markdown(f"""
# # #     <div class="kpi-card">
# # #         <div class="kpi-label">😠 Négatifs</div>
# # #         <div class="kpi-value" style="color: #EF4444;">{stats['negatif']:,} ({stats['taux_neg']}%)</div>
# # #     </div>
# # #     """, unsafe_allow_html=True)

# # # with col4:
# # #     st.markdown(f"""
# # #     <div class="kpi-card">
# # #         <div class="kpi-label">😐 Neutres</div>
# # #         <div class="kpi-value" style="color: {T['accent2']};">{stats['neutre']:,}</div>
# # #     </div>
# # #     """, unsafe_allow_html=True)

# # # st.markdown("---")

# # # # ============================================================
# # # # GRAPHIQUES
# # # # ============================================================
# # # col_chart1, col_chart2 = st.columns(2)

# # # with col_chart1:
# # #     st.markdown('<div class="card">', unsafe_allow_html=True)
# # #     st.markdown('<div class="card-title">Distribution des sentiments</div>', unsafe_allow_html=True)
    
# # #     fig_bar = px.bar(
# # #         x=['Positifs', 'Négatifs', 'Neutres'],
# # #         y=[stats['positif'], stats['negatif'], stats['neutre']],
# # #         color=['Positifs', 'Négatifs', 'Neutres'],
# # #         color_discrete_map={
# # #             'Positifs': T['accent'],
# # #             'Négatifs': '#EF4444',
# # #             'Neutres': T['accent2']
# # #         },
# # #         text_auto=True
# # #     )
# # #     fig_bar.update_layout(
# # #         plot_bgcolor='rgba(0,0,0,0)',
# # #         paper_bgcolor='rgba(0,0,0,0)',
# # #         font=dict(color=T['text']),
# # #         showlegend=False,
# # #         height=350,
# # #         xaxis=dict(gridcolor=T['border2']),
# # #         yaxis=dict(gridcolor=T['border2'])
# # #     )
# # #     fig_bar.update_traces(textfont_color=T['text'])
# # #     st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False})
# # #     st.markdown('</div>', unsafe_allow_html=True)

# # # with col_chart2:
# # #     st.markdown('<div class="card">', unsafe_allow_html=True)
# # #     st.markdown('<div class="card-title">Répartition</div>', unsafe_allow_html=True)
    
# # #     fig_pie = px.pie(
# # #         values=[stats['positif'], stats['negatif'], stats['neutre']],
# # #         names=['Positifs', 'Négatifs', 'Neutres'],
# # #         color=['Positifs', 'Négatifs', 'Neutres'],
# # #         color_discrete_map={
# # #             'Positifs': T['accent'],
# # #             'Négatifs': '#EF4444',
# # #             'Neutres': T['accent2']
# # #         },
# # #         hole=0.5
# # #     )
# # #     fig_pie.update_layout(
# # #         plot_bgcolor='rgba(0,0,0,0)',
# # #         paper_bgcolor='rgba(0,0,0,0)',
# # #         font=dict(color=T['text']),
# # #         showlegend=True,
# # #         height=350
# # #     )
# # #     fig_pie.update_traces(textinfo="percent", textfont_color=T['text'])
# # #     st.plotly_chart(fig_pie, use_container_width=True, config={'displayModeBar': False})
# # #     st.markdown('</div>', unsafe_allow_html=True)

# # # # ============================================================
# # # # STATISTIQUES AVANCÉES (EXPANDER)
# # # # ============================================================
# # # with st.expander("📊 Statistiques détaillées"):
# # #     col_stat1, col_stat2 = st.columns(2)
    
# # #     with col_stat1:
# # #         st.markdown(f"""
# # #         <div style="background: {T['bg3']}; padding: 1rem; border-radius: 12px;">
# # #             <h4 style="color: {T['accent']};">📈 Indicateurs clés</h4>
# # #             <ul style="color: {T['text2']};">
# # #                 <li>Total commentaires: <strong>{stats['total']:,}</strong></li>
# # #                 <li>Positifs: <strong>{stats['positif']:,}</strong> ({stats['taux_pos']}%)</li>
# # #                 <li>Négatifs: <strong>{stats['negatif']:,}</strong> ({stats['taux_neg']}%)</li>
# # #                 <li>Neutres: <strong>{stats['neutre']:,}</strong></li>
# # #             </ul>
# # #         </div>
# # #         """, unsafe_allow_html=True)
    
# # #     with col_stat2:
# # #         st.markdown(f"""
# # #         <div style="background: {T['bg3']}; padding: 1rem; border-radius: 12px;">
# # #             <h4 style="color: {T['accent']};">🎯 Ratios</h4>
# # #             <ul style="color: {T['text2']};">
# # #                 <li>Ratio Positif/Négatif: <strong>{stats['taux_pos']/stats['taux_neg']:.2f if stats['taux_neg'] > 0 else 'N/A'}</strong></li>
# # #                 <li>Taux satisfaction: <strong>{stats['taux_pos']}%</strong></li>
# # #                 <li>Taux insatisfaction: <strong>{stats['taux_neg']}%</strong></li>
# # #             </ul>
# # #         </div>
# # #         """, unsafe_allow_html=True)

# # # # ============================================================
# # # # DERNIERS COMMENTAIRES
# # # # ============================================================
# # # st.markdown('<div class="card">', unsafe_allow_html=True)
# # # st.markdown('<div class="card-title">📝 Derniers commentaires</div>', unsafe_allow_html=True)

# # # for comment in comments:
# # #     sentiment = comment.get('label_final', 'neutre')
# # #     comment_text = comment.get('Commentaire_Client', '')[:200]
# # #     if len(comment_text) > 150:
# # #         comment_text = comment_text[:150] + "..."
# # #     source = comment.get('sources', 'inconnue')
# # #     date = comment.get('dates', 'Date inconnue')
    
# # #     border_color = T['accent'] if sentiment == 'positif' else '#EF4444' if sentiment == 'negatif' else T['accent2']
# # #     text_color = T['accent'] if sentiment == 'positif' else '#EF4444' if sentiment == 'negatif' else T['accent2']
    
# # #     st.markdown(f"""
# # #     <div class="comment-card" style="border-left-color: {border_color};">
# # #         <div class="comment-sentiment" style="color: {text_color};">{sentiment.upper()}</div>
# # #         <div class="comment-text">{comment_text}</div>
# # #         <div class="comment-meta">📱 {source} | 📅 {date}</div>
# # #     </div>
# # #     """, unsafe_allow_html=True)

# # # st.markdown('</div>', unsafe_allow_html=True)

# # # # ============================================================
# # # # BOUTON RAFRAÎCHIR
# # # # ============================================================
# # # if st.button("🔄 Rafraîchir les données", use_container_width=True):
# # #     st.cache_data.clear()
# # #     st.rerun()

# # # dashboard_streamlit.py
# # import streamlit as st
# # import pandas as pd
# # import plotly.express as px
# # import plotly.graph_objects as go
# # from pymongo import MongoClient
# # from datetime import datetime

# # # ============================================================
# # # CONFIGURATION DE LA PAGE
# # # ============================================================
# # st.set_page_config(
# #     page_title="Télécom Algérie - Dashboard",
# #     page_icon="📊",
# #     layout="wide",
# #     initial_sidebar_state="expanded"
# # )

# # # ============================================================
# # # SESSION STATE POUR LE THÈME
# # # ============================================================
# # if "theme" not in st.session_state:
# #     st.session_state.theme = "dark"

# # # ============================================================
# # # THÈMES
# # # ============================================================
# # THEMES = {
# #     "dark": {
# #         "bg": "#0a0b0f",
# #         "bg2": "#0d0e14",
# #         "bg3": "#0f1117",
# #         "border": "#1a1d2e",
# #         "border2": "#2a2d3e",
# #         "text": "#e8e6e0",
# #         "text2": "#9ca3af",
# #         "text3": "#4b5563",
# #         "accent": "#10B981",
# #         "accent2": "#3B82F6",
# #         "card_bg": "#0d0e14",
# #     },
# #     "light": {
# #         "bg": "#f0f4f8",
# #         "bg2": "#ffffff",
# #         "bg3": "#f8fafc",
# #         "border": "#e2e8f0",
# #         "border2": "#cbd5e1",
# #         "text": "#1e293b",
# #         "text2": "#475569",
# #         "text3": "#94a3b8",
# #         "accent": "#10B981",
# #         "accent2": "#3B82F6",
# #         "card_bg": "#ffffff",
# #     }
# # }

# # def set_theme(theme_name):
# #     st.session_state.theme = theme_name
# #     st.rerun()

# # T = THEMES[st.session_state.theme]

# # # ============================================================
# # # CONNEXION MONGODB
# # # ============================================================
# # MONGO_URI = "mongodb://localhost:27018/"
# # client = MongoClient(MONGO_URI)
# # db = client["telecom_algerie"]
# # collection = db["dataset_unifie"]

# # # ============================================================
# # # STYLE CSS PERSONNALISÉ
# # # ============================================================
# # st.markdown(f"""
# # <style>
# #     /* Global */
# #     .stApp {{
# #         background-color: {T['bg']};
# #     }}
    
# #     /* Cards */
# #     .card {{
# #         background: {T['card_bg']};
# #         border: 1px solid {T['border']};
# #         border-radius: 16px;
# #         padding: 1.2rem;
# #         margin-bottom: 1rem;
# #         transition: all 0.2s;
# #     }}
# #     .card:hover {{
# #         border-color: {T['accent']};
# #         transform: translateY(-2px);
# #     }}
# #     .card-title {{
# #         font-size: 0.7rem;
# #         text-transform: uppercase;
# #         letter-spacing: 0.08em;
# #         color: {T['text3']};
# #         margin-bottom: 0.8rem;
# #         font-weight: 600;
# #     }}
    
# #     /* KPI Cards */
# #     .kpi-card {{
# #         background: linear-gradient(135deg, {T['bg2']}, {T['bg']});
# #         border: 1px solid {T['border']};
# #         border-radius: 20px;
# #         padding: 1rem;
# #         text-align: center;
# #         transition: all 0.2s;
# #     }}
# #     .kpi-card:hover {{
# #         border-color: {T['accent']};
# #         transform: translateY(-2px);
# #     }}
# #     .kpi-value {{
# #         font-size: 1.8rem;
# #         font-weight: 700;
# #         color: {T['text']};
# #     }}
# #     .kpi-label {{
# #         font-size: 0.7rem;
# #         color: {T['text3']};
# #         text-transform: uppercase;
# #         letter-spacing: 0.05em;
# #     }}
    
# #     /* Comment cards */
# #     .comment-card {{
# #         background: {T['bg3']};
# #         border-left: 3px solid;
# #         border-radius: 12px;
# #         padding: 0.8rem 1rem;
# #         margin: 0.75rem 0;
# #         transition: all 0.2s;
# #     }}
# #     .comment-card:hover {{
# #         transform: translateX(5px);
# #     }}
# #     .comment-sentiment {{
# #         font-weight: 600;
# #         font-size: 0.7rem;
# #         text-transform: uppercase;
# #         margin-bottom: 0.3rem;
# #     }}
# #     .comment-text {{
# #         color: {T['text2']};
# #         font-size: 0.85rem;
# #         margin: 0.5rem 0;
# #         line-height: 1.5;
# #     }}
# #     .comment-meta {{
# #         font-size: 0.65rem;
# #         color: {T['text3']};
# #     }}
    
# #     /* Filters */
# #     .filter-label {{
# #         font-size: 0.7rem;
# #         color: {T['text3']};
# #         text-transform: uppercase;
# #         letter-spacing: 0.05em;
# #         margin-bottom: 0.5rem;
# #         display: block;
# #     }}
    
# #     /* Sidebar */
# #     [data-testid="stSidebar"] {{
# #         background: {T['bg2']};
# #         border-right: 1px solid {T['border']};
# #     }}
# #     [data-testid="stSidebar"] * {{
# #         color: {T['text']} !important;
# #     }}
    
# #     /* Theme buttons */
# #     .theme-btn {{
# #         background: {T['bg3']} !important;
# #         border: 1px solid {T['border']} !important;
# #         border-radius: 10px !important;
# #         padding: 0.5rem !important;
# #         text-align: center !important;
# #         cursor: pointer !important;
# #         transition: all 0.2s !important;
# #         width: 100% !important;
# #     }}
# #     .theme-btn:hover {{
# #         border-color: {T['accent']} !important;
# #         transform: translateY(-2px) !important;
# #     }}
    
# #     /* Hide Streamlit branding */
# #     #MainMenu {{visibility: hidden;}}
# #     footer {{visibility: hidden;}}
# #     header {{visibility: hidden;}}
    
# #     /* Headers */
# #     h1, h2, h3, h4, h5, h6 {{
# #         color: {T['text']} !important;
# #     }}
    
# #     /* Divider */
# #     hr {{
# #         border-color: {T['border']} !important;
# #     }}
    
# #     /* Selectbox */
# #     .stSelectbox label, .stMultiSelect label {{
# #         color: {T['text3']} !important;
# #     }}
# # </style>
# # """, unsafe_allow_html=True)

# # # ============================================================
# # # FONCTIONS UTILITAIRES
# # # ============================================================
# # @st.cache_data(ttl=30)
# # def get_stats(sentiment_filter=None):
# #     """Récupère les statistiques"""
# #     query = {}
# #     if sentiment_filter and "Tous" not in sentiment_filter:
# #         query["label_final"] = {"$in": sentiment_filter}
    
# #     total = collection.count_documents(query)
# #     positif = collection.count_documents({**query, "label_final": "positif"})
# #     negatif = collection.count_documents({**query, "label_final": "negatif"})
# #     neutre = collection.count_documents({**query, "label_final": "neutre"})
    
# #     return {
# #         "total": total,
# #         "positif": positif,
# #         "negatif": negatif,
# #         "neutre": neutre,
# #         "taux_pos": round(positif/total*100, 1) if total else 0,
# #         "taux_neg": round(negatif/total*100, 1) if total else 0
# #     }

# # @st.cache_data(ttl=30)
# # def get_comments(limit=20, sentiment_filter=None):
# #     """Récupère les commentaires"""
# #     query = {}
# #     if sentiment_filter and "Tous" not in sentiment_filter:
# #         query["label_final"] = {"$in": sentiment_filter}
    
# #     data = list(collection.find(query).limit(limit))
# #     for doc in data:
# #         doc["_id"] = str(doc["_id"])
# #     return data

# # # Couleurs
# # COLOR_MAP = {
# #     'positif': T['accent'],
# #     'negatif': '#EF4444',
# #     'neutre': T['accent2'],
# # }

# # # ============================================================
# # # SIDEBAR - FILTRES
# # # ============================================================
# # with st.sidebar:
# #     st.markdown(f"""
# #     <div style="margin-bottom: 2rem;">
# #         <h2 style="margin: 0 0 0.2rem 0; color: {T['text']};">📡 TélécomDZ</h2>
# #         <div style="font-size: 0.7rem; color: {T['text3']}; text-transform: uppercase;">Analyse des sentiments</div>
# #     </div>
# #     """, unsafe_allow_html=True)
    
# #     st.markdown("---")
# #     st.markdown('<div class="filter-label">🔍 FILTRES</div>', unsafe_allow_html=True)
    
# #     # Filtre sentiment
# #     sentiment_options = ["Tous", "positif", "negatif", "neutre"]
# #     selected_sentiments = st.multiselect(
# #         "Sentiment",
# #         sentiment_options,
# #         default=["Tous"]
# #     )
    
# #     if "Tous" in selected_sentiments:
# #         sentiment_filter = None
# #     else:
# #         sentiment_filter = selected_sentiments if selected_sentiments else None
    
# #     st.markdown("---")
# #     st.markdown('<div class="filter-label">📊 Statistiques</div>', unsafe_allow_html=True)
    
# #     # Afficher un résumé des stats
# #     stats_summary = get_stats(sentiment_filter)
# #     st.markdown(f"""
# #     <div style="font-size: 0.8rem; color: {T['text2']}; line-height: 1.6;">
# #         <span style="color: {T['accent']};">●</span> Total: {stats_summary['total']:,}<br>
# #         <span style="color: {T['accent']};">●</span> Positifs: {stats_summary['positif']:,}<br>
# #         <span style="color: #EF4444;">●</span> Négatifs: {stats_summary['negatif']:,}<br>
# #         <span style="color: {T['accent2']};">●</span> Neutres: {stats_summary['neutre']:,}
# #     </div>
# #     """, unsafe_allow_html=True)
    
# #     # Espacement automatique
# #     st.markdown("<br>" * 10, unsafe_allow_html=True)
    
# #     # ============================================================
# #     # DEUX BOUTONS EN BAS DE LA SIDEBAR (côte à côte)
# #     # ============================================================
# #     st.markdown("---")
# #     st.markdown('<div class="filter-label">🎨 THÈME</div>', unsafe_allow_html=True)
    
# #     # Deux colonnes pour les boutons côte à côte
# #     col_dark, col_light = st.columns(2)
    
# #     with col_dark:
# #         # Bouton mode sombre (🌙)
# #         dark_active = st.session_state.theme == "dark"
# #         btn_style = "🌙" if not dark_active else "✅ 🌙"
# #         if st.button(btn_style, key="btn_dark", use_container_width=True):
# #             set_theme("dark")
    
# #     with col_light:
# #         # Bouton mode clair (☀️)
# #         light_active = st.session_state.theme == "light"
# #         btn_style = "☀️" if not light_active else "✅ ☀️"
# #         if st.button(btn_style, key="btn_light", use_container_width=True):
# #             set_theme("light")

# # # ============================================================
# # # CHARGEMENT DES DONNÉES
# # # ============================================================
# # stats = get_stats(sentiment_filter)
# # comments = get_comments(15, sentiment_filter)

# # # ============================================================
# # # EN-TÊTE
# # # ============================================================
# # st.markdown(f"""
# # <div style="margin-bottom: 2rem;">
# #     <h1 style="margin: 0 0 0.2rem 0;">📊 Vue d'ensemble</h1>
# #     <div style="color: {T['text3']};">Analyse globale des sentiments clients · Télécom Algérie</div>
# #     <div style="font-size: 0.7rem; color: {T['text3']}; margin-top: 0.5rem;">🔄 Dernière mise à jour: {datetime.now().strftime('%H:%M:%S')}</div>
# # </div>
# # """, unsafe_allow_html=True)

# # # ============================================================
# # # KPI CARDS
# # # ============================================================
# # col1, col2, col3, col4 = st.columns(4)

# # with col1:
# #     st.markdown(f"""
# #     <div class="kpi-card">
# #         <div class="kpi-label">📝 Total commentaires</div>
# #         <div class="kpi-value" style="color: {T['accent']};">{stats['total']:,}</div>
# #     </div>
# #     """, unsafe_allow_html=True)

# # with col2:
# #     st.markdown(f"""
# #     <div class="kpi-card">
# #         <div class="kpi-label">😊 Positifs</div>
# #         <div class="kpi-value" style="color: {T['accent']};">{stats['positif']:,} ({stats['taux_pos']}%)</div>
# #     </div>
# #     """, unsafe_allow_html=True)

# # with col3:
# #     st.markdown(f"""
# #     <div class="kpi-card">
# #         <div class="kpi-label">😠 Négatifs</div>
# #         <div class="kpi-value" style="color: #EF4444;">{stats['negatif']:,} ({stats['taux_neg']}%)</div>
# #     </div>
# #     """, unsafe_allow_html=True)

# # with col4:
# #     st.markdown(f"""
# #     <div class="kpi-card">
# #         <div class="kpi-label">😐 Neutres</div>
# #         <div class="kpi-value" style="color: {T['accent2']};">{stats['neutre']:,}</div>
# #     </div>
# #     """, unsafe_allow_html=True)

# # st.markdown("---")

# # # ============================================================
# # # GRAPHIQUES
# # # ============================================================
# # col_chart1, col_chart2 = st.columns(2)

# # with col_chart1:
# #     st.markdown('<div class="card">', unsafe_allow_html=True)
# #     st.markdown('<div class="card-title">Distribution des sentiments</div>', unsafe_allow_html=True)
    
# #     fig_bar = px.bar(
# #         x=['Positifs', 'Négatifs', 'Neutres'],
# #         y=[stats['positif'], stats['negatif'], stats['neutre']],
# #         color=['Positifs', 'Négatifs', 'Neutres'],
# #         color_discrete_map={
# #             'Positifs': T['accent'],
# #             'Négatifs': '#EF4444',
# #             'Neutres': T['accent2']
# #         },
# #         text_auto=True
# #     )
# #     fig_bar.update_layout(
# #         plot_bgcolor='rgba(0,0,0,0)',
# #         paper_bgcolor='rgba(0,0,0,0)',
# #         font=dict(color=T['text']),
# #         showlegend=False,
# #         height=350,
# #         xaxis=dict(gridcolor=T['border2']),
# #         yaxis=dict(gridcolor=T['border2'])
# #     )
# #     fig_bar.update_traces(textfont_color=T['text'])
# #     st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False})
# #     st.markdown('</div>', unsafe_allow_html=True)

# # with col_chart2:
# #     st.markdown('<div class="card">', unsafe_allow_html=True)
# #     st.markdown('<div class="card-title">Répartition</div>', unsafe_allow_html=True)
    
# #     fig_pie = px.pie(
# #         values=[stats['positif'], stats['negatif'], stats['neutre']],
# #         names=['Positifs', 'Négatifs', 'Neutres'],
# #         color=['Positifs', 'Négatifs', 'Neutres'],
# #         color_discrete_map={
# #             'Positifs': T['accent'],
# #             'Négatifs': '#EF4444',
# #             'Neutres': T['accent2']
# #         },
# #         hole=0.5
# #     )
# #     fig_pie.update_layout(
# #         plot_bgcolor='rgba(0,0,0,0)',
# #         paper_bgcolor='rgba(0,0,0,0)',
# #         font=dict(color=T['text']),
# #         showlegend=True,
# #         height=350
# #     )
# #     fig_pie.update_traces(textinfo="percent", textfont_color=T['text'])
# #     st.plotly_chart(fig_pie, use_container_width=True, config={'displayModeBar': False})
# #     st.markdown('</div>', unsafe_allow_html=True)

# # # ============================================================
# # # DERNIERS COMMENTAIRES
# # # ============================================================
# # st.markdown('<div class="card">', unsafe_allow_html=True)
# # st.markdown('<div class="card-title">📝 Derniers commentaires</div>', unsafe_allow_html=True)

# # for comment in comments:
# #     sentiment = comment.get('label_final', 'neutre')
# #     comment_text = comment.get('Commentaire_Client', '')[:200]
# #     if len(comment_text) > 150:
# #         comment_text = comment_text[:150] + "..."
# #     source = comment.get('sources', 'inconnue')
# #     date = comment.get('dates', 'Date inconnue')
    
# #     border_color = T['accent'] if sentiment == 'positif' else '#EF4444' if sentiment == 'negatif' else T['accent2']
# #     text_color = T['accent'] if sentiment == 'positif' else '#EF4444' if sentiment == 'negatif' else T['accent2']
    
# #     st.markdown(f"""
# #     <div class="comment-card" style="border-left-color: {border_color};">
# #         <div class="comment-sentiment" style="color: {text_color};">{sentiment.upper()}</div>
# #         <div class="comment-text">{comment_text}</div>
# #         <div class="comment-meta">📱 {source} | 📅 {date}</div>
# #     </div>
# #     """, unsafe_allow_html=True)

# # st.markdown('</div>', unsafe_allow_html=True)

# # # ============================================================
# # # BOUTON RAFRAÎCHIR
# # # ============================================================
# # if st.button("🔄 Rafraîchir les données", use_container_width=True):
# #     st.cache_data.clear()
# #     st.rerun()


# # app.py - Application complète avec login
# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# from pymongo import MongoClient
# from datetime import datetime
# import hashlib
# import re

# # ============================================================
# # CONFIGURATION DE LA PAGE (UNE SEULE FOIS, TOUT EN HAUT)
# # ============================================================
# st.set_page_config(
#     page_title="Télécom Algérie - Authentification",
#     page_icon="🔐",
#     layout="wide",
#     initial_sidebar_state="collapsed"
# )

# # ============================================================
# # SESSION STATE
# # ============================================================
# if "authenticated" not in st.session_state:
#     st.session_state.authenticated = False
# if "username" not in st.session_state:
#     st.session_state.username = None
# if "theme" not in st.session_state:
#     st.session_state.theme = "light"  # Changé en light par défaut

# # ============================================================
# # FONCTIONS D'AUTHENTIFICATION
# # ============================================================
# def hash_password(password):
#     """Hache un mot de passe"""
#     return hashlib.sha256(password.encode()).hexdigest()

# def verify_credentials(email, password):
#     """Vérifie les identifiants"""
#     # Exemple d'utilisateurs (à remplacer par MongoDB)
#     USERS = {
#         "admin@telecom.dz": hash_password("admin123"),
#         "user@telecom.dz": hash_password("user123"),
#         "demo@telecom.dz": hash_password("demo123")
#     }
    
#     if email in USERS and USERS[email] == hash_password(password):
#         return True
#     return False

# def validate_email(email):
#     """Valide le format de l'email"""
#     pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
#     return re.match(pattern, email) is not None

# # ============================================================
# # STYLE CSS POUR LA PAGE DE LOGIN (FOND BLEU/BLANC)
# # ============================================================
# def show_login():
#     """Affiche la page de connexion"""
    
#     # CSS pour la page de login - fond bleu/gradient
#     st.markdown("""
#     <style>
#         /* Reset et fond - Gradient bleu */
#         .stApp {
#             background: linear-gradient(135deg, #1e3c72 0%, #2a5298 50%, #0f2027 100%) !important;
#         }
        
#         /* Conteneur principal du login - centrage vertical */
#         .login-container {
#             display: flex;
#             justify-content: center;
#             align-items: center;
#             min-height: 100vh;
#             padding: 1rem;
#         }
        
#         /* Carte de login - fond blanc */
#         .login-card {
#             background: #ffffff;
#             border: none;
#             border-radius: 28px;
#             padding: 2.5rem;
#             max-width: 450px;
#             width: 100%;
#             box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
#             transition: all 0.3s ease;
#         }
        
#         .login-card:hover {
#             transform: translateY(-5px);
#             box-shadow: 0 30px 60px -12px rgba(0, 0, 0, 0.3);
#         }
        
#         /* Logo et titre */
#         .logo {
#             text-align: center;
#             margin-bottom: 2rem;
#         }
        
#         .logo-icon {
#             font-size: 4rem;
#             margin-bottom: 1rem;
#         }
        
#         .logo h1 {
#             font-size: 1.8rem;
#             font-weight: 700;
#             background: linear-gradient(135deg, #1e3c72, #2a5298);
#             -webkit-background-clip: text;
#             -webkit-text-fill-color: transparent;
#             background-clip: text;
#             margin: 0;
#         }
        
#         .logo p {
#             color: #6c757d;
#             font-size: 0.8rem;
#             margin-top: 0.5rem;
#         }
        
#         /* Champs de formulaire */
#         .input-group {
#             margin-bottom: 1.5rem;
#         }
        
#         .input-label {
#             display: block;
#             font-size: 0.75rem;
#             text-transform: uppercase;
#             letter-spacing: 0.05em;
#             color: #6c757d;
#             margin-bottom: 0.5rem;
#             font-weight: 600;
#         }
        
#         /* Input fields */
#         .stTextInput > div > div > input {
#             background: #f8f9fa !important;
#             border: 1px solid #e9ecef !important;
#             border-radius: 12px !important;
#             padding: 0.75rem !important;
#             color: #1a1a2e !important;
#         }
        
#         .stTextInput > div > div > input:focus {
#             border-color: #2a5298 !important;
#             box-shadow: 0 0 0 2px rgba(42, 82, 152, 0.2) !important;
#         }
        
#         /* Messages d'erreur */
#         .error-message {
#             background: rgba(220, 53, 69, 0.1);
#             border: 1px solid rgba(220, 53, 69, 0.3);
#             border-radius: 12px;
#             padding: 0.75rem;
#             margin-bottom: 1rem;
#             color: #dc3545;
#             font-size: 0.8rem;
#             text-align: center;
#         }
        
#         /* Bouton de connexion */
#         .stButton > button {
#             width: 100%;
#             background: linear-gradient(135deg, #1e3c72, #2a5298) !important;
#             border: none !important;
#             border-radius: 12px !important;
#             padding: 0.75rem !important;
#             font-weight: 600 !important;
#             font-size: 1rem !important;
#             color: white !important;
#             transition: all 0.2s !important;
#         }
        
#         .stButton > button:hover {
#             opacity: 0.9;
#             transform: translateY(-2px);
#         }
        
#         /* Liens */
#         .links {
#             text-align: center;
#             margin-top: 1.5rem;
#             font-size: 0.75rem;
#             color: #6c757d;
#         }
        
#         .links a {
#             color: #2a5298;
#             text-decoration: none;
#         }
        
#         .links a:hover {
#             text-decoration: underline;
#         }
        
#         /* Footer */
#         .login-footer {
#             text-align: center;
#             margin-top: 2rem;
#             font-size: 0.7rem;
#             color: #adb5bd;
#         }
        
#         /* Hide Streamlit elements */
#         #MainMenu {visibility: hidden;}
#         footer {visibility: hidden;}
#         header {visibility: hidden;}
#     </style>
#     """, unsafe_allow_html=True)
    
#     # Conteneur centré (verticalement et horizontalement)
#     st.markdown('<div class="login-container">', unsafe_allow_html=True)
    
#     # Carte de login
#     st.markdown('<div class="login-card">', unsafe_allow_html=True)
    
#     # Logo et titre
#     st.markdown("""
#     <div class="logo">
#         <div class="logo-icon">📡</div>
#         <h1>Télécom Algérie</h1>
#         <p>Plateforme d'analyse des sentiments clients</p>
#     </div>
#     """, unsafe_allow_html=True)
    
#     # Formulaire de connexion
#     with st.form("login_form"):
#         st.markdown('<div class="input-label">📧 EMAIL</div>', unsafe_allow_html=True)
#         email = st.text_input("", placeholder="exemple@telecom.dz", label_visibility="collapsed")
        
#         st.markdown('<div class="input-label" style="margin-top: 1rem;">🔒 MOT DE PASSE</div>', unsafe_allow_html=True)
#         password = st.text_input("", type="password", placeholder="••••••••", label_visibility="collapsed")
        
#         submitted = st.form_submit_button("🔐 SE CONNECTER", use_container_width=True)
        
#         if submitted:
#             if not email or not password:
#                 st.markdown('<div class="error-message">❌ Veuillez remplir tous les champs</div>', unsafe_allow_html=True)
#             elif not validate_email(email):
#                 st.markdown('<div class="error-message">❌ Format d\'email invalide</div>', unsafe_allow_html=True)
#             elif verify_credentials(email, password):
#                 st.session_state.authenticated = True
#                 st.session_state.username = email.split('@')[0]
#                 st.rerun()
#             else:
#                 st.markdown('<div class="error-message">❌ Email ou mot de passe incorrect</div>', unsafe_allow_html=True)
    
#     # Liens supplémentaires
#     st.markdown("""
#     <div class="links">
#         <a href="#">Mot de passe oublié ?</a> | <a href="#">Créer un compte</a>
#     </div>
#     """, unsafe_allow_html=True)
    
#     # Footer
#     st.markdown("""
#     <div class="login-footer">
#         © 2024 Télécom Algérie - Tous droits réservés
#     </div>
#     """, unsafe_allow_html=True)
    
#     st.markdown('</div>', unsafe_allow_html=True)
#     st.markdown('</div>', unsafe_allow_html=True)


# # ============================================================
# # DASHBOARD (après connexion)
# # ============================================================
# def show_dashboard():
#     """Affiche le dashboard après connexion"""
    
#     # ============================================================
#     # THÈMES
#     # ============================================================
#     THEMES = {
#         "dark": {
#             "bg": "#272757", "bg2": "#0d0e14", "bg3": "#0f1117",
#             "border": "#1a1d2e", "border2": "#2a2d3e",
#             "text": "#e8e6e0", "text2": "#9ca3af", "text3": "#4b5563",
#             "accent": "#10B981", "accent2": "#3B82F6", "card_bg": "#0d0e14",
#         },
#         "light": {
#             "bg": "#D3D3D3", "bg2": "#ffffff", "bg3": "#f8fafc",
#             "border": "#e2e8f0", "border2": "#cbd5e1",
#             "text": "#1e293b", "text2": "#475569", "text3": "#94a3b8",
#             "accent": "#10B981", "accent2": "#3B82F6", "card_bg": "#ffffff",
#         }
#     }
    
#     T = THEMES[st.session_state.theme]
    
#     # ============================================================
#     # CONNEXION MONGODB
#     # ============================================================
#     MONGO_URI = "mongodb://localhost:27018/"
#     client = MongoClient(MONGO_URI)
#     db = client["telecom_algerie"]
#     collection = db["dataset_unifie"]
    
#     # ============================================================
#     # STYLE CSS DASHBOARD
#     # ============================================================
#     st.markdown(f"""
#     <style>
#         .stApp {{ background-color: {T['bg']}; }}
#         .card {{
#             background: {T['card_bg']};
#             border: 1px solid {T['border']};
#             border-radius: 16px;
#             padding: 1.2rem;
#             margin-bottom: 1rem;
#             transition: all 0.2s;
#         }}
#         .card:hover {{ border-color: {T['accent']}; transform: translateY(-2px); }}
#         .card-title {{
#             font-size: 0.7rem;
#             text-transform: uppercase;
#             letter-spacing: 0.08em;
#             color: {T['text3']};
#             margin-bottom: 0.8rem;
#             font-weight: 600;
#         }}
#         .kpi-card {{
#             background: linear-gradient(135deg, {T['bg2']}, {T['bg']});
#             border: 1px solid {T['border']};
#             border-radius: 20px;
#             padding: 1rem;
#             text-align: center;
#             transition: all 0.2s;
#         }}
#         .kpi-card:hover {{ border-color: {T['accent']}; transform: translateY(-2px); }}
#         .kpi-value {{ font-size: 1.8rem; font-weight: 700; color: {T['text']}; }}
#         .kpi-label {{ font-size: 0.7rem; color: {T['text3']}; text-transform: uppercase; letter-spacing: 0.05em; }}
#         .comment-card {{
#             background: {T['bg3']};
#             border-left: 3px solid;
#             border-radius: 12px;
#             padding: 0.8rem 1rem;
#             margin: 0.75rem 0;
#             transition: all 0.2s;
#         }}
#         .comment-card:hover {{ transform: translateX(5px); }}
#         .comment-sentiment {{ font-weight: 600; font-size: 0.7rem; text-transform: uppercase; margin-bottom: 0.3rem; }}
#         .comment-text {{ color: {T['text2']}; font-size: 0.85rem; margin: 0.5rem 0; line-height: 1.5; }}
#         .comment-meta {{ font-size: 0.65rem; color: {T['text3']}; }}
#         .filter-label {{ font-size: 0.7rem; color: {T['text3']}; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; display: block; }}
#         [data-testid="stSidebar"] {{ background: {T['bg2']}; border-right: 1px solid {T['border']}; }}
#         [data-testid="stSidebar"] * {{ color: {T['text']} !important; }}
#         h1, h2, h3, h4, h5, h6 {{ color: {T['text']} !important; }}
#         hr {{ border-color: {T['border']} !important; }}
#         .stButton > button {{
#             background: linear-gradient(135deg, {T['accent']}, {T['accent2']}) !important;
#             color: white !important;
#             border: none !important;
#             border-radius: 10px !important;
#             padding: 0.5rem 1rem !important;
#             font-weight: 600 !important;
#         }}
#         .stButton > button:hover {{ opacity: 0.9; transform: translateY(-1px); }}
#         .logout-btn button {{
#             background: linear-gradient(135deg, #ef4444, #dc2626) !important;
#         }}
#         #MainMenu {{visibility: hidden;}}
#         footer {{visibility: hidden;}}
#         header {{visibility: hidden;}}
#     </style>
#     """, unsafe_allow_html=True)
    
#     # ============================================================
#     # FONCTIONS
#     # ============================================================
#     @st.cache_data(ttl=30)
#     def get_stats(sentiment_filter=None):
#         query = {}
#         if sentiment_filter and "Tous" not in sentiment_filter:
#             query["label_final"] = {"$in": sentiment_filter}
#         total = collection.count_documents(query)
#         positif = collection.count_documents({**query, "label_final": "positif"})
#         negatif = collection.count_documents({**query, "label_final": "negatif"})
#         neutre = collection.count_documents({**query, "label_final": "neutre"})
#         return {
#             "total": total, "positif": positif, "negatif": negatif, "neutre": neutre,
#             "taux_pos": round(positif/total*100, 1) if total else 0,
#             "taux_neg": round(negatif/total*100, 1) if total else 0
#         }
    
#     @st.cache_data(ttl=30)
#     def get_comments(limit=20, sentiment_filter=None):
#         query = {}
#         if sentiment_filter and "Tous" not in sentiment_filter:
#             query["label_final"] = {"$in": sentiment_filter}
#         data = list(collection.find(query).limit(limit))
#         for doc in data:
#             doc["_id"] = str(doc["_id"])
#         return data
    
#     # ============================================================
#     # SIDEBAR
#     # ============================================================
#     with st.sidebar:
#         st.markdown(f"""
#         <div style="margin-bottom: 2rem;">
#             <h2 style="margin: 0;">📡 TélécomDZ</h2>
#             <div style="font-size: 0.7rem; color: {T['text3']};">Bonjour, {st.session_state.username} 👋</div>
#         </div>
#         """, unsafe_allow_html=True)
        
#         st.markdown("---")
#         st.markdown('<div class="filter-label">🔍 FILTRES</div>', unsafe_allow_html=True)
        
#         sentiment_options = ["Tous", "positif", "negatif", "neutre"]
#         selected_sentiments = st.multiselect("Sentiment", sentiment_options, default=["Tous"])
        
#         if "Tous" in selected_sentiments:
#             sentiment_filter = None
#         else:
#             sentiment_filter = selected_sentiments if selected_sentiments else None
        
#         st.markdown("---")
        
#         # Boutons thème
#         st.markdown('<div class="filter-label">🎨 THÈME</div>', unsafe_allow_html=True)
#         col_dark, col_light = st.columns(2)
#         with col_dark:
#             if st.button("🌙 Dark", key="btn_dark", use_container_width=True):
#                 st.session_state.theme = "dark"
#                 st.rerun()
#         with col_light:
#             if st.button("☀️ Light", key="btn_light", use_container_width=True):
#                 st.session_state.theme = "light"
#                 st.rerun()
        
#         st.markdown("---")
        
#         # Bouton déconnexion
#         st.markdown('<div class="filter-label">👤 COMPTE</div>', unsafe_allow_html=True)
#         if st.button("🚪 Se déconnecter", key="logout", use_container_width=True):
#             st.session_state.authenticated = False
#             st.session_state.username = None
#             st.rerun()
    
#     # ============================================================
#     # CHARGEMENT DES DONNÉES
#     # ============================================================
#     stats = get_stats(sentiment_filter)
#     comments = get_comments(15, sentiment_filter)
    
#     # ============================================================
#     # EN-TÊTE
#     # ============================================================
#     st.markdown(f"""
#     <div style="margin-bottom: 2rem;">
#         <h1 style="margin: 0;">📊 Vue d'ensemble</h1>
#         <div style="color: {T['text3']};">Analyse des sentiments clients · Télécom Algérie</div>
#         <div style="font-size: 0.7rem; color: {T['text3']};">🔄 {datetime.now().strftime('%H:%M:%S')}</div>
#     </div>
#     """, unsafe_allow_html=True)
    
#     # ============================================================
#     # KPI CARDS
#     # ============================================================
#     col1, col2, col3, col4 = st.columns(4)
    
#     with col1:
#         st.markdown(f"""
#         <div class="kpi-card">
#             <div class="kpi-label">📝 Total</div>
#             <div class="kpi-value" style="color: {T['accent']};">{stats['total']:,}</div>
#         </div>
#         """, unsafe_allow_html=True)
    
#     with col2:
#         st.markdown(f"""
#         <div class="kpi-card">
#             <div class="kpi-label">😊 Positifs</div>
#             <div class="kpi-value" style="color: {T['accent']};">{stats['positif']:,} ({stats['taux_pos']}%)</div>
#         </div>
#         """, unsafe_allow_html=True)
    
#     with col3:
#         st.markdown(f"""
#         <div class="kpi-card">
#             <div class="kpi-label">😠 Négatifs</div>
#             <div class="kpi-value" style="color: #EF4444;">{stats['negatif']:,} ({stats['taux_neg']}%)</div>
#         </div>
#         """, unsafe_allow_html=True)
    
#     with col4:
#         st.markdown(f"""
#         <div class="kpi-card">
#             <div class="kpi-label">😐 Neutres</div>
#             <div class="kpi-value" style="color: {T['accent2']};">{stats['neutre']:,}</div>
#         </div>
#         """, unsafe_allow_html=True)
    
#     # ============================================================
#     # GRAPHIQUES
#     # ============================================================
#     col_chart1, col_chart2 = st.columns(2)
    
#     with col_chart1:
#         st.markdown('<div class="card">', unsafe_allow_html=True)
#         st.markdown('<div class="card-title">Distribution</div>', unsafe_allow_html=True)
#         fig_bar = px.bar(
#             x=['Positifs', 'Négatifs', 'Neutres'],
#             y=[stats['positif'], stats['negatif'], stats['neutre']],
#             color=['Positifs', 'Négatifs', 'Neutres'],
#             color_discrete_map={'Positifs': T['accent'], 'Négatifs': '#EF4444', 'Neutres': T['accent2']},
#             text_auto=True
#         )
#         fig_bar.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color=T['text']), showlegend=False, height=350)
#         st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False})
#         st.markdown('</div>', unsafe_allow_html=True)
    
#     with col_chart2:
#         st.markdown('<div class="card">', unsafe_allow_html=True)
#         st.markdown('<div class="card-title">Répartition</div>', unsafe_allow_html=True)
#         fig_pie = px.pie(
#             values=[stats['positif'], stats['negatif'], stats['neutre']],
#             names=['Positifs', 'Négatifs', 'Neutres'],
#             color=['Positifs', 'Négatifs', 'Neutres'],
#             color_discrete_map={'Positifs': T['accent'], 'Négatifs': '#EF4444', 'Neutres': T['accent2']},
#             hole=0.5
#         )
#         fig_pie.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color=T['text']), height=350)
#         st.plotly_chart(fig_pie, use_container_width=True, config={'displayModeBar': False})
#         st.markdown('</div>', unsafe_allow_html=True)
    
#     # ============================================================
#     # COMMENTAIRES
#     # ============================================================
#     st.markdown('<div class="card">', unsafe_allow_html=True)
#     st.markdown('<div class="card-title">📝 Derniers commentaires</div>', unsafe_allow_html=True)
    
#     for comment in comments:
#         sentiment = comment.get('label_final', 'neutre')
#         comment_text = comment.get('Commentaire_Client', '')[:200]
#         if len(comment_text) > 150:
#             comment_text = comment_text[:150] + "..."
#         source = comment.get('sources', 'inconnue')
#         date = comment.get('dates', 'Date inconnue')
        
#         border_color = T['accent'] if sentiment == 'positif' else '#EF4444' if sentiment == 'negatif' else T['accent2']
#         text_color = T['accent'] if sentiment == 'positif' else '#EF4444' if sentiment == 'negatif' else T['accent2']
        
#         st.markdown(f"""
#         <div class="comment-card" style="border-left-color: {border_color};">
#             <div class="comment-sentiment" style="color: {text_color};">{sentiment.upper()}</div>
#             <div class="comment-text">{comment_text}</div>
#             <div class="comment-meta">📱 {source} | 📅 {date}</div>
#         </div>
#         """, unsafe_allow_html=True)
    
#     st.markdown('</div>', unsafe_allow_html=True)
    
#     if st.button("🔄 Rafraîchir", use_container_width=True):
#         st.cache_data.clear()
#         st.rerun()


# # ============================================================
# # POINT D'ENTRÉE PRINCIPAL
# # ============================================================
# if st.session_state.authenticated:
#     show_dashboard()
# else:
#     show_login()

import streamlit as st

st.set_page_config(
    page_title="Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS global
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');

* { font-family: 'Plus Jakarta Sans', sans-serif !important; }

/* Hide Streamlit defaults */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.5rem 2rem !important; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #ffffff;
    border-right: 1px solid #f0f0f5;
    min-width: 240px !important;
    max-width: 240px !important;
}
[data-testid="stSidebar"] .block-container { padding: 1.5rem 1rem !important; }

/* Cards */
.metric-card {
    background: white;
    border-radius: 16px;
    padding: 1.2rem 1.4rem;
    border: 1px solid #f0f0f5;
    box-shadow: 0 2px 12px rgba(0,0,0,0.04);
    transition: box-shadow 0.2s;
}
.metric-card:hover { box-shadow: 0 6px 24px rgba(0,0,0,0.08); }

/* Nav items */
.nav-item {
    display: flex; align-items: center; gap: 10px;
    padding: 9px 12px; border-radius: 10px;
    color: #888; font-size: 0.88rem; font-weight: 500;
    cursor: pointer; margin-bottom: 4px;
    transition: all 0.15s;
}
.nav-item:hover { background: #fff5ef; color: #072e8d; }
.nav-item.active { background: #fff5ef; color: #072e8d; font-weight: 600; }

/* Orange accent */
.accent { color: #072e8d; }
.badge {
    background: #072e8d; color: white;
    border-radius: 50%; width: 18px; height: 18px;
    font-size: 0.7rem; display: inline-flex;
    align-items: center; justify-content: center;
    font-weight: 700;
}

/* Buttons */
.stButton > button {
    background: #072e8d !important;
    color: white !important;
    border: none !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.2rem !important;
}
.stButton > button:hover { background: #e55a1f !important; }

/* Section title */
.section-title {
    font-size: 1.5rem; font-weight: 800;
    color: #1a1a2e; margin-bottom: 2px;
}
.section-sub { font-size: 0.82rem; color: #aaa; margin-bottom: 1.2rem; }

/* Progress bar */
.progress-bar {
    height: 4px; background: #f0f0f5;
    border-radius: 4px; overflow: hidden; margin-top: 8px;
}
.progress-fill {
    height: 100%; border-radius: 4px;
    background: linear-gradient(90deg, #3D3B8E, #6C63FF);
}

/* Stat mini */
.stat-up { color: #22c55e; font-size: 0.78rem; font-weight: 600; }
.stat-num { font-size: 1.6rem; font-weight: 800; color: #1a1a2e; }
.stat-label { font-size: 0.75rem; color: #aaa; font-weight: 500; }

/* Customer row */
.customer-row {
    display: flex; align-items: center; justify-content: space-between;
    padding: 10px 0; border-bottom: 1px solid #f8f8f8;
}
.customer-name { font-size: 0.88rem; font-weight: 600; color: #1a1a2e; }
.customer-sub { font-size: 0.73rem; color: #bbb; }

/* Profile card right */
.profile-stat { text-align: center; }
.profile-stat-num { font-size: 1.1rem; font-weight: 800; color: #1a1a2e; }
.profile-stat-label { font-size: 0.7rem; color: #bbb; }

/* Donut label */
.donut-label { text-align: center; margin-top: -10px; }

/* Financial row */
.fin-card {
    background: white; border-radius: 12px;
    padding: 0.9rem 1.1rem; border: 1px solid #f0f0f5;
    margin-bottom: 10px;
}
.fin-label { font-size: 0.72rem; color: #bbb; font-weight: 500; }
.fin-value { font-size: 1.1rem; font-weight: 700; color: #1a1a2e; }

/* Action card right */
.action-row {
    display: flex; align-items: center; justify-content: space-between;
    background: white; border-radius: 14px;
    padding: 1rem 1.2rem; border: 1px solid #f0f0f5;
    margin-bottom: 10px; cursor: pointer;
}
.action-icon {
    width: 38px; height: 38px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 1rem;
}

/* Subscription banner */
.sub-banner {
    background: linear-gradient(135deg, #fff8f3, #fff0e8);
    border-radius: 14px; padding: 1rem 1.3rem;
    border: 1px solid #ffe5d0;
    display: flex; align-items: center; gap: 10px;
    font-size: 0.82rem; color: #b05a20; font-weight: 600;
}

/* Tabs */
[data-baseweb="tab"] { font-family: 'Plus Jakarta Sans', sans-serif !important; }
</style>
""", unsafe_allow_html=True)

from pages_content import sidebar, main_dashboard, inbox_page, accounts_page, meetings_page, settings_page

# Session state for navigation
if "page" not in st.session_state:
    st.session_state.page = "Dashboard"

# Sidebar navigation
sidebar()

# Route pages
page = st.session_state.page
if page == "Dashboard":
    main_dashboard()
elif page == "Inbox":
    inbox_page()
elif page == "Accounts":
    accounts_page()
elif page == "Meetings":
    meetings_page()
elif page == "Settings":
    settings_page()