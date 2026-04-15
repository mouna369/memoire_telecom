# # # # pages/1_Overview.py
# # # import streamlit as st
# # # from pymongo import MongoClient
# # # import pandas as pd
# # # import plotly.express as px
# # # from datetime import datetime
# # # import os

# # # # PAS de st.set_page_config() ici

# # # # ============================================
# # # # CONNEXION MONGODB
# # # # ============================================
# # # MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018/")

# # # @st.cache_resource
# # # def init_connection():
# # #     try:
# # #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
# # #         client.admin.command('ping')
# # #         return client
# # #     except Exception as e:
# # #         st.error(f"❌ Erreur de connexion MongoDB: {e}")
# # #         return None

# # # client = init_connection()

# # # if client is None:
# # #     st.stop()

# # # db = client["telecom_algerie"]
# # # collection = db["dataset_unifie"]

# # # st.title("📊 Vue d'ensemble - Analyse des sentiments")

# # # # ============================================
# # # # SIDEBAR - FILTRES (CORRIGÉ)
# # # # ============================================
# # # st.sidebar.header("🔍 Filtres")

# # # # Option "Tous" gérée correctement
# # # sentiment_options = ["Tous", "positif", "negatif", "neutre", "mixed"]
# # # selected_sentiments = st.sidebar.multiselect(
# # #     "Sentiment",
# # #     sentiment_options,
# # #     default=["Tous"]  # ← Par défaut, "Tous" est sélectionné
# # # )

# # # # CORRECTION ICI : gérer "Tous" correctement
# # # if "Tous" in selected_sentiments:
# # #     # Si "Tous" est sélectionné, ne filtrer sur aucun sentiment
# # #     sentiment_filter = None
# # #     filter_display = "Tous les sentiments"
# # # else:
# # #     # Sinon, filtrer sur les sentiments sélectionnés
# # #     sentiment_filter = selected_sentiments
# # #     filter_display = ", ".join(selected_sentiments)

# # # st.sidebar.caption(f"Filtre actif : {filter_display}")

# # # # ============================================
# # # # CHARGEMENT DES DONNÉES (SANS LIMITE)
# # # # ============================================
# # # @st.cache_data(ttl=3600)
# # # def load_data(sentiments):
# # #     """Charge TOUTES les données depuis MongoDB"""
# # #     try:
# # #         query = {}
# # #         # Appliquer le filtre seulement si des sentiments spécifiques sont sélectionnés
# # #         if sentiments and "Tous" not in sentiments:
# # #             query["label_final"] = {"$in": sentiments}
        
# # #         # Charger TOUTES les données (pas de limite)
# # #         cursor = collection.find(query)
# # #         data = list(cursor)
        
# # #         if data:
# # #             df = pd.DataFrame(data)
# # #             return df
# # #         else:
# # #             return pd.DataFrame()
# # #     except Exception as e:
# # #         st.error(f"Erreur de chargement: {e}")
# # #         return pd.DataFrame()

# # # with st.spinner("Chargement des données..."):
# # #     df = load_data(sentiment_filter)

# # # if df.empty:
# # #     st.warning("⚠️ Aucune donnée trouvée.")
# # #     st.stop()

# # # # ============================================
# # # # AFFICHER LE NOMBRE TOTAL RÉEL
# # # # ============================================
# # # total_reel = len(df)
# # # st.info(f"📊 **{total_reel:,} commentaires chargés** (sur {collection.count_documents({}):,} au total)")

# # # # ============================================
# # # # KPI (Indicateurs clés)
# # # # ============================================
# # # st.subheader("📊 Indicateurs clés")

# # # col1, col2, col3, col4 = st.columns(4)

# # # # Compter par sentiment (en gérant les valeurs manquantes)
# # # positif = len(df[df['label_final'] == 'positif']) if 'label_final' in df.columns else 0
# # # negatif = len(df[df['label_final'] == 'negatif']) if 'label_final' in df.columns else 0
# # # neutre = len(df[df['label_final'] == 'neutre']) if 'label_final' in df.columns else 0
# # # autres = total_reel - (positif + negatif + neutre)

# # # taux_positif = (positif / total_reel * 100) if total_reel > 0 else 0

# # # col1.metric("📝 Total commentaires", f"{total_reel:,}")
# # # col2.metric("😊 Positifs", f"{positif:,}", delta=f"{taux_positif:.1f}%")
# # # col3.metric("😠 Négatifs", f"{negatif:,}")
# # # col4.metric("😐 Neutres/Autres", f"{neutre + autres:,}")

# # # st.markdown("---")

# # # # ============================================
# # # # GRAPHIQUE DES SENTIMENTS
# # # # ============================================
# # # st.subheader("📊 Distribution des sentiments")

# # # if 'label_final' in df.columns:
# # #     # Compter tous les sentiments
# # #     sentiment_counts = df['label_final'].value_counts().reset_index()
# # #     sentiment_counts.columns = ['Sentiment', 'Nombre']
    
# # #     # Définir les couleurs
# # #     color_map = {
# # #         'positif': '#2ecc71', 
# # #         'negatif': '#e74c3c',
# # #         'neutre': '#f39c12'
# # #     }
    
# # #     fig = px.bar(
# # #         sentiment_counts, 
# # #         x='Sentiment', 
# # #         y='Nombre', 
# # #         color='Sentiment',
# # #         color_discrete_map=color_map,
# # #         text='Nombre',
# # #         title="Répartition des sentiments"
# # #     )
# # #     fig.update_traces(textposition='outside')
# # #     st.plotly_chart(fig, use_container_width=True)

# # # # ============================================
# # # # DEUX GRAPHIQUES CÔTE À CÔTE
# # # # ============================================
# # # col1, col2 = st.columns(2)

# # # with col1:
# # #     st.subheader("🥧 Répartition")
# # #     fig_pie = px.pie(
# # #         values=sentiment_counts['Nombre'], 
# # #         names=sentiment_counts['Sentiment'],
# # #         title="Proportion des sentiments",
# # #         color=sentiment_counts['Sentiment'],
# # #         color_discrete_map=color_map
# # #     )
# # #     st.plotly_chart(fig_pie, use_container_width=True)

# # # with col2:
# # #     st.subheader("📊 Score de confiance")
# # #     if 'confidence' in df.columns:
# # #         conf_pos = df[df['label_final'] == 'positif']['confidence'].mean() if positif > 0 else 0
# # #         conf_neg = df[df['label_final'] == 'negatif']['confidence'].mean() if negatif > 0 else 0
# # #         conf_neutre = df[df['label_final'] == 'neutre']['confidence'].mean() if neutre > 0 else 0
        
# # #         conf_df = pd.DataFrame({
# # #             'Sentiment': ['Positif', 'Négatif', 'Neutre'],
# # #             'Confiance moyenne': [conf_pos, conf_neg, conf_neutre]
# # #         })
# # #         fig_conf = px.bar(conf_df, x='Sentiment', y='Confiance moyenne',
# # #                           color='Sentiment',
# # #                           color_discrete_map={'Positif': '#2ecc71', 'Négatif': '#e74c3c', 'Neutre': '#f39c12'},
# # #                           range_y=[0, 1])
# # #         st.plotly_chart(fig_conf, use_container_width=True)

# # # # ============================================
# # # # STATISTIQUES DÉTAILLÉES
# # # # ============================================
# # # with st.expander("📊 Statistiques détaillées"):
# # #     st.write("**Résumé des données**")
    
# # #     col1, col2 = st.columns(2)
    
# # #     with col1:
# # #         st.write(f"- Total commentaires: {total_reel:,}")
# # #         st.write(f"- Positifs: {positif} ({positif/total_reel*100:.1f}%)")
# # #         st.write(f"- Négatifs: {negatif} ({negatif/total_reel*100:.1f}%)")
    
# # #     with col2:
# # #         if 'sources' in df.columns:
# # #             st.write(f"- Sources: {df['sources'].nunique()}")
# # #         if 'confidence' in df.columns:
# # #             st.write(f"- Confiance moyenne: {df['confidence'].mean():.2f}")

# # # # ============================================
# # # # DERNIERS COMMENTAIRES
# # # # ============================================
# # # st.subheader(f"📝 Derniers commentaires")

# # # for idx, row in df.head(20).iterrows():
# # #     sentiment = row.get('label_final', 'unknown')
# # #     if sentiment == 'positif':
# # #         emoji = "😊"
# # #         color = "#2ecc71"
# # #     elif sentiment == 'negatif':
# # #         emoji = "😠"
# # #         color = "#e74c3c"
# # #     else:
# # #         emoji = "😐"
# # #         color = "#f39c12"
    
# # #     comment = row.get('Commentaire_Client', '')
# # #     if len(str(comment)) > 300:
# # #         comment = str(comment)[:300] + "..."
    
# # #     date = row.get('dates', 'Date inconnue')
# # #     source = row.get('sources', 'Source inconnue')
    
# # #     st.markdown(
# # #         f"""
# # #         <div style='border-left: 4px solid {color}; padding: 10px; margin: 10px 0; background-color: #f8f9fa; border-radius: 5px;'>
# # #             <b>{emoji} {sentiment.upper()}</b><br>
# # #             {comment}<br>
# # #             <small>📅 {date} | 📱 {source}</small>
# # #         </div>
# # #         """,
# # #         unsafe_allow_html=True
# # #     )

# # # # ============================================
# # # # EXPORT
# # # # ============================================
# # # with st.expander("📥 Exporter les données"):
# # #     csv = df.to_csv(index=False).encode('utf-8')
# # #     st.download_button(
# # #         label="📥 Télécharger en CSV",
# # #         data=csv,
# # #         file_name=f"telecom_sentiments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
# # #         mime="text/csv"
# # #     )


# # # pages/1_Overview.py
# # import streamlit as st
# # from pymongo import MongoClient
# # import pandas as pd
# # import plotly.express as px
# # import plotly.graph_objects as go
# # from datetime import datetime
# # import os
# # from utils.design import get_sentiment_color, get_sentiment_emoji

# # # CONNEXION MONGODB
# # MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018/")

# # @st.cache_resource
# # def init_connection():
# #     try:
# #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
# #         client.admin.command('ping')
# #         return client
# #     except Exception as e:
# #         st.error(f"❌ Erreur de connexion MongoDB: {e}")
# #         return None

# # client = init_connection()

# # if client is None:
# #     st.stop()

# # db = client["telecom_algerie"]
# # collection = db["dataset_unifie"]

# # st.title("📊 Vue d'ensemble")
# # st.markdown("### Analyse globale des sentiments clients")

# # # SIDEBAR FILTRES
# # with st.sidebar:
# #     st.markdown("### 🔍 Filtres")
    
# #     sentiment_options = ["Tous", "positif", "negatif", "neutre", "mixed"]
# #     selected_sentiments = st.multiselect(
# #         "Sentiment",
# #         sentiment_options,
# #         default=["Tous"]
# #     )
    
# #     if "Tous" in selected_sentiments:
# #         sentiment_filter = None
# #         filter_display = "Tous les sentiments"
# #     else:
# #         sentiment_filter = selected_sentiments
# #         filter_display = ", ".join(selected_sentiments)
    
# #     st.caption(f"📌 Filtre actif : {filter_display}")

# # # CHARGEMENT DES DONNÉES
# # @st.cache_data(ttl=3600)
# # def load_data(sentiments):
# #     try:
# #         query = {}
# #         if sentiments and "Tous" not in sentiments:
# #             query["label_final"] = {"$in": sentiments}
        
# #         cursor = collection.find(query)
# #         data = list(cursor)
        
# #         if data:
# #             df = pd.DataFrame(data)
# #             return df
# #         return pd.DataFrame()
# #     except Exception as e:
# #         st.error(f"Erreur de chargement: {e}")
# #         return pd.DataFrame()

# # with st.spinner("🔄 Chargement des données..."):
# #     df = load_data(sentiment_filter)

# # if df.empty:
# #     st.warning("⚠️ Aucune donnée trouvée")
# #     st.stop()

# # # KPI
# # total_reel = len(df)
# # st.markdown(f"""
# # <div style="background: linear-gradient(135deg, #1E293B, #0F172A); padding: 0.75rem 1rem; border-radius: 12px; margin-bottom: 1rem;">
# #     <span style="color: #94A3B8;">📊 </span>
# #     <span style="color: #F8FAFC; font-weight: 600;">{total_reel:,} commentaires chargés</span>
# #     <span style="color: #475569; margin-left: 0.5rem;">(sur {collection.count_documents({}):,} au total)</span>
# # </div>
# # """, unsafe_allow_html=True)

# # col1, col2, col3, col4 = st.columns(4)

# # positif = len(df[df['label_final'] == 'positif']) if 'label_final' in df.columns else 0
# # negatif = len(df[df['label_final'] == 'negatif']) if 'label_final' in df.columns else 0
# # neutre = len(df[df['label_final'] == 'neutre']) if 'label_final' in df.columns else 0
# # autres = total_reel - (positif + negatif + neutre)
# # taux_positif = (positif / total_reel * 100) if total_reel > 0 else 0

# # with col1:
# #     st.metric("📝 Total", f"{total_reel:,}")
# # with col2:
# #     st.metric("😊 Positifs", f"{positif:,}", delta=f"{taux_positif:.1f}%")
# # with col3:
# #     st.metric("😠 Négatifs", f"{negatif:,}")
# # with col4:
# #     st.metric("😐 Neutres", f"{neutre + autres:,}")

# # st.markdown("---")

# # # GRAPHIQUES
# # if 'label_final' in df.columns:
# #     sentiment_counts = df['label_final'].value_counts().reset_index()
# #     sentiment_counts.columns = ['Sentiment', 'Nombre']
    
# #     # Configuration Plotly thème dark
# #     layout_config = dict(
# #         plot_bgcolor='rgba(0,0,0,0)',
# #         paper_bgcolor='rgba(0,0,0,0)',
# #         font=dict(color='#F8FAFC'),
# #         title_font=dict(color='#3B82F6', size=16),
# #         xaxis=dict(gridcolor='#334155', tickfont=dict(color='#94A3B8')),
# #         yaxis=dict(gridcolor='#334155', tickfont=dict(color='#94A3B8'))
# #     )
    
# #     # Bar chart
# #     fig = px.bar(
# #         sentiment_counts, 
# #         x='Sentiment', 
# #         y='Nombre', 
# #         color='Sentiment',
# #         color_discrete_map={
# #             'positif': '#10B981',
# #             'negatif': '#EF4444', 
# #             'neutre': '#6B7280',
# #             'mixed': '#F59E0B'
# #         },
# #         text='Nombre',
# #         title="Distribution des sentiments"
# #     )
# #     fig.update_layout(**layout_config)
# #     fig.update_traces(textposition='outside', textfont=dict(color='#F8FAFC'))
# #     st.plotly_chart(fig, use_container_width=True)
    
# #     # Deux colonnes
# #     col1, col2 = st.columns(2)
    
# #     with col1:
# #         fig_pie = px.pie(
# #             values=sentiment_counts['Nombre'], 
# #             names=sentiment_counts['Sentiment'],
# #             title="Proportion des sentiments",
# #             color=sentiment_counts['Sentiment'],
# #             color_discrete_map={
# #                 'positif': '#10B981',
# #                 'negatif': '#EF4444',
# #                 'neutre': '#6B7280',
# #                 'mixed': '#F59E0B'
# #             }
# #         )
# #         fig_pie.update_layout(**layout_config)
# #         st.plotly_chart(fig_pie, use_container_width=True)
    
# #     with col2:
# #         if 'confidence' in df.columns:
# #             conf_pos = df[df['label_final'] == 'positif']['confidence'].mean() if positif > 0 else 0
# #             conf_neg = df[df['label_final'] == 'negatif']['confidence'].mean() if negatif > 0 else 0
# #             conf_neutre = df[df['label_final'] == 'neutre']['confidence'].mean() if neutre > 0 else 0
            
# #             conf_df = pd.DataFrame({
# #                 'Sentiment': ['Positif', 'Négatif', 'Neutre'],
# #                 'Confiance moyenne': [conf_pos, conf_neg, conf_neutre]
# #             })
# #             fig_conf = px.bar(
# #                 conf_df, x='Sentiment', y='Confiance moyenne',
# #                 color='Sentiment',
# #                 color_discrete_map={'Positif': '#10B981', 'Négatif': '#EF4444', 'Neutre': '#6B7280'},
# #                 range_y=[0, 1],
# #                 title="Score de confiance par sentiment"
# #             )
# #             fig_conf.update_layout(**layout_config)
# #             st.plotly_chart(fig_conf, use_container_width=True)

# # # STATISTIQUES DÉTAILLÉES
# # with st.expander("📊 Statistiques détaillées"):
# #     col1, col2 = st.columns(2)
    
# #     with col1:
# #         st.markdown(f"""
# #         - **Total commentaires:** {total_reel:,}
# #         - **Positifs:** {positif} ({positif/total_reel*100:.1f}%)
# #         - **Négatifs:** {negatif} ({negatif/total_reel*100:.1f}%)
# #         - **Neutres:** {neutre} ({neutre/total_reel*100:.1f}%)
# #         """)
    
# #     with col2:
# #         if 'sources' in df.columns:
# #             st.write(f"- **Sources uniques:** {df['sources'].nunique()}")
# #         if 'confidence' in df.columns:
# #             st.write(f"- **Confiance moyenne:** {df['confidence'].mean():.2f}")
# #             st.write(f"- **Confiance médiane:** {df['confidence'].median():.2f}")

# # # DERNIERS COMMENTAIRES
# # st.subheader("📝 Derniers commentaires")

# # for idx, row in df.head(20).iterrows():
# #     sentiment = row.get('label_final', 'unknown')
# #     color = get_sentiment_color(sentiment)
# #     emoji = get_sentiment_emoji(sentiment)
    
# #     comment = row.get('Commentaire_Client', '')
# #     if len(str(comment)) > 300:
# #         comment = str(comment)[:300] + "..."
    
# #     date = row.get('dates', 'Date inconnue')
# #     source = row.get('sources', 'Source inconnue')
    
# #     st.markdown(f"""
# #     <div style="
# #         border-left: 4px solid {color};
# #         background: linear-gradient(135deg, #1E293B, #0F172A);
# #         padding: 1rem;
# #         margin: 0.75rem 0;
# #         border-radius: 12px;
# #         transition: transform 0.2s;
# #     ">
# #         <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
# #             <span style="color: {color}; font-weight: 600;">{emoji} {sentiment.upper()}</span>
# #             <span style="color: #64748B; font-size: 0.75rem;">📅 {date}</span>
# #         </div>
# #         <div style="color: #E2E8F0; margin: 0.5rem 0;">
# #             {comment}
# #         </div>
# #         <div style="color: #475569; font-size: 0.75rem;">
# #             📱 {source}
# #         </div>
# #     </div>
# #     """, unsafe_allow_html=True)

# # # EXPORT
# # with st.expander("📥 Exporter les données"):
# #     csv = df.to_csv(index=False).encode('utf-8')
# #     col1, col2, col3 = st.columns(3)
# #     with col1:
# #         st.download_button(
# #             label="📥 Télécharger CSV",
# #             data=csv,
# #             file_name=f"telecom_sentiments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
# #             mime="text/csv"
# #         )

# # pages/1_Overview.py
# import streamlit as st
# from pymongo import MongoClient
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# from datetime import datetime
# import os
# import sys
# sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
# from style import THEME_CSS, PLOTLY_LAYOUT, COLOR_MAP, kpi_card, page_header

# # ── Config ────────────────────────────────────────────────────────────────────
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018/")

# @st.cache_resource
# def init_connection():
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
#         client.admin.command("ping")
#         return client
#     except Exception as e:
#         st.error(f"❌ Erreur MongoDB: {e}")
#         return None

# client = init_connection()
# if client is None:
#     st.stop()

# db         = client["telecom_algerie"]
# collection = db["dataset_unifie"]

# # ── CSS ───────────────────────────────────────────────────────────────────────
# st.markdown(THEME_CSS, unsafe_allow_html=True)

# # ── Sidebar ───────────────────────────────────────────────────────────────────
# with st.sidebar:
#     st.markdown("""
#     <div style="padding:.5rem 0 1.2rem;">
#         <div style="font-size:1.1rem;font-weight:800;color:#f1f5f9;">📡 TélécomDZ</div>
#         <div style="font-size:.7rem;color:#475569;font-weight:500;letter-spacing:.08em;text-transform:uppercase;">Analyse des sentiments</div>
#     </div>
#     """, unsafe_allow_html=True)
#     st.divider()
#     st.markdown("### 🔍 Filtres")

#     options = ["Tous", "positif", "negatif", "neutre", "mixed"]
#     selected = st.multiselect("Sentiment", options, default=["Tous"])

#     if "Tous" in selected or not selected:
#         sentiment_filter = None
#     else:
#         sentiment_filter = selected

#     st.caption(f"📌 Filtre actif : {'Tous' if not sentiment_filter else ', '.join(sentiment_filter)}")

# # ── Données ───────────────────────────────────────────────────────────────────
# @st.cache_data(ttl=3600)
# def load_data(sentiments):
#     try:
#         query = {}
#         if sentiments:
#             query["label_final"] = {"$in": sentiments}
#         return pd.DataFrame(list(collection.find(query)))
#     except Exception as e:
#         st.error(f"Erreur: {e}")
#         return pd.DataFrame()

# with st.spinner("🔄 Chargement…"):
#     df = load_data(tuple(sentiment_filter) if sentiment_filter else None)

# if df.empty:
#     st.warning("⚠️ Aucune donnée disponible")
#     st.stop()

# # ── En-tête ───────────────────────────────────────────────────────────────────
# st.markdown(page_header("📊", "Vue d'ensemble", "Analyse globale des sentiments clients · Télécom Algérie"), unsafe_allow_html=True)

# total   = len(df)
# pos     = len(df[df["label_final"] == "positif"])
# neg     = len(df[df["label_final"] == "negatif"])
# neu     = len(df[df["label_final"] == "neutre"])
# sources = df["sources"].nunique() if "sources" in df.columns else 0
# taux_p  = pos / total * 100 if total else 0
# taux_n  = neg / total * 100 if total else 0

# # ── KPI Row ───────────────────────────────────────────────────────────────────
# k1, k2, k3, k4, k5 = st.columns(5)
# for col, html in zip(
#     [k1, k2, k3, k4, k5],
#     [
#         kpi_card("📝", "Total commentaires", f"{total:,}", color="blue"),
#         kpi_card("😊", "Positifs",  f"{pos:,}",  delta=f"{taux_p:.1f}%", delta_up=True,  color="green"),
#         kpi_card("😠", "Négatifs",  f"{neg:,}",  delta=f"{taux_n:.1f}%", delta_up=False, color="red"),
#         kpi_card("😐", "Neutres",   f"{neu:,}",  color="yellow"),
#         kpi_card("📱", "Sources",   str(sources), color="purple"),
#     ],
# ):
#     with col:
#         st.markdown(html, unsafe_allow_html=True)

# st.markdown("<br>", unsafe_allow_html=True)

# # ── Graphique principal : distribution ────────────────────────────────────────
# if "label_final" in df.columns:
#     counts = df["label_final"].value_counts().reset_index()
#     counts.columns = ["Sentiment", "Nombre"]

#     c1, c2 = st.columns([3, 2])

#     with c1:
#         st.markdown('<div class="card">', unsafe_allow_html=True)
#         st.markdown('<div class="card-title">Distribution des sentiments</div>', unsafe_allow_html=True)
#         fig_bar = px.bar(
#             counts, x="Sentiment", y="Nombre", color="Sentiment",
#             color_discrete_map=COLOR_MAP, text="Nombre",
#         )
#         fig_bar.update_traces(textposition="outside", textfont_color="#f1f5f9", marker_line_width=0)
#         fig_bar.update_layout(**PLOTLY_LAYOUT, height=300, showlegend=False)
#         st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})
#         st.markdown("</div>", unsafe_allow_html=True)

#     with c2:
#         st.markdown('<div class="card">', unsafe_allow_html=True)
#         st.markdown('<div class="card-title">Répartition</div>', unsafe_allow_html=True)
#         fig_pie = px.pie(
#             counts, values="Nombre", names="Sentiment",
#             color="Sentiment", color_discrete_map=COLOR_MAP,
#             hole=.55,
#         )
#         fig_pie.update_traces(textfont_color="#f1f5f9", textinfo="percent")
#         fig_pie.update_layout(**{**PLOTLY_LAYOUT, "margin": dict(l=10, r=10, t=10, b=10)}, height=300)
#         st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False})
#         st.markdown("</div>", unsafe_allow_html=True)

# # ── Score de confiance ────────────────────────────────────────────────────────
# if "confidence" in df.columns:
#     c3, c4 = st.columns(2)

#     with c3:
#         st.markdown('<div class="card">', unsafe_allow_html=True)
#         st.markdown('<div class="card-title">Score de confiance par sentiment</div>', unsafe_allow_html=True)
#         conf_df = (
#             df.groupby("label_final")["confidence"]
#             .mean()
#             .reset_index()
#             .rename(columns={"label_final": "Sentiment", "confidence": "Confiance"})
#         )
#         fig_conf = px.bar(conf_df, x="Sentiment", y="Confiance",
#                           color="Sentiment", color_discrete_map=COLOR_MAP, range_y=[0, 1])
#         fig_conf.update_layout(**PLOTLY_LAYOUT, height=250, showlegend=False)
#         st.plotly_chart(fig_conf, use_container_width=True, config={"displayModeBar": False})
#         st.markdown("</div>", unsafe_allow_html=True)

#     with c4:
#         st.markdown('<div class="card">', unsafe_allow_html=True)
#         st.markdown('<div class="card-title">Distribution des scores de confiance</div>', unsafe_allow_html=True)
#         fig_hist = px.histogram(df, x="confidence", color="label_final",
#                                 color_discrete_map=COLOR_MAP, nbins=30, barmode="overlay")
#         fig_hist.update_layout(**PLOTLY_LAYOUT, height=250)
#         st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": False})
#         st.markdown("</div>", unsafe_allow_html=True)

# # ── Source breakdown ──────────────────────────────────────────────────────────
# if "sources" in df.columns:
#     st.markdown('<div class="card">', unsafe_allow_html=True)
#     st.markdown('<div class="card-title">Sentiments par source</div>', unsafe_allow_html=True)
#     src_df = df.groupby(["sources", "label_final"]).size().reset_index(name="count")
#     fig_src = px.bar(src_df, x="sources", y="count", color="label_final",
#                      color_discrete_map=COLOR_MAP, barmode="group", text="count")
#     fig_src.update_traces(textfont_color="#f1f5f9", textposition="outside")
#     fig_src.update_layout(**PLOTLY_LAYOUT, height=280, showlegend=True)
#     st.plotly_chart(fig_src, use_container_width=True, config={"displayModeBar": False})
#     st.markdown("</div>", unsafe_allow_html=True)

# # ── Derniers commentaires ─────────────────────────────────────────────────────
# st.markdown('<div class="card">', unsafe_allow_html=True)
# st.markdown('<div class="card-title">📝 Derniers commentaires</div>', unsafe_allow_html=True)

# for _, row in df.head(15).iterrows():
#     sent = row.get("label_final", "inconnu")
#     cls  = "pos" if sent == "positif" else "neg"
#     emoji = "😊" if sent == "positif" else "😠" if sent == "negatif" else "😐"
#     color = COLOR_MAP.get(sent, "#6b7280")
#     comment = str(row.get("Commentaire_Client", ""))[:280]
#     if len(str(row.get("Commentaire_Client", ""))) > 280:
#         comment += "…"
#     date   = row.get("dates",   "—")
#     source = row.get("sources", "—")

#     st.markdown(f"""
#     <div class="comment-card {cls}">
#         <div style="display:flex;justify-content:space-between;align-items:center;">
#             <span class="comment-label" style="color:{color};">{emoji} {sent.upper()}</span>
#             <span class="comment-meta">📅 {date} &nbsp;·&nbsp; 📱 {source}</span>
#         </div>
#         <div class="comment-text">{comment}</div>
#     </div>""", unsafe_allow_html=True)

# st.markdown("</div>", unsafe_allow_html=True)

# # ── Statistiques & export ─────────────────────────────────────────────────────
# with st.expander("📊 Statistiques détaillées & Export"):
#     s1, s2 = st.columns(2)
#     with s1:
#         st.markdown(f"""
#         | Indicateur | Valeur |
#         |---|---|
#         | **Total** | {total:,} |
#         | **Positifs** | {pos:,} ({taux_p:.1f}%) |
#         | **Négatifs** | {neg:,} ({taux_n:.1f}%) |
#         | **Neutres** | {neu:,} ({neu/total*100:.1f}%) |
#         """)
#     with s2:
#         if "sources" in df.columns:
#             st.markdown(f"**Sources uniques :** {sources}")
#         if "confidence" in df.columns:
#             st.markdown(f"**Confiance moyenne :** {df['confidence'].mean():.3f}")
#             st.markdown(f"**Confiance médiane :** {df['confidence'].median():.3f}")

#     csv = df.to_csv(index=False).encode("utf-8")
#     st.download_button("📥 Télécharger CSV",  data=csv,
#                        file_name=f"overview_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
#                        mime="text/csv")

# pages/overview.py
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

def layout():
    return html.Div([
        html.H2("Page Overview", style={"color": "white"}),
        html.P("Contenu de la page Overview", style={"color": "#cbd5e1"}),
        dcc.Graph(
            figure={
                "data": [
                    {"x": [1, 2, 3], "y": [4, 1, 2], "type": "bar", "name": "SF"},
                ],
                "layout": {"title": "Exemple de graphique",
                          "paper_bgcolor": "#1e293b",
                          "plot_bgcolor": "#1e293b",
                          "font": {"color": "white"}}
            }
        )
    ])