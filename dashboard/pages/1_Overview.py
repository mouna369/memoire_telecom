# pages/1_Overview.py
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
from datetime import datetime
import os

# PAS de st.set_page_config() ici

# ============================================
# CONNEXION MONGODB
# ============================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")

@st.cache_resource
def init_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception as e:
        st.error(f"❌ Erreur de connexion MongoDB: {e}")
        return None

client = init_connection()

if client is None:
    st.stop()

db = client["telecom_algerie"]
collection = db["dataset_unifie"]

st.title("📊 Vue d'ensemble - Analyse des sentiments")

# ============================================
# SIDEBAR - FILTRES (CORRIGÉ)
# ============================================
st.sidebar.header("🔍 Filtres")

# Option "Tous" gérée correctement
sentiment_options = ["Tous", "positif", "negatif", "neutre", "mixed"]
selected_sentiments = st.sidebar.multiselect(
    "Sentiment",
    sentiment_options,
    default=["Tous"]  # ← Par défaut, "Tous" est sélectionné
)

# CORRECTION ICI : gérer "Tous" correctement
if "Tous" in selected_sentiments:
    # Si "Tous" est sélectionné, ne filtrer sur aucun sentiment
    sentiment_filter = None
    filter_display = "Tous les sentiments"
else:
    # Sinon, filtrer sur les sentiments sélectionnés
    sentiment_filter = selected_sentiments
    filter_display = ", ".join(selected_sentiments)

st.sidebar.caption(f"Filtre actif : {filter_display}")

# ============================================
# CHARGEMENT DES DONNÉES (SANS LIMITE)
# ============================================
@st.cache_data(ttl=3600)
def load_data(sentiments):
    """Charge TOUTES les données depuis MongoDB"""
    try:
        query = {}
        # Appliquer le filtre seulement si des sentiments spécifiques sont sélectionnés
        if sentiments and "Tous" not in sentiments:
            query["label_final"] = {"$in": sentiments}
        
        # Charger TOUTES les données (pas de limite)
        cursor = collection.find(query)
        data = list(cursor)
        
        if data:
            df = pd.DataFrame(data)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur de chargement: {e}")
        return pd.DataFrame()

with st.spinner("Chargement des données..."):
    df = load_data(sentiment_filter)

if df.empty:
    st.warning("⚠️ Aucune donnée trouvée.")
    st.stop()

# ============================================
# AFFICHER LE NOMBRE TOTAL RÉEL
# ============================================
total_reel = len(df)
st.info(f"📊 **{total_reel:,} commentaires chargés** (sur {collection.count_documents({}):,} au total)")

# ============================================
# KPI (Indicateurs clés)
# ============================================
st.subheader("📊 Indicateurs clés")

col1, col2, col3, col4 = st.columns(4)

# Compter par sentiment (en gérant les valeurs manquantes)
positif = len(df[df['label_final'] == 'positif']) if 'label_final' in df.columns else 0
negatif = len(df[df['label_final'] == 'negatif']) if 'label_final' in df.columns else 0
neutre = len(df[df['label_final'] == 'neutre']) if 'label_final' in df.columns else 0
autres = total_reel - (positif + negatif + neutre)

taux_positif = (positif / total_reel * 100) if total_reel > 0 else 0

col1.metric("📝 Total commentaires", f"{total_reel:,}")
col2.metric("😊 Positifs", f"{positif:,}", delta=f"{taux_positif:.1f}%")
col3.metric("😠 Négatifs", f"{negatif:,}")
col4.metric("😐 Neutres/Autres", f"{neutre + autres:,}")

st.markdown("---")

# ============================================
# GRAPHIQUE DES SENTIMENTS
# ============================================
st.subheader("📊 Distribution des sentiments")

if 'label_final' in df.columns:
    # Compter tous les sentiments
    sentiment_counts = df['label_final'].value_counts().reset_index()
    sentiment_counts.columns = ['Sentiment', 'Nombre']
    
    # Définir les couleurs
    color_map = {
        'positif': '#2ecc71', 
        'negatif': '#e74c3c',
        'neutre': '#f39c12'
    }
    
    fig = px.bar(
        sentiment_counts, 
        x='Sentiment', 
        y='Nombre', 
        color='Sentiment',
        color_discrete_map=color_map,
        text='Nombre',
        title="Répartition des sentiments"
    )
    fig.update_traces(textposition='outside')
    st.plotly_chart(fig, use_container_width=True)

# ============================================
# DEUX GRAPHIQUES CÔTE À CÔTE
# ============================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🥧 Répartition")
    fig_pie = px.pie(
        values=sentiment_counts['Nombre'], 
        names=sentiment_counts['Sentiment'],
        title="Proportion des sentiments",
        color=sentiment_counts['Sentiment'],
        color_discrete_map=color_map
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.subheader("📊 Score de confiance")
    if 'confidence' in df.columns:
        conf_pos = df[df['label_final'] == 'positif']['confidence'].mean() if positif > 0 else 0
        conf_neg = df[df['label_final'] == 'negatif']['confidence'].mean() if negatif > 0 else 0
        conf_neutre = df[df['label_final'] == 'neutre']['confidence'].mean() if neutre > 0 else 0
        
        conf_df = pd.DataFrame({
            'Sentiment': ['Positif', 'Négatif', 'Neutre'],
            'Confiance moyenne': [conf_pos, conf_neg, conf_neutre]
        })
        fig_conf = px.bar(conf_df, x='Sentiment', y='Confiance moyenne',
                          color='Sentiment',
                          color_discrete_map={'Positif': '#2ecc71', 'Négatif': '#e74c3c', 'Neutre': '#f39c12'},
                          range_y=[0, 1])
        st.plotly_chart(fig_conf, use_container_width=True)

# ============================================
# STATISTIQUES DÉTAILLÉES
# ============================================
with st.expander("📊 Statistiques détaillées"):
    st.write("**Résumé des données**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"- Total commentaires: {total_reel:,}")
        st.write(f"- Positifs: {positif} ({positif/total_reel*100:.1f}%)")
        st.write(f"- Négatifs: {negatif} ({negatif/total_reel*100:.1f}%)")
    
    with col2:
        if 'sources' in df.columns:
            st.write(f"- Sources: {df['sources'].nunique()}")
        if 'confidence' in df.columns:
            st.write(f"- Confiance moyenne: {df['confidence'].mean():.2f}")

# ============================================
# DERNIERS COMMENTAIRES
# ============================================
st.subheader(f"📝 Derniers commentaires")

for idx, row in df.head(20).iterrows():
    sentiment = row.get('label_final', 'unknown')
    if sentiment == 'positif':
        emoji = "😊"
        color = "#2ecc71"
    elif sentiment == 'negatif':
        emoji = "😠"
        color = "#e74c3c"
    else:
        emoji = "😐"
        color = "#f39c12"
    
    comment = row.get('Commentaire_Client', '')
    if len(str(comment)) > 300:
        comment = str(comment)[:300] + "..."
    
    date = row.get('dates', 'Date inconnue')
    source = row.get('sources', 'Source inconnue')
    
    st.markdown(
        f"""
        <div style='border-left: 4px solid {color}; padding: 10px; margin: 10px 0; background-color: #f8f9fa; border-radius: 5px;'>
            <b>{emoji} {sentiment.upper()}</b><br>
            {comment}<br>
            <small>📅 {date} | 📱 {source}</small>
        </div>
        """,
        unsafe_allow_html=True
    )

# ============================================
# EXPORT
# ============================================
with st.expander("📥 Exporter les données"):
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Télécharger en CSV",
        data=csv,
        file_name=f"telecom_sentiments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )