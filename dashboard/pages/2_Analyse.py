# pages/2_Analyse.py
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27018")
client = MongoClient(MONGO_URI)
db = client["telecom_algerie"]
collection = db["dataset_unifie_sans_doublons"]

st.set_page_config(page_title="Analyse - Télécom Algérie", layout="wide")
st.title("📈 Analyse approfondie - Télécom Algérie")

# ============================================
# FILTRES (comme Prefect)
# ============================================
st.sidebar.header("🔍 Filtres d'analyse")

# Filtre période
period = st.sidebar.selectbox(
    "Période",
    ["7 derniers jours", "30 derniers jours", "3 derniers mois", "Tout"]
)

# Filtre sentiment
sentiment_filter = st.sidebar.multiselect(
    "Sentiment",
    ["positif", "negatif"],
    default=["positif", "negatif"]
)

# Filtre source
sources = st.sidebar.multiselect(
    "Source",
    ["Facebook", "Twitter", "Forum", "Test_manuel"],
    default=["Facebook", "Twitter", "Forum", "Test_manuel"]
)

# ============================================
# RÉCUPÉRATION DES DONNÉES
# ============================================
def load_data():
    query = {}
    if sentiment_filter:
        query["label_final"] = {"$in": sentiment_filter}
    if sources:
        query["sources"] = {"$in": sources}
    
    data = list(collection.find(query).limit(5000))
    df = pd.DataFrame(data) if data else pd.DataFrame()
    
    if not df.empty and 'dates' in df.columns:
        # Conversion des dates
        try:
            df['date_parsed'] = pd.to_datetime(df['dates'], errors='coerce')
        except:
            df['date_parsed'] = datetime.now()
    
    return df

df = load_data()

# ============================================
# KPI PRINCIPAUX (comme Prefect)
# ============================================
st.subheader("📊 Indicateurs clés")

col1, col2, col3, col4 = st.columns(4)

if not df.empty:
    col1.metric("📝 Total commentaires", len(df))
    col2.metric("😊 Taux positif", f"{len(df[df['label_final'] == 'positif']) / len(df) * 100:.1f}%")
    col3.metric("😠 Taux négatif", f"{len(df[df['label_final'] == 'negatif']) / len(df) * 100:.1f}%")
    col4.metric("📱 Sources", df['sources'].nunique() if 'sources' in df else 0)
else:
    st.warning("Aucune donnée disponible")

st.markdown("---")

# ============================================
# GRAPHIQUE 1 : Évolution temporelle
# ============================================
st.subheader("📈 Évolution des sentiments dans le temps")

if not df.empty and 'date_parsed' in df.columns:
    # Agréger par date
    df['date_only'] = df['date_parsed'].dt.date
    daily_counts = df.groupby(['date_only', 'label_final']).size().reset_index(name='count')
    
    fig = px.line(daily_counts, x='date_only', y='count', color='label_final',
                  title="Évolution quotidienne des commentaires",
                  color_discrete_map={'positif': 'green', 'negatif': 'red'})
    st.plotly_chart(fig, use_container_width=True)

# ============================================
# GRAPHIQUE 2 : Distribution des scores
# ============================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🎯 Distribution des sentiments")
    if not df.empty:
        sentiment_counts = df['label_final'].value_counts()
        fig = px.pie(values=sentiment_counts.values, names=sentiment_counts.index,
                     title="Répartition Positif/Négatif",
                     color_discrete_map={'positif': 'green', 'negatif': 'red'})
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("📊 Score de confiance")
    if not df.empty and 'confidence' in df.columns:
        fig = px.histogram(df, x='confidence', nbins=20, color='label_final',
                           title="Distribution des scores de confiance",
                           color_discrete_map={'positif': 'green', 'negatif': 'red'})
        st.plotly_chart(fig, use_container_width=True)

# ============================================
# GRAPHIQUE 3 : Top des raisons (comme Prefect)
# ============================================
st.subheader("🔍 Top des raisons d'insatisfaction/satisfaction")

if not df.empty and 'reason' in df.columns:
    # Raisons négatives
    neg_reasons = df[df['label_final'] == 'negatif']['reason'].value_counts().head(5)
    pos_reasons = df[df['label_final'] == 'positif']['reason'].value_counts().head(5)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("😠 **Principales raisons négatives**")
        for reason, count in neg_reasons.items():
            st.write(f"- {reason}: {count}")
    
    with col2:
        st.write("😊 **Principales raisons positives**")
        for reason, count in pos_reasons.items():
            st.write(f"- {reason}: {count}")

# ============================================
# GRAPHIQUE 4 : Matrice de corrélation
# ============================================
st.subheader("📊 Analyse par source")

if not df.empty and 'sources' in df.columns:
    source_sentiment = pd.crosstab(df['sources'], df['label_final'])
    fig = px.bar(source_sentiment, barmode='group', title="Sentiment par source")
    st.plotly_chart(fig, use_container_width=True)

# ============================================
# TABLEAU DES DONNÉES (comme Prefect logs)
# ============================================
st.subheader("📋 Détail des commentaires")

if not df.empty:
    # Colonnes à afficher
    display_cols = ['Commentaire_Client', 'label_final', 'sources', 'dates', 'confidence']
    existing_cols = [c for c in display_cols if c in df.columns]
    
    st.dataframe(
        df[existing_cols].head(100),
        use_container_width=True,
        hide_index=True
    )
    
    # Export CSV (comme Prefect export)
    csv = df[existing_cols].to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Exporter en CSV",
        data=csv,
        file_name=f"analyse_telecom_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

# ============================================
# STATISTIQUES AVANCÉES
# ============================================
with st.expander("📊 Statistiques avancées"):
    if not df.empty:
        st.write("**Résumé statistique**")
        st.write(f"- Période couverte: du {df['date_only'].min()} au {df['date_only'].max()}")
        st.write(f"- Nombre total de sources: {df['sources'].nunique() if 'sources' in df else 1}")
        st.write(f"- Confiance moyenne: {df['confidence'].mean():.2f}")
        
        # Test du khi-deux (relation source/sentiment)
        from scipy import stats
        if 'sources' in df.columns:
            contingency = pd.crosstab(df['sources'], df['label_final'])
            chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
            st.write(f"- Test khi-deux (source ↔ sentiment): p-value = {p_value:.4f}")
            if p_value < 0.05:
                st.success("✅ Relation significative entre la source et le sentiment")
            else:
                st.info("❌ Pas de relation significative")