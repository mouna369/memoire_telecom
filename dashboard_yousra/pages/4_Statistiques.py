# pages/4_Statistiques.py
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
import sys
from scipy import stats

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from sidebar import render_sidebar
from style import MAIN_CSS

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
.section-title-custom {
    font-size: 1.3rem; font-weight: 600; color: #0f3b5c;
    margin: 1.5rem 0 1rem 0; border-left: 4px solid #2dd4bf; padding-left: 12px;
}
.stat-card {
    background: white; border-radius: 20px; padding: 1.2rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04); border: 1px solid #e2e8f0;
    margin-bottom: 1rem;
}
.stat-value {
    font-size: 2rem; font-weight: 700; color: #0f3b5c; margin: 0.5rem 0;
}
.stat-label {
    font-size: 0.8rem; color: #64748b; letter-spacing: 0.5px;
}
.custom-info {
    background-color: #eef2ff; border-left: 4px solid #3b82f6;
    padding: 0.75rem 1rem; border-radius: 8px; margin-bottom: 1rem;
    font-size: 0.9rem; color: #1e3a8a;
}
.custom-success {
    background-color: #ecfdf5; border-left: 4px solid #10b981;
    padding: 0.75rem 1rem; border-radius: 8px; margin-bottom: 1rem;
    font-size: 0.9rem; color: #065f46;
}
.custom-warning {
    background-color: #fffbeb; border-left: 4px solid #f59e0b;
    padding: 0.75rem 1rem; border-radius: 8px; margin-bottom: 1rem;
    font-size: 0.9rem; color: #92400e;
}
.metric-box {
    background: #f8fafc; border-radius: 16px; padding: 1rem;
    text-align: center; border: 1px solid #e2e8f0;
}
.metric-label {
    font-size: 0.8rem; color: #475569; font-weight: 500;
}
.metric-number {
    font-size: 2rem; font-weight: 700; color: #0f3b5c; margin-top: 0.3rem;
}
</style>
""", unsafe_allow_html=True)

# --- Sidebar (identique au dashboard) ---
render_sidebar()

# --- Contenu principal ---
st.markdown('<div class="section-title-custom"><i class="fas fa-chart-line"></i> Statistiques avancées</div>', unsafe_allow_html=True)

# Chargement des données
with st.spinner('<i class="fas fa-spinner fa-pulse"></i> Chargement des données...'):
    data = list(collection.find().limit(23000))
    df = pd.DataFrame(data) if data else pd.DataFrame()

if df.empty:
    st.markdown("""
    <div class="custom-warning">
        <i class="fas fa-exclamation-triangle"></i> Aucune donnée trouvée dans la base.
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# --- Test du khi-deux ---
if 'sources' in df.columns and 'label_final' in df.columns:
    st.markdown('<div class="section-title-custom"><i class="fas fa-flask"></i> Test du khi-deux (χ²)</div>', unsafe_allow_html=True)
    st.markdown('<p><i class="fas fa-link"></i> Relation entre la source et le sentiment</p>', unsafe_allow_html=True)
    
    contingency = pd.crosstab(df['sources'], df['label_final'])
    st.dataframe(contingency, use_container_width=True)
    
    chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label"><i class="fas fa-calculator"></i> Khi-deux (χ²)</div>
            <div class="metric-number">{chi2:.2f}</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label"><i class="fas fa-chart-simple"></i> Degrés de liberté</div>
            <div class="metric-number">{dof}</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label"><i class="fas fa-percent"></i> p-value</div>
            <div class="metric-number">{p_value:.4f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    if p_value < 0.05:
        st.markdown("""
        <div class="custom-success">
            <i class="fas fa-check-circle"></i> Résultat SIGNIFICATIF : Il existe une relation entre la source et le sentiment.
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="custom-info">
            <i class="fas fa-info-circle"></i> Résultat NON SIGNIFICATIF : Aucune relation détectée entre la source et le sentiment.
        </div>
        """, unsafe_allow_html=True)

# --- Statistiques descriptives par sentiment ---
st.markdown('<div class="section-title-custom"><i class="fas fa-chart-simple"></i> Statistiques descriptives</div>', unsafe_allow_html=True)

col_left, col_right = st.columns(2)

with col_left:
    st.markdown('<div class="stat-card"><i class="far fa-smile-wink"></i> <strong>Sentiment positif</strong></div>', unsafe_allow_html=True)
    pos_df = df[df['label_final'] == 'positif']
    st.markdown(f'<p><i class="fas fa-hashtag"></i> Nombre : <strong>{len(pos_df)}</strong></p>', unsafe_allow_html=True)
    if 'confidence' in pos_df.columns:
        st.markdown(f'<p><i class="fas fa-chart-line"></i> Confiance moyenne : <strong>{pos_df["confidence"].mean():.2f}</strong></p>', unsafe_allow_html=True)
        st.markdown(f'<p><i class="fas fa-chart-gantt"></i> Confiance médiane : <strong>{pos_df["confidence"].median():.2f}</strong></p>', unsafe_allow_html=True)

with col_right:
    st.markdown('<div class="stat-card"><i class="far fa-frown"></i> <strong>Sentiment négatif</strong></div>', unsafe_allow_html=True)
    neg_df = df[df['label_final'] == 'negatif']
    st.markdown(f'<p><i class="fas fa-hashtag"></i> Nombre : <strong>{len(neg_df)}</strong></p>', unsafe_allow_html=True)
    if 'confidence' in neg_df.columns:
        st.markdown(f'<p><i class="fas fa-chart-line"></i> Confiance moyenne : <strong>{neg_df["confidence"].mean():.2f}</strong></p>', unsafe_allow_html=True)
        st.markdown(f'<p><i class="fas fa-chart-gantt"></i> Confiance médiane : <strong>{neg_df["confidence"].median():.2f}</strong></p>', unsafe_allow_html=True)

# --- Test de Student ---
if 'confidence' in df.columns:
    st.markdown('<div class="section-title-custom"><i class="fas fa-chart-line"></i> Test de Student (t-test)</div>', unsafe_allow_html=True)
    st.markdown('<p><i class="fas fa-arrows-left-right"></i> Comparaison des scores de confiance entre positif et négatif</p>', unsafe_allow_html=True)
    
    pos_conf = df[df['label_final'] == 'positif']['confidence'].dropna()
    neg_conf = df[df['label_final'] == 'negatif']['confidence'].dropna()
    
    if len(pos_conf) > 0 and len(neg_conf) > 0:
        t_stat, p_value = stats.ttest_ind(pos_conf, neg_conf)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-label"><i class="fas fa-chart-line"></i> t-statistic</div>
                <div class="metric-number">{t_stat:.2f}</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-label"><i class="fas fa-percent"></i> p-value</div>
                <div class="metric-number">{p_value:.4f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        if p_value < 0.05:
            st.markdown("""
            <div class="custom-success">
                <i class="fas fa-check-circle"></i> Différence significative entre les scores de confiance.
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="custom-info">
                <i class="fas fa-info-circle"></i> Pas de différence significative entre les scores de confiance.
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="custom-warning">
            <i class="fas fa-exclamation-triangle"></i> Données insuffisantes pour réaliser le test t.
        </div>
        """, unsafe_allow_html=True)

# Pied de page
st.caption(f'<i class="fas fa-database"></i> Base analysée : {len(df)} commentaires | <i class="far fa-clock"></i> {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")}', unsafe_allow_html=True)