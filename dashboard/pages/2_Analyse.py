# 📁 pages/2_Analyse.py - Version optimisée
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from style import THEME_CSS, PLOTLY_LAYOUT, COLOR_MAP, kpi_card, page_header

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
client = MongoClient(MONGO_URI)
db = client["telecom_algerie"]
collection = db["dataset_unifie_sans_doublons"]

st.markdown(THEME_CSS, unsafe_allow_html=True)

# ── Sidebar Filtres ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="filter-label">🔍 FILTRES D\'ANALYSE</div>', unsafe_allow_html=True)
    
    # Période
    st.markdown('<span class="filter-label">Période</span>', unsafe_allow_html=True)
    period = st.selectbox(
        "Sélectionner",
        ["7 derniers jours", "30 derniers jours", "3 derniers mois", "Tout"],
        label_visibility="collapsed", key="period_filter"
    )
    
    # Sentiment
    st.markdown('<span class="filter-label" style="margin-top:0.5rem;">Sentiment</span>', unsafe_allow_html=True)
    sentiment_filter = st.multiselect(
        "Choisir", ["positif", "negatif", "neutre"],
        default=["positif", "negatif"], label_visibility="collapsed", key="sentiment_analysis"
    )
    
    # Source
    st.markdown('<span class="filter-label" style="margin-top:0.5rem;">Source</span>', unsafe_allow_html=True)
    sources_filter = st.multiselect(
        "Choisir", ["Facebook", "Twitter", "Forum", "Test_manuel"],
        default=["Facebook", "Twitter", "Forum", "Test_manuel"],
        label_visibility="collapsed", key="source_filter"
    )
    
    # Appliquer
    if st.button("🔄 Appliquer les filtres", use_container_width=True, type="primary"):
        st.rerun()

# ── Chargement Données ────────────────────────────────────────────────────────
def load_data():
    query = {}
    if sentiment_filter:
        query["label_final"] = {"$in": sentiment_filter}
    if sources_filter:
        query["sources"] = {"$in": sources_filter}
    
    # Filtre période
    if period != "Tout":
        days_map = {"7 derniers jours": 7, "30 derniers jours": 30, "3 derniers mois": 90}
        date_limit = datetime.now() - timedelta(days=days_map.get(period, 30))
        # Note: adapter selon format de vos dates MongoDB
    
    data = list(collection.find(query).limit(5000))
    df = pd.DataFrame(data) if data else pd.DataFrame()
    
    if not df.empty and 'dates' in df.columns:
        df['date_parsed'] = pd.to_datetime(df['dates'], errors='coerce')
    
    return df

df = load_data()

if df.empty:
    st.warning("⚠️ Aucune donnée disponible")
    st.stop()

# ── En-tête ───────────────────────────────────────────────────────────────────
st.markdown(page_header("📈", "Analyse approfondie", 
                       "Évolutions, distributions et corrélations"), 
           unsafe_allow_html=True)

# ── KPI Row ───────────────────────────────────────────────────────────────────
total = len(df)
pos = len(df[df["label_final"] == "positif"]) if "label_final" in df.columns else 0
neg = len(df[df["label_final"] == "negatif"]) if "label_final" in df.columns else 0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(kpi_card("📝", "Total", f"{total:,}", color="blue"), unsafe_allow_html=True)
with col2:
    taux_p = pos/total*100 if total else 0
    st.markdown(kpi_card("😊", "Taux positif", f"{taux_p:.1f}%", 
                        delta=f"{pos:,}", delta_up=True, color="green"), 
               unsafe_allow_html=True)
with col3:
    taux_n = neg/total*100 if total else 0
    st.markdown(kpi_card("😠", "Taux négatif", f"{taux_n:.1f}%", 
                        delta=f"{neg:,}", delta_up=False, color="red"), 
               unsafe_allow_html=True)
with col4:
    sources_count = df['sources'].nunique() if 'sources' in df.columns else 0
    st.markdown(kpi_card("📱", "Sources", str(sources_count), color="purple"), 
               unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Graphique Évolution Temporelle ────────────────────────────────────────────
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown('<div class="card-title">📈 Évolution dans le temps</div>', unsafe_allow_html=True)

if not df.empty and 'date_parsed' in df.columns:
    df_clean = df.dropna(subset=['date_parsed'])
    if not df_clean.empty:
        df_clean['date_only'] = df_clean['date_parsed'].dt.date
        daily = df_clean.groupby(['date_only', 'label_final']).size().reset_index(name='count')
        
        fig = px.line(
            daily, x='date_only', y='count', color='label_final',
            color_discrete_map=COLOR_MAP, markers=True
        )
        fig.update_traces(line=dict(width=2.5), marker=dict(size=5))
        fig.update_layout(
            **PLOTLY_LAYOUT, height=320,
            xaxis_title="Date", yaxis_title="Nombre de commentaires",
            legend_title="Sentiment"
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})

st.markdown("</div>", unsafe_allow_html=True)

# ── Distribution & Confiance ──────────────────────────────────────────────────
c1, c2 = st.columns(2)

with c1:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">🎯 Distribution</div>', unsafe_allow_html=True)
    
    if 'label_final' in df.columns:
        counts = df['label_final'].value_counts()
        fig_pie = px.pie(
            values=counts.values, names=counts.index,
            color=counts.index, color_discrete_map=COLOR_MAP,
            hole=0.5
        )
        fig_pie.update_traces(textinfo="percent+label", textfont_color="white")
        fig_pie.update_layout(**PLOTLY_LAYOUT, height=280, showlegend=False)
        st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False})
    
    st.markdown("</div>", unsafe_allow_html=True)

with c2:
    if 'confidence' in df.columns and not df['confidence'].isna().all():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">📊 Scores de confiance</div>', unsafe_allow_html=True)
        
        fig_hist = px.histogram(
            df, x='confidence', color='label_final',
            color_discrete_map=COLOR_MAP, nbins=30, barmode='overlay', opacity=0.7
        )
        fig_hist.update_layout(
            **PLOTLY_LAYOUT, height=280,
            xaxis_title="Score de confiance", yaxis_title="Fréquence"
        )
        st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)

# ── Top Raisons ───────────────────────────────────────────────────────────────
if 'reason' in df.columns and not df['reason'].isna().all():
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="card-title">🔍 Top des raisons</div>', unsafe_allow_html=True)
    
    r1, r2 = st.columns(2)
    
    with r1:
        st.markdown('<div style="font-weight:600; color:#ef4444; margin-bottom:0.5rem;">😠 Négatives</div>', unsafe_allow_html=True)
        neg_reasons = df[df['label_final'] == 'negatif']['reason'].value_counts().head(5)
        for reason, count in neg_reasons.items():
            st.markdown(f"""
            <div style="display:flex; justify-content:space-between; padding:0.3rem 0; border-bottom:1px solid #f1f5f9;">
                <span style="color:#64748b; font-size:0.85rem;">{reason[:40]}{'...' if len(reason)>40 else ''}</span>
                <span style="background:#fee2e2; color:#ef4444; padding:0.1rem 0.5rem; border-radius:12px; font-size:0.75rem; font-weight:600;">{count}</span>
            </div>
            """, unsafe_allow_html=True)
    
    with r2:
        st.markdown('<div style="font-weight:600; color:#22c55e; margin-bottom:0.5rem;">😊 Positives</div>', unsafe_allow_html=True)
        pos_reasons = df[df['label_final'] == 'positif']['reason'].value_counts().head(5)
        for reason, count in pos_reasons.items():
            st.markdown(f"""
            <div style="display:flex; justify-content:space-between; padding:0.3rem 0; border-bottom:1px solid #f1f5f9;">
                <span style="color:#64748b; font-size:0.85rem;">{reason[:40]}{'...' if len(reason)>40 else ''}</span>
                <span style="background:#dcfce7; color:#22c55e; padding:0.1rem 0.5rem; border-radius:12px; font-size:0.75rem; font-weight:600;">{count}</span>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("</div>", unsafe_allow_html=True)

# ── Tableau & Export ──────────────────────────────────────────────────────────
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown('<div class="card-title">📋 Données détaillées</div>', unsafe_allow_html=True)

display_cols = ['Commentaire_Client', 'label_final', 'sources', 'dates', 'confidence']
existing_cols = [c for c in display_cols if c in df.columns]

st.dataframe(
    df[existing_cols].head(50),
    use_container_width=True,
    hide_index=True,
    column_config={
        "label_final": st.column_config.SelectboxColumn(
            "Sentiment", options=["positif", "negatif", "neutre"]
        )
    }
)

# Export
csv = df[existing_cols].to_csv(index=False).encode('utf-8')
st.download_button(
    label="📥 Exporter en CSV",
    data=csv,
    file_name=f"analyse_telecom_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv",
    use_container_width=True
)
st.markdown("</div>", unsafe_allow_html=True)

# ── Stats Avancées (expander) ─────────────────────────────────────────────────
with st.expander("🧮 Tests statistiques avancés"):
    from scipy import stats
    
    if 'sources' in df.columns and 'label_final' in df.columns:
        st.markdown("**Test du χ² : Source ↔ Sentiment**")
        contingency = pd.crosstab(df['sources'], df['label_final'])
        
        col_stat1, col_stat2 = st.columns(2)
        chi2, p_value, dof, _ = stats.chi2_contingency(contingency)
        
        with col_stat1:
            st.metric("χ²", f"{chi2:.2f}")
        with col_stat2:
            st.metric("p-value", f"{p_value:.4f}")
        
        if p_value < 0.05:
            st.success("✅ Relation significative détectée (p < 0.05)")
        else:
            st.info("ℹ️ Pas de relation significative (p ≥ 0.05)")