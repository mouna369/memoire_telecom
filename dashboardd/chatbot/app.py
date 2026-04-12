# app.py
import streamlit as st

st.set_page_config(
    page_title="Dashboard Télécom Algérie",
    page_icon="📊",
    layout="wide"
)

st.title("🏠 Tableau de bord - Télécom Algérie")
st.markdown("---")

st.markdown("""
## 📊 Bienvenue sur votre dashboard d'analyse des sentiments

Utilisez le menu à gauche pour naviguer entre les pages.

### 📈 Pages disponibles

| Page | Description |
|------|-------------|
| **Overview** | Vue d'ensemble avec KPIs et graphiques principaux |
| **Analyse** | Analyse approfondie des données (évolutions, distributions) |
| **Commentaires** | Exploration détaillée des commentaires clients |
| **Statistiques** | Analyses statistiques avancées |
| **Chatbot** | Assistant conversationnel (questions/réponses) |
""")
