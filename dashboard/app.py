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

Ce dashboard vous permet d'analyser les commentaires clients des opérateurs télécoms algériens.

### 📈 Pages disponibles

| Page | Description |
|------|-------------|
| **Overview** | Vue d'ensemble avec KPIs et graphiques principaux |
| **Analyse** | Analyse approfondie des données (évolutions, distributions) |
| **Commentaires** | Exploration détaillée des commentaires clients |
| **Statistiques** | Analyses statistiques avancées (tests, corrélations) |
| **🤖 ChatBot IA** | Conversation intelligente + analyse de vos données |

### 🚀 Fonctionnalités

- ✅ Analyse des sentiments (positif/négatif)
- ✅ Visualisations interactives
- ✅ Filtres dynamiques
- ✅ Export des données (CSV)
- ✅ Mise à jour en temps réel
- ✅ ChatBot IA (Ollama · Mistral 7B)

### 📊 Sources de données

- Commentaires Facebook
- Données annotées manuellement
- Base MongoDB : `telecom_algerie`

---
*Dernière mise à jour : Avril 2026*
""")

st.sidebar.success("✅ Dashboard prêt !")
st.sidebar.info("Utilisez le menu à gauche pour naviguer entre les pages")