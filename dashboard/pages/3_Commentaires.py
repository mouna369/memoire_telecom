# pages/3_Commentaires.py
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
client = MongoClient(MONGO_URI)
db = client["telecom_algerie"]
collection = db["dataset_unifie_sans_doublons"]

st.set_page_config(page_title="Commentaires", layout="wide")
st.title("💬 Exploration des commentaires")

# Filtres
st.sidebar.header("🔍 Filtres")
sentiment_filter = st.sidebar.multiselect(
    "Sentiment", ["positif", "negatif"], default=["positif", "negatif"]
)
search_text = st.sidebar.text_input("🔎 Rechercher dans les commentaires", "")

# Chargement
query = {}
if sentiment_filter:
    query["label_final"] = {"$in": sentiment_filter}

data = list(collection.find(query).limit(5000))
df = pd.DataFrame(data) if data else pd.DataFrame()

if df.empty:
    st.warning("Aucune donnée trouvée")
    st.stop()

# Recherche textuelle
if search_text:
    mask = df['Commentaire_Client'].str.contains(search_text, case=False, na=False)
    df = df[mask]
    st.info(f"🔍 {len(df)} résultat(s) pour '{search_text}'")

# Affichage
st.subheader(f"📝 {len(df)} commentaires trouvés")

for idx, row in df.iterrows():
    sentiment = row['label_final']
    emoji = "😊" if sentiment == 'positif' else "😠"
    color = "#2ecc71" if sentiment == 'positif' else "#e74c3c"
    
    st.markdown(
        f"""
        <div style='border-left: 4px solid {color}; padding: 10px; margin: 10px 0; background-color: #f8f9fa;'>
            <b>{emoji} {sentiment.upper()}</b><br>
            {row['Commentaire_Client']}<br>
            <small>📅 {row.get('dates', 'N/A')} | 📱 {row.get('sources', 'N/A')}</small>
        </div>
        """,
        unsafe_allow_html=True
    )

# Export
if st.button("📥 Exporter en CSV"):
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("Télécharger", csv, "commentaires.csv", "text/csv")