#!/usr/bin/env python3
# dashboard.py - Affichage des résultats en temps réel

import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION_NAME = "commentaires_predictions"

# ============================================================
# CONNEXION MONGODB
# ============================================================
@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI)

client = get_mongo_client()
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ============================================================
# TITRE
# ============================================================
st.set_page_config(page_title="Dashboard Sentiment", layout="wide")
st.title("📊 Dashboard Analyse des Commentaires")
st.markdown("---")

# ============================================================
# MÉTRIQUES
# ============================================================
col1, col2, col3, col4 = st.columns(4)

total = collection.count_documents({})
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
today_count = collection.count_documents({"date_traitement": {"$gte": today}})

with col1:
    st.metric("📝 Total commentaires", total)
with col2:
    st.metric("📅 Aujourd'hui", today_count)
with col3:
    last = collection.find_one(sort=[("date_traitement", -1)])
    last_time = last["date_traitement"].strftime("%H:%M:%S") if last else "N/A"
    st.metric("🕐 Dernier", last_time)
with col4:
    st.metric("⚡ Statut", "Temps réel")

st.markdown("---")

# ============================================================
# RÉPARTITION DES SENTIMENTS
# ============================================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🎭 Sentiments")
    pipeline = [{"$group": {"_id": "$prediction", "count": {"$sum": 1}}}]
    data = list(collection.aggregate(pipeline))
    
    if data:
        df = pd.DataFrame(data)
        colors = {"POSITIF": "#4CAF50", "NEUTRE": "#FFA500", "NEGATIF": "#FF4B4B"}
        fig = px.pie(df, values="count", names="_id", color="_id", 
                     color_discrete_map=colors, title="Répartition")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("📈 Évolution")
    last_7_days = datetime.now() - timedelta(days=7)
    pipeline_time = [
        {"$match": {"date_traitement": {"$gte": last_7_days}}},
        {"$group": {
            "_id": {"date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_traitement"},
                     "sentiment": "$prediction"},
            "count": {"$sum": 1}
        }}  
        }]
    time_data = list(collection.aggregate(pipeline_time))
    
    if time_data:
        df_time = pd.DataFrame(time_data)
        df_time["date"] = pd.to_datetime([d["_id"]["date"] for d in time_data])
        df_time["sentiment"] = [d["_id"]["sentiment"] for d in time_data]
        fig = px.line(df_time, x="date", y="count", color="sentiment",
                      color_discrete_map=colors, title="Évolution")
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ============================================================
# DERNIERS COMMENTAIRES
# ============================================================
st.subheader("🔝 Derniers commentaires analysés")

tab1, tab2, tab3 = st.tabs(["🟢 Positifs", "🟡 Neutres", "🔴 Négatifs"])

with tab1:
    positifs = list(collection.find({"prediction": "POSITIF"}).sort("date_traitement", -1).limit(10))
    for c in positifs:
        st.markdown(f"**{c['commentaire_original'][:100]}...**")
        st.caption(f"Confiance: {c['confidence']:.2%} | {c['date_traitement'].strftime('%H:%M:%S')}")
        st.divider()

with tab2:
    neutres = list(collection.find({"prediction": "NEUTRE"}).sort("date_traitement", -1).limit(10))
    for c in neutres:
        st.markdown(f"**{c['commentaire_original'][:100]}...**")
        st.caption(f"Confiance: {c['confidence']:.2%} | {c['date_traitement'].strftime('%H:%M:%S')}")
        st.divider()

with tab3:
    negatifs = list(collection.find({"prediction": "NEGATIF"}).sort("date_traitement", -1).limit(10))
    for c in negatifs:
        st.markdown(f"**{c['commentaire_original'][:100]}...**")
        st.caption(f"Confiance: {c['confidence']:.2%} | {c['date_traitement'].strftime('%H:%M:%S')}")
        st.divider()

st.markdown("---")
st.caption(f"🔄 Dernière mise à jour: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")