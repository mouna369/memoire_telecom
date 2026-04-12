#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dashboard.py — Tableau de bord d'analyse du ChatBot Algérien
Lancer : streamlit run analyse/dashboard.py --server.port 8502
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime, timedelta
import numpy as np

st.set_page_config(
    page_title="Analyse — ChatBot DziriBERT",
    page_icon="📊",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────
# CONNEXION MONGODB
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    return client["chatbot_algerie"]

db = get_db()

# ─────────────────────────────────────────────────────────────
# CHARGEMENT DES DONNÉES
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def charger_donnees():
    docs = list(db.conversations.find({}, {"_id": 0}))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["heure"] = df["timestamp"].dt.hour
    df["date"]  = df["timestamp"].dt.date
    return df

# ─────────────────────────────────────────────────────────────
# EN-TÊTE
# ─────────────────────────────────────────────────────────────
st.title("📊 Tableau de bord — ChatBot DziriBERT")
st.caption("Analyse des conversations en temps réel")

col_refresh, col_periode = st.columns([1, 3])
with col_refresh:
    if st.button("🔄 Actualiser"):
        st.cache_data.clear()
        st.rerun()
with col_periode:
    periode = st.selectbox("Période", ["7 derniers jours", "30 derniers jours", "Tout"], index=0)

df = charger_donnees()

# Filtrage par période
if not df.empty:
    if periode == "7 derniers jours":
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
        df = df[df["timestamp"] >= cutoff]
    elif periode == "30 derniers jours":
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
        df = df[df["timestamp"] >= cutoff]

st.divider()

# ─────────────────────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

if df.empty:
    k1.metric("Messages", 0)
    k2.metric("Sessions", 0)
    k3.metric("Confiance moy.", "—")
    k4.metric("Taux incompris", "—")
    k5.metric("Langues détectées", 0)
    st.info("Aucune donnée disponible pour cette période. Démarrez une conversation !")
    st.stop()

total       = len(df)
sessions    = df["session_id"].nunique()
conf_moy    = df["confiance"].mean()
incompris   = (df["intention"] == "incompris").sum()
taux_inc    = incompris / total
nb_langues  = df["langue_detectee"].nunique() if "langue_detectee" in df.columns else 1

k1.metric("Messages",        total)
k2.metric("Sessions",        sessions)
k3.metric("Confiance moy.",  f"{conf_moy:.1%}")
k4.metric("Taux incompris",  f"{taux_inc:.1%}", delta=f"-{taux_inc:.1%}" if taux_inc < 0.2 else f"+{taux_inc:.1%}", delta_color="inverse")
k5.metric("Langues",         nb_langues)

st.divider()

# ─────────────────────────────────────────────────────────────
# GRAPHIQUES LIGNE 1
# ─────────────────────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Distribution des intentions")
    counts = df["intention"].value_counts().reset_index()
    counts.columns = ["intention", "count"]
    fig = px.bar(
        counts, x="intention", y="count",
        color="intention",
        color_discrete_sequence=px.colors.qualitative.Set2,
        text="count",
    )
    fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="Messages",
                      height=350, xaxis_tickangle=-30)
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("Répartition des langues détectées")
    if "langue_detectee" in df.columns:
        lang_counts = df["langue_detectee"].value_counts().reset_index()
        lang_counts.columns = ["langue", "count"]
        LANG_LABELS_NICE = {"fr": "Français", "ar": "Arabe", "darija_latin": "Darija", "inconnu": "Inconnu"}
        lang_counts["langue"] = lang_counts["langue"].map(lambda x: LANG_LABELS_NICE.get(x, x))
        fig_lang = px.pie(
            lang_counts, names="langue", values="count",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            hole=0.4,
        )
        fig_lang.update_layout(height=350)
        st.plotly_chart(fig_lang, use_container_width=True)
    else:
        st.info("Colonne langue_detectee non disponible.")

# ─────────────────────────────────────────────────────────────
# GRAPHIQUES LIGNE 2
# ─────────────────────────────────────────────────────────────
col_c, col_d = st.columns(2)

with col_c:
    st.subheader("Confiance moyenne par intention")
    conf_intent = df.groupby("intention")["confiance"].mean().reset_index()
    conf_intent = conf_intent.sort_values("confiance", ascending=True)
    fig3 = px.bar(
        conf_intent, x="confiance", y="intention",
        orientation="h",
        color="confiance",
        color_continuous_scale="RdYlGn",
        range_x=[0, 1],
        text=conf_intent["confiance"].map(lambda x: f"{x:.0%}"),
    )
    fig3.update_layout(height=350, showlegend=False,
                       coloraxis_showscale=False, xaxis_title="Confiance")
    fig3.update_traces(textposition="outside")
    st.plotly_chart(fig3, use_container_width=True)

with col_d:
    st.subheader("Volume de messages par heure")
    heure_counts = df.groupby("heure").size().reset_index(name="messages")
    fig4 = px.area(
        heure_counts, x="heure", y="messages",
        markers=True,
        color_discrete_sequence=["#3B82F6"],
        labels={"heure": "Heure de la journée", "messages": "Messages"},
    )
    fig4.update_layout(height=350)
    st.plotly_chart(fig4, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# ÉVOLUTION TEMPORELLE
# ─────────────────────────────────────────────────────────────
st.subheader("Évolution quotidienne des messages")
if "date" in df.columns:
    daily = df.groupby(["date", "intention"]).size().reset_index(name="count")
    fig5 = px.line(
        daily, x="date", y="count", color="intention",
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Set1,
    )
    fig5.update_layout(height=350, xaxis_title="Date", yaxis_title="Messages")
    st.plotly_chart(fig5, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# TABLEAU CONVERSATIONS RÉCENTES
# ─────────────────────────────────────────────────────────────
st.subheader("💬 Dernières conversations")

cols_afficher = [c for c in ["timestamp", "session_id", "message",
                              "langue_detectee", "intention", "confiance", "reponse"]
                 if c in df.columns]

df_display = df[cols_afficher].sort_values("timestamp", ascending=False).head(25).copy()

if "confiance" in df_display.columns:
    df_display["confiance"] = df_display["confiance"].map(lambda x: f"{x:.0%}")
if "timestamp" in df_display.columns:
    df_display["timestamp"] = df_display["timestamp"].dt.strftime("%d/%m %H:%M")

st.dataframe(df_display, use_container_width=True, height=400)

# ─────────────────────────────────────────────────────────────
# FILTRE PAR SESSION
# ─────────────────────────────────────────────────────────────
st.subheader("🔍 Analyser une session")
sessions_list = df["session_id"].unique().tolist()
session_choisie = st.selectbox("Choisir une session", sessions_list)

if session_choisie:
    df_session = df[df["session_id"] == session_choisie].sort_values("timestamp")
    st.write(f"**{len(df_session)} messages** dans cette session")

    for _, row in df_session.iterrows():
        with st.expander(f"{row.get('timestamp', '')} — {row.get('intention', '')} ({row.get('confiance', 0):.0%})"):
            c1, c2 = st.columns(2)
            c1.write(f"**Message :** {row.get('message', '')}")
            c2.write(f"**Réponse :** {row.get('reponse', '')}")
            if "langue_detectee" in row:
                st.caption(f"Langue : {row['langue_detectee']} | Normalisé : {row.get('texte_normalise', '')}")
