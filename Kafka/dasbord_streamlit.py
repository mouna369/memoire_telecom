# #!/usr/bin/env python3
# # dashboard_final.py - Dashboard Streamlit (VERSION CORRECTE)

# # ⚠️ CRUCIAL : set_page_config DOIT être la PREMIÈRE ligne après les imports !
# # Rien avant, pas même de print ou de commentaire exécutable !

# import streamlit as st

# # ============================================================
# # PREMIÈRE COMMANDE STREAMLIT - TOUT DE SUITE !
# # ============================================================
# st.set_page_config(
#     page_title="Dashboard Sentiment",
#     layout="wide",
#     initial_sidebar_state="collapsed"
# )

# # ============================================================
# # TOUS LES AUTRES IMPORTS APRÈS set_page_config
# # ============================================================
# import pandas as pd
# from pymongo import MongoClient
# from datetime import datetime, timedelta
# import plotly.express as px
# import traceback

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# COLLECTION_NAME = "commentaires_predictions"

# # ============================================================
# # CONNEXION MONGODB
# # ============================================================
# @st.cache_resource
# def get_mongo_client():
#     return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

# try:
#     client = get_mongo_client()
#     db = client[DB_NAME]
#     collection = db[COLLECTION_NAME]
#     # Test de connexion
#     total_docs = collection.count_documents({})
#     st.success(f"✅ Connecté à MongoDB - {total_docs} commentaires")
# except Exception as e:
#     st.error(f"❌ Erreur de connexion MongoDB: {e}")
#     st.stop()

# # ============================================================
# # TITRE
# # ============================================================
# st.title("📊 Dashboard Analyse des Commentaires")
# st.markdown("---")

# # ============================================================
# # MÉTRIQUES
# # ============================================================
# try:
#     total = collection.count_documents({})
#     total_pos = collection.count_documents({"label": "POSITIF"})
#     total_neu = collection.count_documents({"label": "NEUTRE"})
#     total_neg = collection.count_documents({"label": "NEGATIF"})
    
#     # Compter les fréquences
#     pipeline_freq = [
#         {"$group": {
#             "_id": "$prediction",
#             "total_frequence": {"$sum": "$frequence"}
#         }}
#     ]
#     freq_data = list(collection.aggregate(pipeline_freq))
#     freq_dict = {d["_id"]: d["total_frequence"] for d in freq_data if d["_id"]}
    
#     col1, col2, col3, col4, col5 = st.columns(5)
    
#     with col1:
#         st.metric("📝 Total commentaires", total)
#     with col2:
#         st.metric("🟢 Positifs", total_pos)
#     with col3:
#         st.metric("🟡 Neutres", total_neu)
#     with col4:
#         st.metric("🔴 Négatifs", total_neg)
#     with col5:
#         st.metric("📊 Fréquences totales", sum(freq_dict.values()) if freq_dict else total)
# except Exception as e:
#     st.error(f"Erreur métriques: {e}")

# st.markdown("---")

# # ============================================================
# # RÉPARTITION DES SENTIMENTS
# # ============================================================
# col1, col2 = st.columns(2)

# with col1:
#     st.subheader("🎭 Sentiments (commentaires uniques)")
#     try:
#         pipeline = [{"$group": {"_id": "$prediction", "count": {"$sum": 1}}}]
#         data = list(collection.aggregate(pipeline))
        
#         if data:
#             df = pd.DataFrame(data)
#             colors = {"POSITIF": "#4CAF50", "NEUTRE": "#FFA500", "NEGATIF": "#FF4B4B"}
#             fig = px.pie(df, values="count", names="_id", color="_id", 
#                          color_discrete_map=colors, title="Répartition par commentaire unique")
#             st.plotly_chart(fig, use_container_width=True)
#         else:
#             st.info("Aucune donnée")
#     except Exception as e:
#         st.info(f"Erreur: {e}")

# with col2:
#     st.subheader("📊 Sentiments (pondérés par fréquence)")
#     try:
#         if freq_dict:
#             df_freq = pd.DataFrame([{"sentiment": k, "total": v} for k, v in freq_dict.items()])
#             colors = {"POSITIF": "#4CAF50", "NEUTRE": "#FFA500", "NEGATIF": "#FF4B4B"}
#             fig = px.pie(df_freq, values="total", names="sentiment", color="sentiment",
#                          color_discrete_map=colors, title="Répartition par fréquence réelle")
#             st.plotly_chart(fig, use_container_width=True)
#         else:
#             st.info("Aucune donnée de fréquence")
#     except Exception as e:
#         st.info(f"Erreur: {e}")

# st.markdown("---")

# # ============================================================
# # TOP COMMENTAIRES FRÉQUENTS
# # ============================================================
# st.subheader("🔝 Top 10 commentaires les plus fréquents")

# try:
#     top_comments = list(collection.find({}).sort("frequence", -1).limit(10))
    
#     if top_comments:
#         for c in top_comments:
#             emoji = {"POSITIF": "🟢", "NEUTRE": "🟡", "NEGATIF": "🔴"}.get(c.get("prediction", ""), "⚪")
#             st.markdown(f"**{emoji} {c.get('prediction', 'N/A')}** - {c.get('frequence', 1)} occurrences")
#             st.markdown(f"> {c.get('commentaire_original', '')[:200]}...")
#             if c.get("derniere_occurrence"):
#                 st.caption(f"Dernière occurrence: {c['derniere_occurrence'].strftime('%Y-%m-%d %H:%M:%S')}")
#             st.divider()
#     else:
#         st.info("Aucun commentaire disponible")
# except Exception as e:
#     st.info(f"Erreur: {e}")

# st.markdown("---")

# # ============================================================
# # DERNIERS COMMENTAIRES
# # ============================================================
# st.subheader("🕐 Derniers commentaires analysés")

# tab1, tab2, tab3 = st.tabs(["🟢 Positifs", "🟡 Neutres", "🔴 Négatifs"])

# with tab1:
#     try:
#         positifs = list(collection.find({"prediction": "POSITIF"}).sort("date_creation", -1).limit(10))
#         if positifs:
#             for c in positifs:
#                 freq = c.get("frequence", 1)
#                 st.markdown(f"**{c.get('commentaire_original', '')[:150]}...**")
#                 date_str = c.get('date_creation', datetime.now()).strftime('%Y-%m-%d %H:%M:%S') if c.get('date_creation') else "Date inconnue"
#                 st.caption(f"Confiance: {c.get('confidence', 0):.2%} | Fréquence: {freq} | {date_str}")
#                 st.divider()
#         else:
#             st.info("Aucun commentaire positif")
#     except Exception as e:
#         st.info(f"Erreur: {e}")

# with tab2:
#     try:
#         neutres = list(collection.find({"prediction": "NEUTRE"}).sort("date_creation", -1).limit(10))
#         if neutres:
#             for c in neutres:
#                 freq = c.get("frequence", 1)
#                 st.markdown(f"**{c.get('commentaire_original', '')[:150]}...**")
#                 date_str = c.get('date_creation', datetime.now()).strftime('%Y-%m-%d %H:%M:%S') if c.get('date_creation') else "Date inconnue"
#                 st.caption(f"Confiance: {c.get('confidence', 0):.2%} | Fréquence: {freq} | {date_str}")
#                 st.divider()
#         else:
#             st.info("Aucun commentaire neutre")
#     except Exception as e:
#         st.info(f"Erreur: {e}")

# with tab3:
#     try:
#         negatifs = list(collection.find({"prediction": "NEGATIF"}).sort("date_creation", -1).limit(10))
#         if negatifs:
#             for c in negatifs:
#                 freq = c.get("frequence", 1)
#                 st.markdown(f"**{c.get('commentaire_original', '')[:150]}...**")
#                 date_str = c.get('date_creation', datetime.now()).strftime('%Y-%m-%d %H:%M:%S') if c.get('date_creation') else "Date inconnue"
#                 st.caption(f"Confiance: {c.get('confidence', 0):.2%} | Fréquence: {freq} | {date_str}")
#                 st.divider()
#         else:
#             st.info("Aucun commentaire négatif")
#     except Exception as e:
#         st.info(f"Erreur: {e}")

# st.markdown("---")
# st.caption(f"🔄 Dernière mise à jour: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


#!/usr/bin/env python3
# dashboard_final.py - Dashboard avec indicateurs de variation

import streamlit as st

# ============================================================
# PREMIÈRE COMMANDE STREAMLIT
# ============================================================
st.set_page_config(
    page_title="Dashboard Sentiment",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================
# IMPORTS
# ============================================================
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import plotly.express as px

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
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

try:
    client = get_mongo_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    total_docs = collection.count_documents({})
    st.success(f"✅ Connecté à MongoDB - {total_docs} commentaires")
except Exception as e:
    st.error(f"❌ Erreur de connexion MongoDB: {e}")
    st.stop()

# ============================================================
# FONCTIONS POUR CALCULER LES VARIATIONS
# ============================================================

def get_variation(collection, champ_label, valeur_label, periode_heures=24):
    """
    Calcule la variation entre maintenant et il y a X heures
    Retourne: (nouveaux, anciens, variation_pct, variation_abs)
    """
    maintenant = datetime.now()
    periode = maintenant - timedelta(hours=periode_heures)
    periode_precedente = periode - timedelta(hours=periode_heures)
    
    # Nouveaux (période actuelle)
    nouveaux = collection.count_documents({
        champ_label: valeur_label,
        "date_creation": {"$gte": periode}
    })
    
    # Anciens (période précédente)
    anciens = collection.count_documents({
        champ_label: valeur_label,
        "date_creation": {"$gte": periode_precedente, "$lt": periode}
    })
    
    if anciens > 0:
        variation_pct = ((nouveaux - anciens) / anciens) * 100
    else:
        variation_pct = 100 if nouveaux > 0 else 0
    
    variation_abs = nouveaux - anciens
    
    return nouveaux, anciens, variation_pct, variation_abs

def get_variation_emoji(variation_pct):
    """Retourne l'emoji et la couleur selon la variation"""
    if variation_pct > 0:
        return "📈", "▲", f"+{variation_pct:.1f}%", "increased"
    elif variation_pct < 0:
        return "📉", "▼", f"{variation_pct:.1f}%", "decreased"
    else:
        return "➡️", "•", "stable", "stable"

# ============================================================
# MÉTRIQUES AVEC VARIATIONS
# ============================================================
st.title("📊 Dashboard Analyse des Commentaires")
st.markdown("---")

# Récupérer les variations
total_pos, _, pos_var_pct, pos_var_abs = get_variation(collection, "label", "POSITIF", 24)
total_neu, _, neu_var_pct, neu_var_abs = get_variation(collection, "label", "NEUTRE", 24)
total_neg, _, neg_var_pct, neg_var_abs = get_variation(collection, "label", "NEGATIF", 24)

# Total général
total = collection.count_documents({})
total_avant = collection.count_documents({"date_creation": {"$lt": datetime.now() - timedelta(hours=24)}})
total_var_abs = total - total_avant
total_var_pct = (total_var_abs / total_avant * 100) if total_avant > 0 else 100

col1, col2, col3, col4 = st.columns(4)

with col1:
    delta_pos = f"{pos_var_abs:+d} ({pos_var_pct:+.1f}%)" if pos_var_abs != 0 else "stable"
    st.metric("🟢 Positifs", total_pos, delta=delta_pos)

with col2:
    delta_neu = f"{neu_var_abs:+d} ({neu_var_pct:+.1f}%)" if neu_var_abs != 0 else "stable"
    st.metric("🟡 Neutres", total_neu, delta=delta_neu)

with col3:
    delta_neg = f"{neg_var_abs:+d} ({neg_var_pct:+.1f}%)" if neg_var_abs != 0 else "stable"
    st.metric("🔴 Négatifs", total_neg, delta=delta_neg)

with col4:
    delta_total = f"{total_var_abs:+d} ({total_var_pct:+.1f}%)" if total_var_abs != 0 else "stable"
    st.metric("📝 Total", total, delta=delta_total)

st.markdown("---")

# ============================================================
# TAUX DE SATISFACTION
# ============================================================
col1, col2 = st.columns(2)

with col1:
    satisfaction = (total_pos / total * 100) if total > 0 else 0
    insatisfaction = (total_neg / total * 100) if total > 0 else 0
    
    # Variation de la satisfaction
    sat_avant = collection.count_documents({
        "label": "POSITIF",
        "date_creation": {"$lt": datetime.now() - timedelta(hours=24)}
    })
    sat_var = total_pos - sat_avant
    sat_var_pct = (sat_var / sat_avant * 100) if sat_avant > 0 else 0
    
    delta_sat = f"{sat_var:+d} ({sat_var_pct:+.1f}%)" if sat_var != 0 else "stable"
    st.metric("😊 Taux de satisfaction", f"{satisfaction:.1f}%", delta=delta_sat)

with col2:
    col2_1, col2_2 = st.columns(2)
    with col2_1:
        st.metric("👍 Positifs", f"{total_pos}", delta=f"{pos_var_abs:+d}")
    with col2_2:
        st.metric("👎 Négatifs", f"{total_neg}", delta=f"{neg_var_abs:+d}")

st.markdown("---")

# ============================================================
# RÉPARTITION DES SENTIMENTS
# ============================================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🎭 Répartition des sentiments")
    try:
        pipeline = [{"$group": {"_id": "$label", "count": {"$sum": "$frequence"}}}]
        data = list(collection.aggregate(pipeline))
        
        if data:
            df = pd.DataFrame(data)
            df = df[df["_id"] is not None]
            colors = {"POSITIF": "#4CAF50", "NEUTRE": "#FFA500", "NEGATIF": "#FF4B4B"}
            fig = px.pie(df, values="count", names="_id", color="_id", 
                         color_discrete_map=colors, title="Répartition des sentiments")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Aucune donnée")
    except Exception as e:
        st.info(f"Erreur: {e}")

with col2:
    st.subheader("📈 Évolution (7 derniers jours)")
    try:
        last_7_days = datetime.now() - timedelta(days=7)
        pipeline_time = [
            {"$match": {"date_creation": {"$gte": last_7_days}}},
            {"$group": {
                "_id": {
                    "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_creation"}},
                    "sentiment": "$label"
                },
                "count": {"$sum": "$frequence"}
            }}
        ]
        time_data = list(collection.aggregate(pipeline_time))
        
        if time_data:
            df_time = pd.DataFrame(time_data)
            df_time["date"] = pd.to_datetime([d["_id"]["date"] for d in time_data])
            df_time["sentiment"] = [d["_id"]["sentiment"] for d in time_data]
            df_time = df_time.rename(columns={"count": "nombre"})
            
            colors = {"POSITIF": "#4CAF50", "NEUTRE": "#FFA500", "NEGATIF": "#FF4B4B"}
            fig = px.line(df_time, x="date", y="nombre", color="sentiment",
                          color_discrete_map=colors, title="Évolution quotidienne")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas assez de données")
    except Exception as e:
        st.info(f"Erreur: {e}")

st.markdown("---")

# ============================================================
# TOP COMMENTAIRES FRÉQUENTS
# ============================================================
st.subheader("🔝 Top 10 commentaires les plus fréquents")

try:
    top_comments = list(collection.find({}).sort("frequence", -1).limit(10))
    
    if top_comments:
        for c in top_comments:
            emoji = {"POSITIF": "🟢", "NEUTRE": "🟡", "NEGATIF": "🔴"}.get(c.get("label", ""), "⚪")
            st.markdown(f"**{emoji} {c.get('label', 'N/A')}** - {c.get('frequence', 1)} occurrences")
            st.markdown(f"> {c.get('commentaire_original', '')[:200]}...")
            if c.get("derniere_occurrence"):
                st.caption(f"Dernière occurrence: {c['derniere_occurrence'].strftime('%Y-%m-%d %H:%M:%S')}")
            st.divider()
    else:
        st.info("Aucun commentaire disponible")
except Exception as e:
    st.info(f"Erreur: {e}")

st.markdown("---")

# ============================================================
# ACTIVITÉ RÉCENTE
# ============================================================
st.subheader("⚡ Activité récente")

col1, col2, col3 = st.columns(3)

# Dernière heure
heure_derniere = datetime.now() - timedelta(hours=1)
activite_heure = collection.count_documents({"date_creation": {"$gte": heure_derniere}})

# Dernières 24 heures
jour_dernier = datetime.now() - timedelta(hours=24)
activite_jour = collection.count_documents({"date_creation": {"$gte": jour_dernier}})

# Dernière semaine
semaine_derniere = datetime.now() - timedelta(days=7)
activite_semaine = collection.count_documents({"date_creation": {"$gte": semaine_derniere}})

with col1:
    st.metric("📊 Dernière heure", activite_heure)
with col2:
    st.metric("📊 Dernières 24h", activite_jour)
with col3:
    st.metric("📊 Dernière semaine", activite_semaine)

st.markdown("---")

# ============================================================
# DERNIERS COMMENTAIRES PAR SENTIMENT
# ============================================================
st.subheader("🕐 Derniers commentaires analysés")

tab1, tab2, tab3 = st.tabs(["🟢 Positifs", "🟡 Neutres", "🔴 Négatifs"])

with tab1:
    try:
        positifs = list(collection.find({"label": "POSITIF"}).sort("date_creation", -1).limit(10))
        if positifs:
            for c in positifs:
                freq = c.get("frequence", 1)
                st.markdown(f"**{c.get('commentaire_original', '')[:150]}...**")
                date_str = c.get('date_creation', datetime.now()).strftime('%Y-%m-%d %H:%M:%S') if c.get('date_creation') else "Date inconnue"
                st.caption(f"Confiance: {c.get('confidence', 0):.2%} | Fréquence: {freq} | {date_str}")
                st.divider()
        else:
            st.info("Aucun commentaire positif")
    except Exception as e:
        st.info(f"Erreur: {e}")

with tab2:
    try:
        neutres = list(collection.find({"label": "NEUTRE"}).sort("date_creation", -1).limit(10))
        if neutres:
            for c in neutres:
                freq = c.get("frequence", 1)
                st.markdown(f"**{c.get('commentaire_original', '')[:150]}...**")
                date_str = c.get('date_creation', datetime.now()).strftime('%Y-%m-%d %H:%M:%S') if c.get('date_creation') else "Date inconnue"
                st.caption(f"Confiance: {c.get('confidence', 0):.2%} | Fréquence: {freq} | {date_str}")
                st.divider()
        else:
            st.info("Aucun commentaire neutre")
    except Exception as e:
        st.info(f"Erreur: {e}")

with tab3:
    try:
        negatifs = list(collection.find({"label": "NEGATIF"}).sort("date_creation", -1).limit(10))
        if negatifs:
            for c in negatifs:
                freq = c.get("frequence", 1)
                st.markdown(f"**{c.get('commentaire_original', '')[:150]}...**")
                date_str = c.get('date_creation', datetime.now()).strftime('%Y-%m-%d %H:%M:%S') if c.get('date_creation') else "Date inconnue"
                st.caption(f"Confiance: {c.get('confidence', 0):.2%} | Fréquence: {freq} | {date_str}")
                st.divider()
        else:
            st.info("Aucun commentaire négatif")
    except Exception as e:
        st.info(f"Erreur: {e}")

st.markdown("---")
st.caption(f"🔄 Dernière mise à jour: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")