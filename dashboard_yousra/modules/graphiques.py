import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pymongo import MongoClient

def get_collection():
    client = MongoClient("mongodb://localhost:27018/")
    return client["telecom_algerie"]["dataset_unifie"]

def graphique_evolution_sentiments():
    """Graphique d'évolution des sentiments dans le temps"""
    col = get_collection()
    
    pipeline = [
        {"$match": {"dates": {"$exists": True}}},
        {"$addFields": {"mois": {"$substr": ["$dates", 3, 2]}}},
        {"$group": {
            "_id": "$mois",
            "neg": {"$sum": {"$cond": [{"$eq": ["$label_final", "negatif"]}, 1, 0]}},
            "pos": {"$sum": {"$cond": [{"$eq": ["$label_final", "positif"]}, 1, 0]}},
            "neu": {"$sum": {"$cond": [{"$eq": ["$label_final", "neutre"]}, 1, 0]}},
            "total": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    
    data = list(col.aggregate(pipeline))
    if not data:
        return None
    
    df = pd.DataFrame([{
        'mois': d['_id'],
        'negatif': round(d['neg']/d['total']*100, 1),
        'positif': round(d['pos']/d['total']*100, 1),
        'neutre': round(d['neu']/d['total']*100, 1)
    } for d in data])
    
    fig = px.line(df, x='mois', y=['negatif', 'positif', 'neutre'],
                  title="📈 Évolution des sentiments",
                  color_discrete_map={'negatif': '#e74c3c', 'positif': '#2ecc71', 'neutre': '#f39c12'})
    
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
    return fig

def graphique_top_mots(limit=10):
    """Graphique des mots les plus fréquents"""
    col = get_collection()
    
    mots = ["5ayeb", "problème", "connexion", "réseau", "service", "lent", "fibre", "installation"]
    compteur = {}
    
    for doc in col.find({"label_final": "negatif"}):
        texte = doc.get("Commentaire_Client_Original", "").lower()
        for mot in mots:
            if mot in texte:
                compteur[mot] = compteur.get(mot, 0) + 1
    
    if not compteur:
        return None
    
    df = pd.DataFrame(sorted(compteur.items(), key=lambda x: x[1], reverse=True)[:limit],
                      columns=['Mot', 'Occurrences'])
    
    fig = px.bar(df, x='Occurrences', y='Mot', orientation='h',
                 title="🔍 Top mots négatifs", color='Occurrences')
    
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
    return fig

def graphique_repartition_sentiments():
    """Camembert de répartition des sentiments"""
    col = get_collection()
    
    neg = col.count_documents({"label_final": "negatif"})
    pos = col.count_documents({"label_final": "positif"})
    neu = col.count_documents({"label_final": "neutre"})
    
    fig = px.pie(values=[neg, pos, neu], names=['Négatifs', 'Positifs', 'Neutres'],
                 title="🥧 Répartition des sentiments",
                 color_discrete_map={'Négatifs': '#e74c3c', 'Positifs': '#2ecc71', 'Neutres': '#f39c12'})
    
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
    return fig