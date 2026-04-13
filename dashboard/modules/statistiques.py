from pymongo import MongoClient

def get_collection():
    client = MongoClient("mongodb://localhost:27018/")
    return client["telecom_algerie"]["dataset_unifie"]

def stats_globales():
    """Retourne les statistiques globales formatées"""
    col = get_collection()
    
    total = col.count_documents({})
    neg = col.count_documents({"label_final": "negatif"})
    pos = col.count_documents({"label_final": "positif"})
    neu = col.count_documents({"label_final": "neutre"})
    
    return {
        'total': total,
        'negatif': neg,
        'positif': pos,
        'neutre': neu,
        'pct_neg': round(neg/total*100, 1),
        'pct_pos': round(pos/total*100, 1),
        'pct_neu': round(neu/total*100, 1)
    }

def stats_par_source():
    """Statistiques par source"""
    col = get_collection()
    
    sources = {}
    for doc in col.find({}, {"sources": 1, "label_final": 1}):
        src = doc.get("sources", "Inconnu")
        if src not in sources:
            sources[src] = {'total': 0, 'neg': 0, 'pos': 0, 'neu': 0}
        sources[src]['total'] += 1
        label = doc.get("label_final", "")
        if label == 'negatif':
            sources[src]['neg'] += 1
        elif label == 'positif':
            sources[src]['pos'] += 1
        elif label == 'neutre':
            sources[src]['neu'] += 1
    
    return sources

def generer_tableau_html():
    """Génère un tableau HTML des statistiques"""
    stats = stats_globales()
    sources = stats_par_source()
    
    html = f"""
    <div style="background: var(--bg-surface); border-radius: 12px; padding: 16px; margin: 10px 0;">
        <h3>📊 Statistiques globales</h3>
        <table style="width: 100%;">
            <tr><td>📝 Total commentaires</td><td><b>{stats['total']:,}</b></td></tr>
            <tr><td>😠 Négatifs</td><td><b>{stats['negatif']}</b> ({stats['pct_neg']}%)</td></tr>
            <tr><td>😊 Positifs</td><td><b>{stats['positif']}</b> ({stats['pct_pos']}%)</td></tr>
            <tr><td>😐 Neutres</td><td><b>{stats['neutre']}</b> ({stats['pct_neu']}%)</td></tr>
        </table>
    </div>
    """
    return html