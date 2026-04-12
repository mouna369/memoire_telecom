from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import Counter
import re

# ============================================================
# CONNEXION MONGODB LOCAL
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION_NAME = "dataset_unifie_sans_doublons"

print("🔌 Connexion à MongoDB local...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
print("✅ Connecté")

# ============================================================
# FONCTIONS DE CONVERSION DE DATE (à partir du champ 'dates')
# ============================================================
def parse_date(date_str):
    """Extrait la première date d'une chaîne comme '25/11/2025 08:06,24/11/2025...'"""
    if not date_str or not isinstance(date_str, str):
        return None
    premiere = date_str.split(',')[0].strip()
    for fmt in ['%d/%m/%Y %H:%M', '%d/%m/%Y']:
        try:
            return datetime.strptime(premiere, fmt)
        except:
            continue
    return None

# ============================================================
# STATISTIQUES GLOBALES
# ============================================================
def get_stats_globales():
    total = collection.count_documents({})
    neg = collection.count_documents({"label_final": "negatif"})
    pos = collection.count_documents({"label_final": "positif"})
    neu = collection.count_documents({"label_final": "neutre"})
    return {
        'total': total,
        'negatif': round(neg/total*100, 1) if total else 0,
        'positif': round(pos/total*100, 1) if total else 0,
        'neutre': round(neu/total*100, 1) if total else 0
    }

# ============================================================
# TOP MOTS NÉGATIFS
# ============================================================
def get_top_mots_negatifs(limit=10):
    mots_negatifs = [
        "5ayeb", "خايب", "problème", "مشكل", "nul", "connexion", "réseau",
        "service", "lent", "بطيء", "mauvais", "cher", "غالي", "facture",
        "fibre", "فيبر", "installation", "تركيب", "retard", "تأخير",
        "client", "support", "réponse", "رد", "coupé", "مقطوع"
    ]
    negatifs = list(collection.find({"label_final": "negatif"}))
    compteur = {}
    for doc in negatifs:
        texte = doc.get('Commentaire_Client_Original', '').lower()
        for mot in mots_negatifs:
            if mot.lower() in texte:
                compteur[mot] = compteur.get(mot, 0) + 1
    top = sorted(compteur.items(), key=lambda x: x[1], reverse=True)[:limit]
    return top

# ============================================================
# STATISTIQUES PAR MOIS (basées sur le champ 'dates')
# ============================================================
def get_stats_par_mois():
    tous = list(collection.find({}, {"dates": 1, "label_final": 1}))
    mois_stats = {}
    for doc in tous:
        date_str = doc.get('dates')
        dt = parse_date(date_str)
        if dt is None:
            continue
        mois_key = dt.strftime("%Y-%m")
        label = doc.get('label_final')
        if mois_key not in mois_stats:
            mois_stats[mois_key] = {'total': 0, 'negatif': 0, 'positif': 0, 'neutre': 0}
        mois_stats[mois_key]['total'] += 1
        if label == 'negatif':
            mois_stats[mois_key]['negatif'] += 1
        elif label == 'positif':
            mois_stats[mois_key]['positif'] += 1
        elif label == 'neutre':
            mois_stats[mois_key]['neutre'] += 1
    resultats = []
    for mois, stats in sorted(mois_stats.items()):
        total = stats['total']
        if total == 0:
            continue
        resultats.append({
            'mois': mois,
            'nom': datetime.strptime(mois, "%Y-%m").strftime("%B %Y"),
            'total': total,
            'negatif': round(stats['negatif']/total*100, 1),
            'positif': round(stats['positif']/total*100, 1),
            'neutre': round(stats['neutre']/total*100, 1)
        })
    return resultats

# ============================================================
# STATISTIQUES PAR SOURCE
# ============================================================
def get_stats_par_source():
    pipeline = [
        {"$group": {
            "_id": {"source": "$sources", "label": "$label_final"},
            "count": {"$sum": 1}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    sources = {}
    for r in results:
        src = r['_id']['source']
        lbl = r['_id']['label']
        cnt = r['count']
        if src not in sources:
            sources[src] = {'total': 0, 'negatif': 0, 'positif': 0, 'neutre': 0}
        sources[src]['total'] += cnt
        if lbl == 'negatif':
            sources[src]['negatif'] = cnt
        elif lbl == 'positif':
            sources[src]['positif'] = cnt
        elif lbl == 'neutre':
            sources[src]['neutre'] = cnt
    resultats = []
    for src, stats in sources.items():
        if stats['total'] == 0:
            continue
        resultats.append({
            'source': src,
            'total': stats['total'],
            'negatif': round(stats['negatif']/stats['total']*100, 1),
            'positif': round(stats['positif']/stats['total']*100, 1),
            'neutre': round(stats['neutre']/stats['total']*100, 1)
        })
    return sorted(resultats, key=lambda x: x['total'], reverse=True)

# ============================================================
# TENDANCE (dernier mois vs avant-dernier)
# ============================================================
def get_tendance():
    mois_stats = get_stats_par_mois()
    if len(mois_stats) >= 2:
        dernier = mois_stats[-1]
        avant = mois_stats[-2]
        evolution = dernier['negatif'] - avant['negatif']
        return {
            'evolution': 'augmentation' if evolution > 0 else 'diminution',
            'hausse': abs(evolution),
            'dernier_mois': dernier['negatif'],
            'mois_precedent': avant['negatif'],
            'nom_dernier': dernier['nom'],
            'nom_precedent': avant['nom']
        }
    return None

# ============================================================
# PIC DE NÉGATIVITÉ (semaine avec plus fort % négatif)
# ============================================================
def get_pic_negativite():
    tous = list(collection.find({}, {"dates": 1, "label_final": 1}))
    semaines = {}
    for doc in tous:
        date_str = doc.get('dates')
        dt = parse_date(date_str)
        if dt is None:
            continue
        annee_semaine = dt.strftime("%Y-%W")
        label = doc.get('label_final')
        if annee_semaine not in semaines:
            semaines[annee_semaine] = {'total': 0, 'negatif': 0, 'semaine_start': dt - timedelta(days=dt.weekday())}
        semaines[annee_semaine]['total'] += 1
        if label == 'negatif':
            semaines[annee_semaine]['negatif'] += 1
    meilleur = None
    meilleur_pct = -1
    for semaine, data in semaines.items():
        if data['total'] < 10:   # seuil pour éviter les semaines avec très peu de données
            continue
        pct = data['negatif'] / data['total'] * 100
        if pct > meilleur_pct:
            meilleur_pct = pct
            meilleur = (semaine, data['semaine_start'], data['negatif'], data['total'])
    if meilleur:
        return {
            'semaine': meilleur[1],
            'negatifs': meilleur[2],
            'total': meilleur[3],
            'pct': round(meilleur_pct, 1)
        }
    return None

# ============================================================
# CHATBOT
# ============================================================
class ChatBotAnalyste:
    def __init__(self):
        self.stats = get_stats_globales()
        self.top_mots = get_top_mots_negatifs(10)
        self.mois_stats = get_stats_par_mois()
        self.sources_stats = get_stats_par_source()
        self.tendance = get_tendance()
        self.pic = get_pic_negativite()

    def menu_aide(self):
        return """
🤖 Questions possibles :
   • "Statistiques globales"
   • "Pourquoi les commentaires sont négatifs ?"
   • "Quelle est la tendance ?"
   • "Quel est le mois le plus négatif ?"
   • "Quand y a-t-il eu un pic ?"
   • "Compare Facebook et Instagram"
   • "Résumé"
"""

    def repondre(self, question):
        q = question.lower()

        if "statistiques" in q or "global" in q:
            return f"""
📊 STATISTIQUES GLOBALES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 Total : {self.stats['total']} commentaires
🔴 Négatifs : {self.stats['negatif']}%
🟢 Positifs : {self.stats['positif']}%
⚪ Neutres : {self.stats['neutre']}%
"""

        if "pourquoi" in q and "negatif" in q:
            rep = "🔍 PRINCIPALES CAUSES\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            for i, (mot, count) in enumerate(self.top_mots[:5], 1):
                rep += f"{i}. '{mot}' : {count} occurrences\n"
            return rep

        if "tendance" in q or "evolution" in q:
            if self.tendance:
                return f"""
📈 TENDANCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 {self.tendance['nom_dernier']} : {self.tendance['dernier_mois']}% négatifs
📊 {self.tendance['nom_precedent']} : {self.tendance['mois_precedent']}% négatifs
{'📈' if self.tendance['evolution'] == 'augmentation' else '📉'} {self.tendance['evolution'].upper()} de {abs(self.tendance['hausse'])} points
"""
            return "Pas assez de données pour calculer la tendance."

        if "mois le plus négatif" in q or "pire mois" in q:
            if self.mois_stats:
                pire = max(self.mois_stats, key=lambda x: x['negatif'])
                return f"📉 Le mois le plus négatif est **{pire['nom']}** avec {pire['negatif']}% de négatifs (sur {pire['total']} commentaires)."
            return "Données insuffisantes."

        if "pic" in q or "quand y a-t-il eu un pic" in q:
            if self.pic:
                return f"⚠️ Le pic de négativité a eu lieu la semaine du {self.pic['semaine'].strftime('%d/%m/%Y')} avec {self.pic['pct']}% de négatifs ({self.pic['negatifs']} sur {self.pic['total']} commentaires)."
            return "Aucun pic détecté."

        if "facebook" in q or "instagram" in q or "source" in q:
            rep = "📱 PAR SOURCE\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            for s in self.sources_stats[:3]:
                rep += f"\n📌 {s['source'].upper()} ({s['total']} coms)\n   🔴 Négatifs : {s['negatif']}%\n   🟢 Positifs : {s['positif']}%\n   ⚪ Neutres : {s['neutre']}%\n"
            return rep

        if "resume" in q or "résumé" in q or "rapport" in q:
            rapport = f"""
╔════════════════════════════════════════════════════════════╗
║                    RAPPORT D'ANALYSE                       ║
╚════════════════════════════════════════════════════════════╝

📊 VUE D'ENSEMBLE
• {self.stats['total']} commentaires
• {self.stats['negatif']}% négatifs, {self.stats['positif']}% positifs

🔍 TOP CAUSES
"""
            for i, (mot, count) in enumerate(self.top_mots[:3], 1):
                rapport += f"• {i}. '{mot}' : {count}\n"
            if self.tendance:
                rapport += f"\n📈 TENDANCE : {self.tendance['evolution']} de {abs(self.tendance['hausse'])} points"
            if self.pic:
                rapport += f"\n⚠️ PIC : semaine du {self.pic['semaine'].strftime('%d/%m/%Y')} ({self.pic['pct']}% négatifs)"
            return rapport

        return self.menu_aide()

# ============================================================
# MAIN
# ============================================================
def main():
    chatbot = ChatBotAnalyste()
    print("\n" + "="*50)
    print("🤖 CHATBOT ANALYSTE")
    print("="*50)
    print(chatbot.menu_aide())

    while True:
        question = input("\n💬 Votre question : ").strip()
        if question.lower() in ['quit', 'exit', 'stop', 'q']:
            print("\n👋 Au revoir !")
            break
        if not question:
            continue
        reponse = chatbot.repondre(question)
        print(f"\n🤖 {reponse}")

if __name__ == "__main__":
    main()