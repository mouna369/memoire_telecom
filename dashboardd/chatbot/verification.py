from pymongo import MongoClient
from datetime import datetime, timedelta

MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION_NAME = "dataset_unifie_sans_doublons"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def extraire_premiere_date(chaine_dates):
    """Extrait la première date d'une chaîne comme '25/11/2025 08:06,24/11/2025...'"""
    if not chaine_dates or not isinstance(chaine_dates, str):
        return None
    premiere = chaine_dates.split(',')[0].strip()
    for fmt in ['%d/%m/%Y %H:%M', '%d/%m/%Y']:
        try:
            return datetime.strptime(premiere, fmt)
        except:
            continue
    return None

# Récupération des documents
cursor = collection.find({"dates": {"$exists": True, "$ne": None}}, {"dates": 1, "label_final": 1})

stats_mois = {}
stats_semaine = {}

for doc in cursor:
    date_str = doc.get('dates')
    label = doc.get('label_final')
    dt = extraire_premiere_date(date_str)
    if dt is None:
        continue

    # Mois
    mois_key = dt.strftime('%Y-%m')
    if mois_key not in stats_mois:
        stats_mois[mois_key] = {'total': 0, 'neg': 0}
    stats_mois[mois_key]['total'] += 1
    if label == 'negatif':
        stats_mois[mois_key]['neg'] += 1

    # Semaine (même format que le chatbot : %Y-%W, début lundi)
    semaine_key = dt.strftime('%Y-%W')
    if semaine_key not in stats_semaine:
        start_of_week = dt - timedelta(days=dt.weekday())
        stats_semaine[semaine_key] = {'total': 0, 'neg': 0, 'date': start_of_week}
    stats_semaine[semaine_key]['total'] += 1
    if label == 'negatif':
        stats_semaine[semaine_key]['neg'] += 1

# Mois le plus négatif
mois_pct = [(mois, (data['neg']/data['total'])*100, data['total'], data['neg']) for mois, data in stats_mois.items()]
mois_pct.sort(key=lambda x: x[1], reverse=True)
if mois_pct:
    best = mois_pct[0]
    print(f"Mois le plus négatif : {best[0]} avec {best[1]:.1f}% de négatifs ({best[3]} sur {best[2]} commentaires)")

# Semaine avec pic (seuil >= 10 commentaires)
semaines_pct = []
for semaine, data in stats_semaine.items():
    if data['total'] >= 10:
        pct = (data['neg'] / data['total']) * 100
        semaines_pct.append((data['date'], pct, data['total'], data['neg']))
semaines_pct.sort(key=lambda x: x[1], reverse=True)
if semaines_pct:
    best = semaines_pct[0]
    print(f"Pic de négativité : semaine du {best[0].strftime('%d/%m/%Y')} avec {best[1]:.1f}% de négatifs ({best[3]} sur {best[2]} commentaires)")
else:
    print("Aucune semaine avec assez de données (>=10 commentaires).")

client.close()