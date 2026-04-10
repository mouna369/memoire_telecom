# collector/collector.py (modifié)
from pymongo import MongoClient
import time
from datetime import datetime
import random

MONGO_HOST = 'mongodb'
MONGO_PORT = 27017
DB_NAME = 'telecom_algerie'

MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db["commentaires_bruts"]

commentaires = [
    "#algerei telecom votre réseau 4G est trop lent depuis 3 jours !!! 😡😡😡 #mauvaisexpérience",
]

def collect():
    texte = random.choice(commentaires)
    doc = {
        "Commentaire_Client": "#algerei telecom votre réseau 4G est trop lent depuis 3 jours !!! 😡😡😡 #mauvaisexpérience",
       "commentaire_moderateur": "Pour une meilleure prise en charge, nous vous invitons à nous fournir plus de détails concernant votre dérangement en message privé, ainsi que toutes les informations liées à votre abonnement, à savoir : Nom et prénom, numéro de téléphone fixe et numéro de mobile. Merci pour votre collaboration. 😊",
        "date": "31/02/2026 23:51",
        "source": "Facebook",
        "moderateur": "mouna",
        "date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "metadata": {"fichier": "collecteur_auto"}

    }

    
    collection.insert_one(doc)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Ajouté: {texte[:50]}...")

if __name__ == "__main__":
    print("🚀 COLLECTEUR DÉMARRÉ (traite=false)")
    while True:
        collect()
        time.sleep(30)