#!/usr/bin/env python3
# inserer_commentaires.py - Insertion de commentaires dans commentaires_bruts

from pymongo import MongoClient
from datetime import datetime
import random
import uuid

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION_NAME = "commentaires_bruts"

# ============================================================
# COMMENTAIRES DE TEST
# ============================================================

COMMENTAIRES_TEST = [
    # Positifs
    "Très bon service, je recommande !",
    # Négatifs
    "Service nul, connexion coupée depuis 3 jours",
    # Neutres
    "Je voudrais plus d'informations",
]

# ============================================================
# FONCTIONS
# ============================================================

def generer_id_string():
    """Génère un ID string de 24 caractères"""
    return str(uuid.uuid4()).replace("-", "")[:24]

def inserer_commentaire(texte, source="test_manuel"):
    """Insère un seul commentaire"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    doc = {
        "_id": generer_id_string(),
        "Commentaire_Client": texte,
        "source": source,
        "statut": "brut",
        "traite": False,
        "envoye_kafka": False,
        "date": datetime.now(),
        "metadata": {"fichier": "insertion_manuelle"}
    }
    
    result = collection.insert_one(doc)
    client.close()
    
    print(f"✅ Commentaire inséré:")
    print(f"   ID: {doc['_id']}")
    print(f"   Texte: {texte[:50]}...")
    print(f"   Source: {source}")
    return doc['_id']

def inserer_plusieurs_commentaires(commentaires, source="test_manuel"):
    """Insère plusieurs commentaires"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    documents = []
    for texte in commentaires:
        doc = {
            "_id": generer_id_string(),
            "Commentaire_Client": texte,
            "source": source,
            "statut": "brut",
            "traite": False,
            "envoye_kafka": False,
            "date": datetime.now(),
            "metadata": {"fichier": "insertion_multiple"}
        }
        documents.append(doc)
    
    if documents:
        result = collection.insert_many(documents)
        client.close()
        print(f"✅ {len(result.inserted_ids)} commentaires insérés")
        return result.inserted_ids
    
    client.close()
    return []

def inserer_commentaire_aleatoire():
    """Insère un commentaire aléatoire de la liste"""
    texte = random.choice(COMMENTAIRES_TEST)
    return inserer_commentaire(texte, source="test_aleatoire")

def compter_commentaires():
    """Compte le nombre total de commentaires"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    count = collection.count_documents({})
    client.close()
    return count

def compter_commentaires_non_traites():
    """Compte les commentaires non encore traités"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    count = collection.count_documents({"traite": False})
    client.close()
    return count

def compter_commentaires_non_envoyes_kafka():
    """Compte les commentaires non encore envoyés à Kafka"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    count = collection.count_documents({"envoye_kafka": {"$ne": True}, "traite": False})
    client.close()
    return count

def afficher_commentaires(n=10):
    """Affiche les n derniers commentaires"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    print(f"\n📋 Derniers {n} commentaires:")
    print("-" * 60)
    
    for doc in collection.find().sort("date", -1).limit(n):
        print(f"   ID: {doc['_id']}")
        print(f"   Texte: {doc['Commentaire_Client'][:60]}...")
        print(f"   Traité: {doc.get('traite', False)}")
        print(f"   Envoyé Kafka: {doc.get('envoye_kafka', False)}")
        print(f"   Date: {doc.get('date', 'N/A')}")
        print("-" * 60)
    
    client.close()

# ============================================================
# MENU INTERACTIF
# ============================================================

def menu():
    print("=" * 60)
    print("📝 GESTION DES COMMENTAIRES - commentaires_bruts")
    print("=" * 60)
    print(f"\n📊 Statistiques actuelles:")
    print(f"   Total commentaires: {compter_commentaires()}")
    print(f"   Non traités (traite=false): {compter_commentaires_non_traites()}")
    print(f"   Non envoyés à Kafka: {compter_commentaires_non_envoyes_kafka()}")
    
    print("\n" + "=" * 60)
    print("1. Insérer un commentaire personnalisé")
    print("2. Insérer un commentaire aléatoire")
    print("3. Insérer tous les commentaires de test")
    print("4. Afficher les derniers commentaires")
    print("5. Supprimer tous les commentaires (ATTENTION!)")
    print("6. Quitter")
    print("=" * 60)

def main():
    while True:
        menu()
        choix = input("\nVotre choix: ")
        
        if choix == "1":
            texte = input("Entrez votre commentaire: ")
            source = input("Source (facebook/twitter/instagram): ") or "manuel"
            inserer_commentaire(texte, source)
            
        elif choix == "2":
            inserer_commentaire_aleatoire()
            
        elif choix == "3":
            print(f"\n📥 Insertion de {len(COMMENTAIRES_TEST)} commentaires...")
            inserer_plusieurs_commentaires(COMMENTAIRES_TEST, source="batch_test")
            
        elif choix == "4":
            afficher_commentaires(10)
            
        elif choix == "5":
            confirm = input("⚠️ Supprimer TOUS les commentaires ? (oui/non): ")
            if confirm.lower() == "oui":
                client = MongoClient(MONGO_URI)
                db = client[DB_NAME]
                collection = db[COLLECTION_NAME]
                result = collection.delete_many({})
                client.close()
                print(f"✅ {result.deleted_count} commentaires supprimés")
            
        elif choix == "6":
            print("👋 Au revoir !")
            break
        
        print("\n" + "-" * 60)
        input("Appuyez sur Entrée pour continuer...")

# ============================================================
# EXÉCUTION RAPIDE (SANS MENU)
# ============================================================

def inserer_commentaires_rapide():
    """Insertion rapide de quelques commentaires pour tester"""
    print("=" * 60)
    print("🚀 INSERTION RAPIDE DE COMMENTAIRES DE TEST")
    print("=" * 60)
    
    # Insérer quelques commentaires
    inserer_commentaire("Très bon service, je recommande !", "facebook")
    inserer_commentaire("Service nul, connexion coupée", "twitter")
    inserer_commentaire("rependre moi algerei", "facebook")
    inserer_commentaire("Merci beaucoup pour votre aide", "instagram")
    
    print("\n📊 Statistiques finales:")
    print(f"   Total: {compter_commentaires()}")
    print(f"   Non traités: {compter_commentaires_non_traites()}")
    print(f"   Non envoyés Kafka: {compter_commentaires_non_envoyes_kafka()}")

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("📝 INSERTION DE COMMENTAIRES DANS MongoDB")
    print(f"   Base: {DB_NAME}")
    print(f"   Collection: {COLLECTION_NAME}")
    print("=" * 60)
    
    choix = input("\n1. Insertion rapide (quelques commentaires)\n2. Mode interactif\n\nVotre choix: ")
    
    if choix == "1":
        inserer_commentaires_rapide()
    else:
        main()