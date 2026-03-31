# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# transf_commentaires_to_atlas.py – Transfère uniquement la collection commentaire_brutes
# """

# from pymongo import MongoClient
# from config import MONGO_URI  # ← Import direct des variables, pas de "config."
# import time

# def copy_commentaires_to_atlas():
#     """
#     Copie uniquement la collection 'commentaire_brutes' de la base locale vers Atlas
#     """
#     # Connexion à MongoDB local
#     print("🔌 Connexion à MongoDB local...")
#     local_client = MongoClient('localhost', 27018)
#     local_db = local_client['telecom_algerie']
    
#     # Vérifier si la collection existe
#     if 'commentaires_bruts' not in local_db.list_collection_names():
#         print("❌ Erreur: La collection 'commentaire_brutes' n'existe pas dans la base 'telecom_algerie'")
#         local_client.close()
#         return
    
#     # Connexion à MongoDB Atlas
#     print("🔌 Connexion à MongoDB Atlas...")
#     atlas_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
#     atlas_client.admin.command('ping')  # Test de connexion
#     atlas_db = atlas_client['telecom_algerie_new']
#     print("  ✅ Atlas connecté")

    
#     # Copier la collection commentaire_brutes
#     collection_name = 'commentaires_normalises'
#     print(f"\n🔄 Copie de la collection '{collection_name}'...")
    
#     source_collection = local_db[collection_name]
#     target_collection = atlas_db[collection_name]
    
#     # Compter les documents avant transfert
#     total_documents = source_collection.count_documents({})
#     print(f"   📊 Nombre total de documents à copier: {total_documents}")
    
#     # Récupérer tous les documents
#     documents = list(source_collection.find())
    
#     if documents:
#         # Insérer dans la nouvelle base
#         try:
#             result = target_collection.insert_many(documents)
#             print(f"   ✅ {len(result.inserted_ids)} documents copiés avec succès")
#         except Exception as e:
#             print(f"   ❌ Erreur lors de l'insertion: {e}")
#             local_client.close()
#             atlas_client.close()
#             return
#     else:
#         print(f"   ⚠️ La collection est vide")
    
#     # Vérification
#     print(f"\n📊 Vérification dans Atlas (base: telecom_algerie_new):")
#     if collection_name in atlas_db.list_collection_names():
#         count = atlas_db[collection_name].count_documents({})
#         print(f"   - {collection_name}: {count} documents")
        
#         if count == total_documents:
#             print("   ✅ Vérification réussie: tous les documents ont été transférés")
#         else:
#             print(f"   ⚠️ Attention: {total_documents} documents source, {count} documents dans Atlas")
#     else:
#         print(f"   ❌ La collection '{collection_name}' n'a pas été créée dans Atlas")
    
#     # Fermeture des connexions
#     local_client.close()
#     atlas_client.close()
#     print("\n✅ Transfert terminé avec succès!")

# # Utilisation
# if __name__ == "__main__":
#     copy_commentaires_to_atlas()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
transf_local_vers_atlas.py – Transfère commentaires_normalises vers Atlas avec meilleure gestion
"""

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
from config import MONGO_URI
import time
import sys

def test_atlas_connection():
    """Test la connexion à Atlas avec différentes options"""
    
    print("🔌 Test de connexion à MongoDB Atlas...")
    
    # Options de connexion
    connection_options = [
        # Option 1: URI standard
        {"uri": MONGO_URI, "name": "URI standard"},
        
        # Option 2: Avec timeout plus long
        {"uri": MONGO_URI, "serverSelectionTimeoutMS": 30000, "name": "Timeout 30s"},
        
        # Option 3: Sans vérification SSL
        {"uri": MONGO_URI, "tlsAllowInvalidCertificates": True, "name": "SSL désactivé"},
    ]
    
    for options in connection_options:
        try:
            print(f"\n📡 Essai avec: {options.get('name', 'Standard')}")
            
            # Extraire l'URI et les options
            uri = options.pop("uri")
            name = options.pop("name", "Standard")
            
            # Connexion
            client = MongoClient(uri, **options)
            
            # Tester la connexion
            client.admin.command('ping')
            print("   ✅ Connexion réussie!")
            
            # Afficher les infos du cluster
            db_list = client.list_database_names()
            print(f"   📊 Bases disponibles: {', '.join(db_list[:5])}...")
            
            return client
            
        except (ServerSelectionTimeoutError, ConnectionFailure) as e:
            print(f"   ❌ Échec: {str(e)[:100]}...")
            continue
        except Exception as e:
            print(f"   ❌ Autre erreur: {e}")
            continue
    
    print("\n❌ Toutes les tentatives de connexion ont échoué")
    return None

def copy_commentaires_to_atlas():
    """
    Copie la collection 'commentaires_normalises' de la base locale vers Atlas
    """
    # 1. Connexion à MongoDB local
    print("\n" + "="*60)
    print("🔌 CONNEXION À MONGODB LOCAL")
    print("="*60)
    
    try:
        local_client = MongoClient('localhost', 27018, serverSelectionTimeoutMS=5000)
        local_client.admin.command('ping')
        print("✅ Connexion à MongoDB locale réussie")
    except Exception as e:
        print(f"❌ Erreur de connexion locale: {e}")
        return
    
    local_db = local_client['telecom_algerie']
    
    # Vérifier si la collection existe
    collection_name = 'commentaires_normalises'
    
    if collection_name not in local_db.list_collection_names():
        print(f"❌ Erreur: La collection '{collection_name}' n'existe pas")
        print(f"📋 Collections disponibles: {local_db.list_collection_names()}")
        local_client.close()
        return
    
    # 2. Connexion à MongoDB Atlas
    print("\n" + "="*60)
    print("🔌 CONNEXION À MONGODB ATLAS")
    print("="*60)
    
    atlas_client = test_atlas_connection()
    
    if not atlas_client:
        print("❌ Impossible de se connecter à Atlas")
        local_client.close()
        return
    
    atlas_db = atlas_client['telecom_algerie_new']
    
    # 3. Transfert des données
    print("\n" + "="*60)
    print("🔄 TRANSFERT DES DONNÉES")
    print("="*60)
    
    source_collection = local_db[collection_name]
    target_collection = atlas_db[collection_name]
    
    # Compter les documents
    total_documents = source_collection.count_documents({})
    print(f"📊 Documents à transférer: {total_documents}")
    
    if total_documents == 0:
        print("⚠️ La collection est vide")
        local_client.close()
        atlas_client.close()
        return
    
    # Vérifier si la collection existe déjà
    if collection_name in atlas_db.list_collection_names():
        existing_count = target_collection.count_documents({})
        print(f"⚠️ La collection existe déjà dans Atlas avec {existing_count} documents")
        reponse = input("Voulez-vous l'écraser? (oui/non): ")
        if reponse.lower() != 'oui':
            print("❌ Transfert annulé")
            local_client.close()
            atlas_client.close()
            return
        target_collection.drop()
        print("🗑️ Ancienne collection supprimée")
        target_collection = atlas_db[collection_name]
    
    # Transfert par lots
    print("\n🔄 Transfert en cours...")
    batch_size = 500
    count = 0
    batch = []
    
    for doc in source_collection.find():
        batch.append(doc)
        count += 1
        
        if len(batch) >= batch_size:
            target_collection.insert_many(batch)
            print(f"   ✅ {count}/{total_documents} documents transférés ({count/total_documents*100:.1f}%)")
            batch = []
    
    # Dernier lot
    if batch:
        target_collection.insert_many(batch)
        print(f"   ✅ {count}/{total_documents} documents transférés (100%)")
    
    # 4. Vérification
    print("\n" + "="*60)
    print("✅ VÉRIFICATION")
    print("="*60)
    
    final_count = target_collection.count_documents({})
    print(f"📊 Documents dans Atlas: {final_count}")
    
    if final_count == total_documents:
        print("✅ Transfert réussi!")
        
        # Afficher un exemple
        sample = target_collection.find_one()
        if sample:
            print(f"\n📝 Exemple de document transféré:")
            print(f"   - ID: {sample.get('_id')}")
            print(f"   - Commentaire: {sample.get('Commentaire_Client', 'N/A')[:100]}...")
    else:
        print(f"⚠️ Attention: {total_documents} source, {final_count} dans Atlas")
    
    # 5. Fermeture des connexions
    local_client.close()
    atlas_client.close()
    print("\n✅ Opération terminée!")

# Solution 3: Script simple pour récupérer la bonne URI
def get_correct_uri():
    """Aide à obtenir la bonne URI depuis MongoDB Atlas"""
    
    print("\n" + "="*60)
    print("🔧 COMMENT OBTENIR LA BONNE URI")
    print("="*60)
    print("""
1. Connectez-vous à MongoDB Atlas: https://cloud.mongodb.com
2. Cliquez sur "Connect" (se connecter)
3. Choisissez "Drivers"
4. Copiez la chaîne de connexion (elle commence par mongodb+srv://)
5. Remplacez <password> par votre mot de passe
6. Mettez à jour config.py avec cette URI
    """)
    
    print("Exemple d'URI correcte:")
    print("mongodb+srv://yousrahadjabderrahmane_db_user:<PASSWORD>@cluster0.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    print("\n" + "="*60)

if __name__ == "__main__":
    # Afficher l'aide pour obtenir la bonne URI
    get_correct_uri()
    
    # Demander confirmation
    reponse = input("\nAvez-vous mis à jour config.py avec la bonne URI? (oui/non): ")
    
    if reponse.lower() == 'oui':
        copy_commentaires_to_atlas()
    else:
        print("❌ Veuillez d'abord mettre à jour config.py avec la bonne URI")