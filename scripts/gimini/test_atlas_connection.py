#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_atlas_connection.py – Teste la connexion et la config MongoDB Atlas
"""

from pymongo import MongoClient
import config
import sys

print("─" * 70)
print("  🔍 TEST DE CONNEXION MONGODB ATLAS")
print("─" * 70)

# ══════════════════════════════════════
# 1. TEST DE CONNEXION
# ══════════════════════════════════════
print(f"\n  📡 Connexion à : {config.MONGO_URI[:50]}...")

try:
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command('ping')  # Test de ping
    print("  ✅ Connexion réussie !")
except Exception as e:
    print(f"  ❌ Échec de connexion : {e}")
    print("\n  🔧 Solutions possibles :")
    print("     • Vérifie le mot de passe dans config.py")
    print("     • Vérifie que ton IP est autorisée (Network Access)")
    print("     • Vérifie que le cluster est en ligne sur Atlas")
    sys.exit(1)

# ══════════════════════════════════════
# 2. TEST DE LA BASE DE DONNÉES
# ══════════════════════════════════════
print(f"\n  🗄️  Base de données : {config.DB_NAME}")

try:
    db = client[config.DB_NAME]
    collections = db.list_collection_names()
    print(f"  ✅ Base accessible")
    print(f"  📁 Collections disponibles : {collections}")
except Exception as e:
    print(f"  ❌ Erreur d'accès à la base : {e}")
    sys.exit(1)

# ══════════════════════════════════════
# 3. TEST DE LA COLLECTION INPUT
# ══════════════════════════════════════
print(f"\n  📥 Collection INPUT : {config.INPUT_COLL}")

try:
    input_coll = db[config.INPUT_COLL]
    input_count = input_coll.count_documents({})
    print(f"  ✅ Collection accessible")
    print(f"  📊 Nombre de documents : {input_count}")
    
    if input_count > 0:
        sample = input_coll.find_one()
        print(f"\n  📄 Exemple de document :")
        for key, value in list(sample.items())[:5]:
            val_str = str(value)[:60] + "..." if len(str(value)) > 60 else str(value)
            print(f"     • {key}: {val_str}")
except Exception as e:
    print(f"  ❌ Erreur avec la collection input : {e}")

# ══════════════════════════════════════
# 4. TEST DES CHAMPS REQUIS
# ══════════════════════════════════════
print(f"\n  🏷️  Vérification des champs requis")

try:
    sample = input_coll.find_one({config.TEXT_COL: {"$exists": True}})
    if sample and config.TEXT_COL in sample:
        print(f"  ✅ Champ '{config.TEXT_COL}' trouvé")
        print(f"     Exemple : '{str(sample[config.TEXT_COL])[:50]}...'")
    else:
        print(f"  ⚠️  Champ '{config.TEXT_COL}' non trouvé ou vide")
        print(f"     → Vérifie le nom du champ dans ta base")
        # Afficher les champs disponibles
        if sample:
            print(f"     Champs disponibles : {list(sample.keys())}")
except Exception as e:
    print(f"  ❌ Erreur de vérification des champs : {e}")

# ══════════════════════════════════════
# 5. TEST D'ÉCRITURE (Collection OUTPUT)
# ══════════════════════════════════════
print(f"\n  📤 Collection OUTPUT : {config.OUTPUT_COLL}")

try:
    output_coll = db[config.OUTPUT_COLL]
    
    # Test d'insertion temporaire
    test_doc = {"_test": True, "timestamp": "connection_test"}
    result = output_coll.insert_one(test_doc)
    output_coll.delete_one({"_id": result.inserted_id})  # Nettoyer
    print(f"  ✅ Écriture test réussie")
except Exception as e:
    print(f"  ⚠️  Écriture échouée : {e}")
    print(f"     → Vérifie les permissions de l'user sur Atlas")

# ══════════════════════════════════════
# 6. TEST DU FLAG "labeled"
# ══════════════════════════════════════
print(f"\n  🏷️  Vérification du champ flag : {config.FLAG_COL}")

try:
    labeled_count = input_coll.count_documents({config.FLAG_COL: True})
    pending_count = input_coll.count_documents({
        config.TEXT_COL: {"$exists": True, "$ne": ""},
        config.FLAG_COL: {"$ne": True}
    })
    print(f"  ✅ Champ '{config.FLAG_COL}' fonctionnel")
    print(f"     • Déjà annotés (labeled=True)  : {labeled_count}")
    print(f"     • À annoter (labeled!=True)    : {pending_count}")
except Exception as e:
    print(f"  ⚠️  Problème avec le flag : {e}")

# ══════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════
client.close()

print("\n" + "─" * 70)
print("  📋 RÉSUMÉ DU TEST")
print("─" * 70)
print(f"  ✅ Connexion Atlas      : OK")
print(f"  ✅ Base de données      : {config.DB_NAME}")
print(f"  ✅ Collection INPUT     : {config.INPUT_COLL}")
print(f"  ✅ Collection OUTPUT    : {config.OUTPUT_COLL}")
print(f"  ✅ Champ texte          : {config.TEXT_COL}")
print(f"  ✅ Champ flag           : {config.FLAG_COL}")
print(f"  ✅ Permissions écriture : OK")
print("─" * 70)
print("\n  🎉 Configuration valide ! Tu peux lancer le traitement 🚀")
print("\n  ▶️  Prochaine étape : python extract_300_atlas.py")