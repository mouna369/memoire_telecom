#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supprimer de MongoDB (dataset_unifie) les commentaires qui apparaissent à la fois
dans un fichier CSV et dans un fichier Excel (intersection).
Colonne utilisée : "Commentaire_Client_Original" (modifiable).
"""

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# ================= CONFIGURATION =================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION = "dataset_unifie"

CSV_FILE = "commentaires_manquants_uniife_sans.csv"
EXCEL_FILE = "supprimes_v3.xlsx"

# Colonne à utiliser pour la comparaison (texte du commentaire)
# Si vous voulez utiliser un champ "_id" (ObjectId), mettez USE_ID_FIELD = True
COLUMN_NAME = "Commentaire_Client_Original"
USE_ID_FIELD = False   # Mettre True si les fichiers contiennent des _id (ObjectId) dans une colonne "_id"

# ================= FONCTIONS =================
def charger_valeurs(fichier, colonne):
    """Charge les valeurs uniques d'une colonne depuis un fichier CSV ou Excel."""
    if fichier.endswith('.csv'):
        df = pd.read_csv(fichier, encoding='utf-8-sig')
    else:
        df = pd.read_excel(fichier)
    
    if colonne not in df.columns:
        raise KeyError(f"La colonne '{colonne}' n'existe pas dans {fichier}. "
                       f"Colonnes trouvées : {list(df.columns)}")
    
    # Nettoyer : supprimer les lignes vides et les doublons
    valeurs = df[colonne].dropna().astype(str).str.strip()
    valeurs = valeurs[valeurs != ""].unique()
    return set(valeurs)

def supprimer_de_mongodb(valeurs_a_supprimer, use_id=False, champ_texte=COLUMN_NAME):
    """Supprime de la collection les documents dont la valeur (texte ou _id) est dans l'ensemble."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION]
    
    total_supprimes = 0
    batch_size = 1000
    valeurs_list = list(valeurs_a_supprimer)
    
    for i in range(0, len(valeurs_list), batch_size):
        batch = valeurs_list[i:i+batch_size]
        if use_id:
            # On suppose que les valeurs sont des ObjectId valides (strings ou ObjectId)
            # Pour être sûr, on peut convertir en ObjectId si nécessaire
            from bson.objectid import ObjectId
            object_ids = []
            for v in batch:
                try:
                    object_ids.append(ObjectId(v))
                except:
                    print(f"⚠️  '{v}' n'est pas un ObjectId valide, ignoré.")
            if object_ids:
                result = collection.delete_many({"_id": {"$in": object_ids}})
                total_supprimes += result.deleted_count
        else:
            result = collection.delete_many({champ_texte: {"$in": batch}})
            total_supprimes += result.deleted_count
        print(f"   → Lot {i//batch_size + 1} : {result.deleted_count} supprimés")
    
    client.close()
    return total_supprimes

# ================= MAIN =================
def main():
    print("📂 Lecture des fichiers...")
    try:
        valeurs_csv = charger_valeurs(CSV_FILE, COLUMN_NAME)
        print(f"   → {len(valeurs_csv)} valeurs uniques dans {CSV_FILE}")
        
        valeurs_excel = charger_valeurs(EXCEL_FILE, COLUMN_NAME)
        print(f"   → {len(valeurs_excel)} valeurs uniques dans {EXCEL_FILE}")
        
    except KeyError as e:
        print(f"❌ {e}")
        return
    
    # Intersection : commentaires présents dans les deux fichiers
    intersection = valeurs_csv.intersection(valeurs_excel)
    print(f"\n🔍 Commentaires communs (intersection) : {len(intersection)}")
    
    if not intersection:
        print("✅ Aucun commentaire commun à supprimer.")
        return
    
    # Aperçu des premiers
    print("\n🔍 Aperçu des 5 premiers commentaires communs :")
    for i, val in enumerate(list(intersection)[:5], 1):
        print(f"   {i}. {val[:80]}...")
    if len(intersection) > 5:
        print(f"   ... et {len(intersection)-5} autres.")
    
    rep = input(f"\n⚠️  Voulez-vous supprimer ces {len(intersection)} commentaires de la collection '{COLLECTION}' ? (oui/NON) : ")
    if rep.strip().lower() not in ("oui", "o", "yes", "y"):
        print("Opération annulée.")
        return
    
    print("\n🗑️ Suppression en cours...")
    supprimes = supprimer_de_mongodb(intersection, use_id=USE_ID_FIELD, champ_texte=COLUMN_NAME)
    print(f"\n✅ Terminé : {supprimes} documents supprimés.")

if __name__ == "__main__":
    main()