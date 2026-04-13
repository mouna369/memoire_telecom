import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import gc
from pymongo import MongoClient

# ============================================================
# CONFIGURATION MONGODB
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_sans_urls_arobase"
COLLECTION_DEST = "commentaires_sans_doublons_tfidf"

# ============================================================
# 1. CONNEXION À MONGODB
# ============================================================
print("🔌 Connexion à MongoDB...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(f"✅ Connecté à {DB_NAME}")

# ============================================================
# 2. CHARGER LES DONNÉES DEPUIS MONGODB
# ============================================================
print("\n📂 Chargement depuis MongoDB...")

# Lire depuis MongoDB
cursor = db[COLLECTION_SOURCE].find({})
df = pd.DataFrame(list(cursor))

print(f"✅ {len(df)} commentaires chargés")

# ============================================================
# 3. NETTOYAGE DES TEXTES (sans modifier les colonnes originales)
# ============================================================
print("\n🔧 Nettoyage des textes...")

# Créer une colonne temporaire pour le traitement
df['_texte_temp'] = df['Commentaire_Client'].fillna('').astype(str).str.lower()
df['_texte_temp'] = df['_texte_temp'].str.replace(r'[^\w\s]', ' ', regex=True)
df['_texte_temp'] = df['_texte_temp'].str.replace(r'\s+', ' ', regex=True).str.strip()

# Filtrer les textes trop courts
df = df[df['_texte_temp'].str.len() > 2].copy()

textes = df['_texte_temp'].tolist()
n = len(textes)
print(f"📊 {n} commentaires après nettoyage")

# ============================================================
# 4. TRAITEMENT PAR LOTS (TF-IDF)
# ============================================================
SEUIL = 0.85
LOT_SIZE = 2000

print(f"\n🔄 Traitement par lots de {LOT_SIZE} commentaires...")

tous_gardes = []
indices_supprimes = set()

# Traiter par petits groupes
for debut in range(0, n, LOT_SIZE):
    fin = min(debut + LOT_SIZE, n)
    num_lot = debut // LOT_SIZE + 1
    
    print(f"\n📦 Lot {num_lot}: commentaires {debut} à {fin}")
    
    # Prendre un lot
    lot_textes = textes[debut:fin]
    
    # Calculer TF-IDF sur ce lot seulement
    vectorizer = TfidfVectorizer(
        ngram_range=(1, 2), 
        min_df=2, 
        max_df=0.95,
        sublinear_tf=True
    )
    tfidf = vectorizer.fit_transform(lot_textes)
    similarites = cosine_similarity(tfidf)
    
    # Trouver les doublons dans ce lot
    for i in range(len(lot_textes)):
        idx_global = debut + i
        
        if idx_global in indices_supprimes:
            continue
        
        # Garder ce commentaire (garder TOUTES les colonnes originales)
        document = df.iloc[idx_global].to_dict()
        
        # Supprimer la colonne temporaire avant sauvegarde
        if '_texte_temp' in document:
            del document['_texte_temp']
        if '_id' in document:
            # Convertir ObjectId en string pour MongoDB
            document['_id'] = str(document['_id'])
        
        tous_gardes.append(document)
        
        # Chercher ses doublons
        for j in range(i+1, len(lot_textes)):
            idx_global_j = debut + j
            if idx_global_j not in indices_supprimes and similarites[i][j] >= SEUIL:
                indices_supprimes.add(idx_global_j)
                print(f"   🗑️ Doublon trouvé: {idx_global_j}")
    
    # Nettoyer la mémoire
    del vectorizer, tfidf, similarites
    gc.collect()

# ============================================================
# 5. AFFICHER LES RÉSULTATS
# ============================================================
print("\n" + "="*50)
print("📊 RÉSULTATS FINAUX")
print("="*50)
print(f"   Commentaires originaux : {n}")
print(f"   Commentaires gardés    : {len(tous_gardes)}")
print(f"   Doublons supprimés     : {len(indices_supprimes)}")
print(f"   Réduction              : {len(indices_supprimes)/n*100:.2f}%")

# ============================================================
# 6. SAUVEGARDER DANS MONGODB
# ============================================================
print("\n💾 Sauvegarde dans MongoDB...")

# Supprimer l'ancienne collection si elle existe
db[COLLECTION_DEST].drop()

# Insérer les nouveaux documents
if tous_gardes:
    db[COLLECTION_DEST].insert_many(tous_gardes)
    print(f"✅ {len(tous_gardes)} documents insérés dans {COLLECTION_DEST}")
else:
    print("⚠️ Aucun document à sauvegarder")

# ============================================================
# 7. VÉRIFICATION
# ============================================================
count = db[COLLECTION_DEST].count_documents({})
print(f"\n🔍 Vérification: {count} documents dans {DB_NAME}.{COLLECTION_DEST}")

# Afficher les noms des colonnes conservées
if count > 0:
    sample = db[COLLECTION_DEST].find_one()
    print(f"\n📋 Colonnes conservées: {list(sample.keys())}")

# ============================================================
# FIN
# ============================================================
print("\n" + "="*50)
print("🏁 TRAITEMENT TERMINÉ")
print("="*50)

# Fermer la connexion
client.close()