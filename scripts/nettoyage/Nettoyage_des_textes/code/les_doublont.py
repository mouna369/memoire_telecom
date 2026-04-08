from pymongo import MongoClient
import pandas as pd
import json

# ============================================================
# CONNEXION À MONGODB ATLAS
# ============================================================

MONGO_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(MONGO_URI)
db = client["telecom_algerie_new"]
collection = db["dataset_unifie"]

print("✅ Connexion à MongoDB Atlas réussie")

# ============================================================
# EXTRAIRE 100 COMMENTAIRES
# ============================================================

print("\n📝 Extraction de 100 commentaires...")

commentaires_bruts = list(collection.aggregate([
    { "$sample": { "size": 1000 } }
]))

print(f"✅ {len(commentaires_bruts)} commentaires extraits")

# ============================================================
# NETTOYAGE DES CARACTÈRES POUR ÉVITER L'ERREUR UTF-8
# ============================================================

def nettoyer_chaine(texte):
    """
    Nettoie les chaînes de caractères pour éviter les erreurs UTF-8
    """
    if not isinstance(texte, str):
        return texte
    
    # Remplacer les caractères problématiques
    texte = texte.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
    
    # Supprimer les caractères non imprimables
    texte = ''.join(c for c in texte if c.isprintable() or c in '\n\r\t')
    
    return texte

def nettoyer_document(doc):
    """
    Nettoie tous les champs texte d'un document
    """
    doc_nettoye = {}
    for key, value in doc.items():
        if isinstance(value, str):
            doc_nettoye[key] = nettoyer_chaine(value)
        elif isinstance(value, dict):
            doc_nettoye[key] = nettoyer_document(value)
        elif isinstance(value, list):
            doc_nettoye[key] = [nettoyer_chaine(v) if isinstance(v, str) else v for v in value]
        else:
            doc_nettoye[key] = value
    return doc_nettoye

# Nettoyer tous les commentaires
commentaires_nettoyes = [nettoyer_document(doc) for doc in commentaires_bruts]

# ============================================================
# AFFICHER LES PREMIERS RÉSULTATS
# ============================================================

print("\n📊 Aperçu des 5 premiers commentaires :")
print("-" * 60)

# Utiliser le bon champ pour le commentaire
champ_commentaire = 'normalized_arabert'  # D'après vos colonnes

for i, doc in enumerate(commentaires_nettoyes[:5]):
    commentaire = doc.get(champ_commentaire, doc.get('commentaire_moderateur', 'Pas de texte'))
    sentiment = doc.get('label', doc.get('sentiment', 'Non annoté'))
    
    # Tronquer si trop long
    com_str = str(commentaire)[:80] if commentaire else 'Pas de texte'
    
    print(f"{i+1}. Commentaire : {com_str}...")
    print(f"   Sentiment : {sentiment}")
    print(f"   Source : {doc.get('source', 'Inconnue')}")
    print(f"   Date : {doc.get('date', 'Inconnue')}")
    print()

# ============================================================
# CONVERTIR EN DATAFRAME
# ============================================================

df = pd.DataFrame(commentaires_nettoyes)

print(f"📊 DataFrame créé : {df.shape[0]} lignes, {df.shape[1]} colonnes")
print(f"Colonnes disponibles : {list(df.columns)}")

# ============================================================
# SAUVEGARDER (VERSION CORRIGÉE
# ============================================================

# Sauvegarder en CSV (plus tolérant que JSON)
df.to_csv("test_100_commentaires.csv", index=False, encoding='utf-8-sig')
print("\n✅ 100 commentaires sauvegardés dans 'test_100_commentaires.csv'")

# Sauvegarder en JSON avec gestion des erreurs
try:
    # Convertir en dict et sauvegarder manuellement
    data = df.to_dict(orient='records')
    
    with open("test_100_commentaires.json", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print("✅ Sauvegardé aussi en JSON")
except Exception as e:
    print(f"⚠️ Erreur JSON (ignorée) : {e}")
    print("   Le fichier CSV est disponible")

# ============================================================
# EXTRAIRE LES DOUBLONS (SUR LE CHAMP COMMENTAIRE)
# ============================================================

from collections import defaultdict

# Utiliser le bon champ pour les doublons
champ_doublons = 'Commentaire_Client_Original'

if champ_doublons in df.columns:
    commentaires = df[champ_doublons].fillna('').astype(str).tolist()
    
    groupes = defaultdict(list)
    for i, com in enumerate(commentaires):
        if com and com != '':  # Ignorer les vides
            groupes[com].append(i)
    
    print("\n🔍 Détection des doublons :")
    print("-" * 40)
    
    nb_doublons = 0
    for com, indices in groupes.items():
        if len(indices) > 1:
            nb_doublons += 1
            print(f"\n🔴 DOUBLON : '{com[:50]}...'")
            print(f"   → Apparaît {len(indices)} fois")
            print(f"   → Indices : {indices}")
            print(f"   → Sentiments : {df.iloc[indices]['label'].tolist()}")
    
    if nb_doublons == 0:
        print("\n🟢 Aucun doublon détecté dans les 100 commentaires")
else:
    print(f"\n⚠️ Champ '{champ_doublons}' non trouvé pour la détection des doublons")
    print(f"   Champs disponibles : {list(df.columns)}")

# Fermer la connexion
client.close()
print("\n🔒 Connexion fermée")