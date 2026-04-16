from pymongo import MongoClient
import csv
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI    = "mongodb://localhost:27018/"
DB_NAME      = "telecom_algerie"
TARGET_COLL  = "commentaires_normalises"   # Collection à vérifier
OUTPUT_FILE  = "commentaires_sans_score.csv"  # Fichier de sortie

# ============================================================
# CONNEXION
# ============================================================
print("🔌 Connexion à MongoDB...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Vérifier que la collection existe
if TARGET_COLL not in db.list_collection_names():
    print(f"❌ La collection '{TARGET_COLL}' n'existe pas.")
    client.close()
    exit(1)

collection = db[TARGET_COLL]

# ============================================================
# RECHERCHE DES COMMENTAIRES SANS SCORE
# ============================================================
print(f"\n📊 Recherche des commentaires SANS score dans '{TARGET_COLL}'...")

# Compter
total = collection.count_documents({})
with_score = collection.count_documents({"score": {"$exists": True}})
without_score = collection.count_documents({"score": {"$exists": False}})

print(f"\n📈 STATISTIQUES:")
print(f"   Total documents      : {total}")
print(f"   Avec score           : {with_score}")
print(f"   Sans score           : {without_score}")

if without_score == 0:
    print("\n✅ Tous les commentaires ont un score !")
    client.close()
    exit(0)

# ============================================================
# RÉCUPÉRATION DES COMMENTAIRES SANS SCORE
# ============================================================
print(f"\n📥 Récupération des {without_score} commentaires sans score...")

# Récupérer tous les documents sans score
sans_score_docs = list(collection.find(
    {"score": {"$exists": False}},
    {
        "_id": 1,
        "Commentaire_Client": 1,
        "normalized_arabert": 1,
        "normalized_full": 1,
        "source": 1,
        "date": 1,
        "label": 1,
        "confidence": 1,
        "traite": 1
    }
))

print(f"   ✅ {len(sans_score_docs)} commentaires récupérés")

# ============================================================
# SAUVEGARDE DANS CSV
# ============================================================
print(f"\n💾 Sauvegarde dans '{OUTPUT_FILE}'...")

# Champs à exporter
fieldnames = [
    "_id",
    "Commentaire_Client",
    "normalized_arabert",
    "normalized_full",
    "source",
    "date",
    "label",
    "confidence",
    "traite"
]

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    
    for doc in sans_score_docs:
        # Convertir ObjectId en string
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        writer.writerow(doc)

print(f"   ✅ {len(sans_score_docs)} commentaires sauvegardés")

# ============================================================
# AFFICHER QUELQUES EXEMPLES
# ============================================================
print("\n📋 EXEMPLES de commentaires sans score (5 premiers):")
print("-" * 70)

for i, doc in enumerate(sans_score_docs[:5]):
    commentaire = doc.get("Commentaire_Client", "")[:80]
    source = doc.get("source", "inconnu")
    print(f"{i+1}. [{source}] {commentaire}...")

# ============================================================
# STATISTIQUES DÉTAILLÉES PAR SOURCE
# ============================================================
print("\n📊 STATISTIQUES PAR SOURCE:")
print("-" * 40)

# Grouper par source
sources = {}
for doc in sans_score_docs:
    source = doc.get("source", "inconnu")
    sources[source] = sources.get(source, 0) + 1

for source, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
    print(f"   {source:20} : {count} commentaires")

# ============================================================
# OPTION : AJOUTER SCORE PAR DÉFAUT (0)
# ============================================================
print("\n" + "=" * 70)
print("🔧 OPTIONS POUR CORRIGER")
print("=" * 70)
print(f"\nPour ajouter un score par défaut (0) à ces {without_score} commentaires :")
print("   db.commentaires_normalises.updateMany(")
print("       { score: { $exists: false } },")
print("       { $set: { score: 0, score_ajoute_le: new Date() } }")
print("   )")

# ============================================================
# FERMETURE
# ============================================================
print(f"\n📁 Fichier généré : {OUTPUT_FILE}")
print(f"   Emplacement : {os.path.abspath(OUTPUT_FILE) if 'os' in dir() else OUTPUT_FILE}")

client.close()
print("\n🔒 Connexion fermée.")