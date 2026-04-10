from pymongo import MongoClient
import pandas as pd

# ============================================================
# CONNEXION À MONGODB ATLAS
# ============================================================

MONGO_URI = 'mongodb://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net:27017,ac-1ksfahb-shard-00-01.gejzu4a.mongodb.net:27017,ac-1ksfahb-shard-00-02.gejzu4a.mongodb.net:27017/?ssl=true&replicaSet=atlas-mdnqx7-shard-0&authSource=admin&appName=Cluster0'
DB_NAME = 'telecom_algerie_new'
COLLECTION = 'dataset_unifie_sans_doublons'

print("🔌 Connexion à MongoDB Atlas...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

# ============================================================
# EXTRAIRE 10 COMMENTAIRES PAR CLASSE
# ============================================================

print("\n📥 Extraction des commentaires...")

# Classes à extraire
classes = ['negatif', 'positif', 'neutre']

exemples = {}

for classe in classes:
    # Récupérer 10 commentaires de cette classe
    cursor = collection.find({"label_final": classe}).limit(10)
    
    commentaires = []
    for doc in cursor:
        texte = doc.get('normalized_arabert', '')
        # 🔧 CORRECTION : texte au lieu de textes
        if texte and texte != '':
            commentaires.append(texte)
    
    exemples[classe] = commentaires
    print(f"   {classe}: {len(commentaires)} commentaires extraits")

# ============================================================
# AJOUTER LES EXEMPLES DE FORMULES DE POLITESSE (SOCIAL)
# ============================================================

print("\n📝 Recherche de formules de politesse...")

# Mots-clés pour trouver des formules de politesse
mots_sociaux = [
    "ربي يوفقكم", "بارك الله", "ربي يحفظك", "جزاك الله",
    "merci", "شكرا", "bonne année", "رمضان كريم", 
    "kol 3am", "sa3id", "bkhir", "عاشت ايديك"
]

exemples_sociaux = []

# Chercher dans la base
for mot in mots_sociaux:
    cursor = collection.find({
        "normalized_arabert": {"$regex": mot, "$options": "i"}
    }).limit(3)
    
    for doc in cursor:
        texte = doc.get('normalized_arabert', '')
        if texte and texte not in exemples_sociaux:
            exemples_sociaux.append(texte)
            if len(exemples_sociaux) >= 10:
                break
    
    if len(exemples_sociaux) >= 10:
        break

# Si pas assez trouvé, ajouter des exemples par défaut
exemples_sociaux_defaut = [
    "ربي يوفقكم",
    "بارك الله فيك",
    "merci beaucoup",
    "bonne année",
    "رمضان كريم",
    "شكرا جزيلا",
    "ربي يحفظك",
    "kol 3am wentom bkhir",
    "عاشت ايديك",
    "جزاك الله خير"
]

while len(exemples_sociaux) < 10:
    exemples_sociaux.append(exemples_sociaux_defaut[len(exemples_sociaux)])

exemples['social'] = exemples_sociaux[:10]
print(f"   social: {len(exemples['social'])} commentaires extraits")

# ============================================================
# AJOUTER LES EXEMPLES DE PLAINTES DÉGUISÉES (PLAINTE)
# ============================================================

print("\n📝 Recherche de plaintes déguisées...")

# Mots-clés pour trouver des demandes de contact
mots_contact = ["prv", "inbox", "ردو", "خاص", "contactez"]

exemples_plainte = []

for mot in mots_contact:
    cursor = collection.find({
        "Commentaire_Client_Original": {"$regex": mot, "$options": "i"},
        "label_final": "negatif"
    }).limit(5)
    
    for doc in cursor:
        texte = doc.get('normalized_arabert', '')
        if texte and texte not in exemples_plainte:
            exemples_plainte.append(texte)
            if len(exemples_plainte) >= 10:
                break
    
    if len(exemples_plainte) >= 10:
        break

# Si pas assez trouvé, ajouter des exemples par défaut
exemples_plainte_defaut = [
    "repondiw prv service 5ayeb",
    "contactez-nous problème réseau",
    "répondez moi internet coupé",
    "prv connexion lente",
    "inbox service nul",
    "ردو علينا الخاص مشكل",
    "contactez nous fibre optique",
    "jawbouna problème wifi",
    "prv réseau machi kayen",
    "inbox application bug"
]

while len(exemples_plainte) < 10:
    exemples_plainte.append(exemples_plainte_defaut[len(exemples_plainte)])

exemples['plainte'] = exemples_plainte[:10]
print(f"   plainte: {len(exemples['plainte'])} commentaires extraits")

# ============================================================
# AFFICHER LES RÉSULTATS
# ============================================================

print("\n" + "="*60)
print("RÉSULTAT DE L'EXTRACTION")
print("="*60)

for classe, commentaires in exemples.items():
    print(f"\n📁 {classe.upper()} ({len(commentaires)} commentaires) :")
    for i, com in enumerate(commentaires[:5], 1):
        print(f"   {i}. {com[:80]}...")
    if len(commentaires) > 5:
        print(f"   ... et {len(commentaires)-5} autres")

# ============================================================
# SAUVEGARDER DANS UN FICHIER
# ============================================================

# Sauvegarder dans un fichier texte
with open("exemples_prototypes.txt", "w", encoding="utf-8") as f:
    for classe, commentaires in exemples.items():
        f.write(f"\n=== {classe.upper()} ===\n")
        for com in commentaires:
            f.write(f"{com}\n")

print("\n✅ Exemples sauvegardés dans 'exemples_prototypes.txt'")

# ============================================================
# FORMAT POUR LE CODE PROTOTYPICAL NETWORKS
# ============================================================

print("\n" + "="*60)
print("CODE PRÊT À COPIER POUR PROTOTYPICAL NETWORKS")
print("="*60)

print("""
# Copiez ce code dans votre script Prototypical Networks

exemples = {
""")

for classe, commentaires in exemples.items():
    print(f"    '{classe}': [")
    for com in commentaires:
        # Échapper les guillemets dans les commentaires
        com_echappe = com.replace('"', '\\"')
        print(f"        \"{com_echappe}\",")
    print(f"    ],")

print("""
}
""")

# ============================================================
# FERMETURE
# ============================================================

client.close()
print("\n🔒 Connexion fermée")