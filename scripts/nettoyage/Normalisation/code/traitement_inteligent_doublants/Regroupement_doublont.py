from pymongo import MongoClient
import pandas as pd
from collections import Counter

MONGO_URI = "mongodb+srv://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(MONGO_URI)
db = client["telecom_algerie_new"]
collection_originale = db["dataset_unifie_copie"]

print("✅ Connexion réussie")

# Chargement des données
cursor = collection_originale.find({})
df = pd.DataFrame(list(cursor))

colonne_texte = 'normalized_arabert'
if colonne_texte not in df.columns:
    colonne_texte = 'Commentaire_Client_Original'

print(f"📝 Colonne texte : '{colonne_texte}'")

# Convertir les colonnes numériques
if 'confidence' in df.columns:
    df['confidence'] = pd.to_numeric(df['confidence'], errors='coerce')
if 'score' in df.columns:
    df['score'] = pd.to_numeric(df['score'], errors='coerce')

# Groupement
groupes = df.groupby(colonne_texte)

resultats_groupes = []
group_id = 1

for texte, groupe in groupes:
    # 🔑 Fonction pour prendre la première valeur non nulle
    def get_first_value(values):
        if not values:
            return None
        for v in values:
            if v and str(v) != 'nan' and str(v) != '[]':
                return v
        return None
    
    # ==========================================================
    # CHAMPS QUI PRENNENT TOUTES LES VALEURS (listes)
    # ==========================================================
    
    # dates : TOUTES les valeurs (garder la diversité)
    dates = groupe['date'].tolist() if 'date' in df.columns else []
    
    # sources : valeurs UNIQUES (pas besoin de répéter la même source)
    sources = groupe['source'].unique().tolist() if 'source' in df.columns else []
    
    # moderateurs : valeurs UNIQUES
    moderateurs = groupe['moderateur'].unique().tolist() if 'moderateur' in df.columns else []
    
    # labels_originaux : TOUTES les valeurs (important pour les conflits)
    labels = groupe['label'].tolist() if 'label' in df.columns else []
    
    # ==========================================================
    # TOUS LES AUTRES CHAMPS : UNE SEULE VALEUR
    # ==========================================================
    
    commentaire_client_original = get_first_value(groupe['Commentaire_Client_Original'].tolist()) if 'Commentaire_Client_Original' in df.columns else None
    commentaire_moderateur = get_first_value(groupe['commentaire_moderateur'].tolist()) if 'commentaire_moderateur' in df.columns else None
    commentaire_client = get_first_value(groupe['Commentaire_Client'].tolist()) if 'Commentaire_Client' in df.columns else None
    normalized_full = get_first_value(groupe['normalized_full'].tolist()) if 'normalized_full' in df.columns else None
    statut = get_first_value(groupe['statut'].tolist()) if 'statut' in df.columns else None
    
    # Émojis
    emojis_originaux = get_first_value(groupe['emojis_originaux'].tolist()) if 'emojis_originaux' in df.columns else None
    emojis_sentiment = get_first_value(groupe['emojis_sentiment'].tolist()) if 'emojis_sentiment' in df.columns else None
    
    # Score, confidence, reason
    score = get_first_value(groupe['score'].tolist()) if 'score' in df.columns else None
    confidence = get_first_value(groupe['confidence'].tolist()) if 'confidence' in df.columns else None
    reason = get_first_value(groupe['reason'].tolist()) if 'reason' in df.columns else None
    anote = get_first_value(groupe['annoté'].tolist()) if 'annoté' in df.columns else None
    
    # Résolution des conflits de labels
    if labels:
        comptage = Counter(labels)
        label_majoritaire = comptage.most_common(1)[0][0]
        nb_majoritaire = comptage.most_common(1)[0][1]
        nb_total = len(labels)
        
        if nb_majoritaire / nb_total >= 0.6:
            label_final = label_majoritaire
            conflit = False
        else:
            label_final = "CONFLIT_A_REVOIR"
            conflit = True
    else:
        label_final = None
        conflit = False
    
    # Construction du document
    resultats_groupes.append({
        'Group_ID': f"groupe_{group_id:04d}",
        'nb_occurrences': len(groupe),
        
        # Champs avec UNE SEULE valeur
        'Commentaire_Client_Original': commentaire_client_original,
        'commentaire_moderateur': commentaire_moderateur,
        'statut': statut,
        'Commentaire_Client': commentaire_client,
        'normalized_arabert': texte,
        'normalized_full': normalized_full,
        'emojis_originaux': emojis_originaux,
        'emojis_sentiment': emojis_sentiment,
        'score': round(score, 2) if score and not pd.isna(score) else None,
        'confidence': round(confidence, 2) if confidence and not pd.isna(confidence) else None,
        'reason': reason,
        'annoté': anote,
        'label_final': label_final,
        'conflit': conflit,
        
        # Champs avec LISTES
        'dates': dates,                    # TOUTES les dates
        'sources': sources,                # Sources UNIQUES
        'moderateurs': moderateurs,        # Modérateurs UNIQUES
        'labels_originaux': labels         # TOUS les labels
    })
    
    group_id += 1

df_groupes = pd.DataFrame(resultats_groupes)

# Sauvegarde en CSV
df_groupes.to_csv("dataset_groupes_uniques.csv", index=False, encoding='utf-8-sig')
print(f"✅ Dataset groupé sauvegardé : {len(df_groupes)} lignes")

# Création dans MongoDB
print("\n" + "="*60)
print("CRÉATION DANS MONGODB")
print("="*60)

reponse = input("\nVoulez-vous créer la collection 'dataset_unifie_sans_doublons' ? (oui/non) : ")

if reponse.lower() == 'oui':
    nouvelle_collection = db["dataset_unifie_sans_doublons"]
    
    if "dataset_unifie_sans_doublons" in db.list_collection_names():
        reponse2 = input("La collection existe déjà. Voulez-vous la remplacer ? (oui/non) : ")
        if reponse2.lower() == 'oui':
            nouvelle_collection.drop()
            print("   Ancienne collection supprimée")
        else:
            print("   Opération annulée")
            client.close()
            exit()
    
    documents = df_groupes.to_dict('records')
    
    # Convertir les listes en chaînes pour MongoDB
    for doc in documents:
        for key, value in doc.items():
            if isinstance(value, list):
                doc[key] = ','.join(str(v) for v in value)
    
    nouvelle_collection.insert_many(documents)
    print(f"✅ Nouvelle collection créée : {len(documents)} documents")
    
    print("\n📁 Collections disponibles :")
    for name in db.list_collection_names():
        count = db[name].count_documents({})
        print(f"   - {name} : {count} documents")

client.close()
print("\n🔒 Connexion fermée")