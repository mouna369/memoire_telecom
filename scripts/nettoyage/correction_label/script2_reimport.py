"""
╔══════════════════════════════════════════════════════════════════════╗
║  SCRIPT 2 — Réimportation des corrections manuelles dans MongoDB    ║
║  Lit les deux Excel corrigés → met à jour label_final dans MongoDB  ║
╚══════════════════════════════════════════════════════════════════════╝

USAGE:
    python script2_reimport.py
    
    Lit : review_MOI.xlsx  +  review_CAMARADE.xlsx  (colonnes label_corrige remplies)
    Écrit : mise à jour MongoDB  label_final  +  annoté=true
"""

import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import math

# ══════════════════════════════════════════════
# ⚙️  CONFIGURATION — même que script1
# ══════════════════════════════════════════════

# MONGO_URI  = "mongodb://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net:27017,ac-1ksfahb-shard-00-01.gejzu4a.mongodb.net:27017,ac-1ksfahb-shard-00-02.gejzu4a.mongodb.net:27017/?ssl=true&replicaSet=atlas-mdnqx7-shard-0&authSource=admin&appName=Cluster0"
MONGO_URI ="mongodb://localhost:27017/"
DB_NAME    = "telecom_algerie_new"
COLLECTION = "dataset_unifie_sans_doublons"
CSV_INPUT  = None   # Si tu travailles sans MongoDB

FILES_TO_REIMPORT = ["review_MOI.xlsx", "review_CAMARADE.xlsx"]

VALEURS_AUTORISEES = {"positif", "negatif", "neutre", "supprimer"}
# ══════════════════════════════════════════════


def load_excel_corrections(filepath):
    """Charge le fichier Excel et extrait les corrections."""
    print(f"\n📂 Lecture : {filepath}")
    try:
        df = pd.read_excel(filepath, sheet_name="📝 A corriger", header=2)
    except Exception as e:
        print(f"   ❌ Erreur lecture : {e}")
        return pd.DataFrame()

    # Cherche les colonnes clés (robuste aux variantes de nom)
    col_id     = next((c for c in df.columns if "_id" in str(c).lower() or "id" in str(c).lower()), None)
    col_corr   = next((c for c in df.columns if "label_corrige" in str(c).lower()), None)
    col_label  = next((c for c in df.columns if "label_final" in str(c).lower()), None)

    if not col_corr:
        print(f"   ❌ Colonne 'label_corrige' introuvable dans {filepath}")
        return pd.DataFrame()

    df_clean = df[[col_id, col_corr, col_label]].copy() if col_label else df[[col_id, col_corr]].copy()
    df_clean.columns = ["_id", "label_corrige"] + (["label_original"] if col_label else [])

    # Garder seulement les lignes remplies
    df_clean = df_clean[
        df_clean["label_corrige"].notna() &
        (df_clean["label_corrige"].astype(str).str.strip() != "")
    ].copy()

    df_clean["label_corrige"] = df_clean["label_corrige"].astype(str).str.strip().str.lower()

    # Valider les valeurs
    invalides = df_clean[~df_clean["label_corrige"].isin(VALEURS_AUTORISEES)]
    if len(invalides) > 0:
        print(f"   ⚠️  {len(invalides)} valeurs invalides ignorées :")
        for _, row in invalides.iterrows():
            print(f"      ID={row['_id']} → '{row['label_corrige']}'")
        df_clean = df_clean[df_clean["label_corrige"].isin(VALEURS_AUTORISEES)]

    print(f"   ✅ {len(df_clean)} corrections valides trouvées")
    return df_clean


def apply_corrections_mongodb(df_all):
    """Applique les corrections dans MongoDB."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db  = client[DB_NAME]
    col = db[COLLECTION]

    stats = {"mis_a_jour": 0, "supprimes": 0, "introuvables": 0, "erreurs": 0}

    for _, row in df_all.iterrows():
        doc_id     = str(row["_id"]).strip()
        new_label  = row["label_corrige"]

        # Construire le filtre MongoDB
        try:
            filt = {"_id": ObjectId(doc_id)}
        except:
            filt = {"_id": doc_id}

        try:
            if new_label == "supprimer":
                res = col.delete_one(filt)
                if res.deleted_count:
                    stats["supprimes"] += 1
                else:
                    stats["introuvables"] += 1
            else:
                res = col.update_one(
                    filt,
                    {"$set": {
                        "label_final": new_label,
                        "annoté": True,
                        "corrected_manually": True,
                        "label_source": "correction_manuelle"
                    }}
                )
                if res.matched_count:
                    stats["mis_a_jour"] += 1
                else:
                    stats["introuvables"] += 1

        except Exception as e:
            print(f"   ⚠️  Erreur doc {doc_id}: {e}")
            stats["erreurs"] += 1

    return stats


def apply_corrections_csv(df_all, csv_path):
    """Applique les corrections sur un CSV local (si pas MongoDB)."""
    print(f"\n📂 Mise à jour CSV : {csv_path}")
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except:
        df = pd.read_csv(csv_path, encoding="latin1")

    # Index par _id
    if "_id" in df.columns:
        df = df.set_index("_id")

    stats = {"mis_a_jour": 0, "supprimes": 0, "introuvables": 0, "erreurs": 0}

    ids_supprimer = df_all[df_all["label_corrige"] == "supprimer"]["_id"].tolist()
    df = df[~df.index.astype(str).isin([str(x) for x in ids_supprimer])]
    stats["supprimes"] = len(ids_supprimer)

    for _, row in df_all[df_all["label_corrige"] != "supprimer"].iterrows():
        doc_id = str(row["_id"]).strip()
        if doc_id in df.index.astype(str):
            df.at[doc_id, "label_final"] = row["label_corrige"]
            df.at[doc_id, "corrected_manually"] = True
            stats["mis_a_jour"] += 1
        else:
            stats["introuvables"] += 1

    df.reset_index().to_csv(csv_path, index=False, encoding="utf-8")
    return stats


def export_corrected_for_chi2(df_all):
    """Exporte un CSV propre pour la Phase 3 (Chi2 + dissimilarité)."""
    print("\n📤 Export CSV pour Phase 3 (Chi2)...")

    # Reconnexion pour récupérer tout le corpus avec labels mis à jour
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    col    = client[DB_NAME][COLLECTION]

    docs = list(col.find({"label_final": {"$in": ["positif","negatif","neutre"]}}))
    df   = pd.DataFrame(docs)
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    output = "corpus_pour_chi2.csv"
    df.to_csv(output, index=False, encoding="utf-8")
    print(f"   ✅ {len(df)} documents exportés → {output}")

    # Stats rapides
    dist = df["label_final"].value_counts()
    print("\n📊 Distribution des labels après correction :")
    for label, count in dist.items():
        pct = count / len(df) * 100
        bar = "█" * int(pct / 2)
        print(f"   {label:<12} {count:>6}  ({pct:.1f}%)  {bar}")

    return output


def main():
    print("=" * 60)
    print("  SCRIPT 2 — Réimportation des corrections manuelles")
    print("=" * 60)

    # Charger tous les fichiers Excel corrigés
    dfs = []
    for f in FILES_TO_REIMPORT:
        df = load_excel_corrections(f)
        if len(df) > 0:
            dfs.append(df)

    if not dfs:
        print("\n❌ Aucune correction trouvée dans les fichiers Excel.")
        print("   Vérifie que la colonne 'label_corrige' est bien remplie.")
        return

    df_all = pd.concat(dfs, ignore_index=True)

    # Supprimer les doublons d'ID (priorité au dernier fichier)
    df_all = df_all.drop_duplicates(subset=["_id"], keep="last")
    print(f"\n📊 Total corrections à appliquer : {len(df_all)}")
    print(f"   positif   : {len(df_all[df_all['label_corrige']=='positif'])}")
    print(f"   negatif   : {len(df_all[df_all['label_corrige']=='negatif'])}")
    print(f"   neutre    : {len(df_all[df_all['label_corrige']=='neutre'])}")
    print(f"   supprimer : {len(df_all[df_all['label_corrige']=='supprimer'])}")

    # Appliquer
    if CSV_INPUT:
        stats = apply_corrections_csv(df_all, CSV_INPUT)
    else:
        stats = apply_corrections_mongodb(df_all)

    print(f"\n✅ Résultat :")
    print(f"   Mis à jour  : {stats['mis_a_jour']}")
    print(f"   Supprimés   : {stats['supprimes']}")
    print(f"   Introuvables: {stats['introuvables']}")
    print(f"   Erreurs     : {stats['erreurs']}")

    # Export pour Phase 3
    if not CSV_INPUT:
        export_corrected_for_chi2(df_all)

    print("\n✅ Terminé ! Lance maintenant script3_chi2_phase3.py")

if __name__ == "__main__":
    main()
