# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # """
# # mark_as_done_atlas.py – Importe les annotations dans MongoDB Atlas
# # """

# # import sys, pandas as pd, re, os
# # from pymongo import MongoClient
# # from datetime import datetime
# # import config

# # if len(sys.argv) < 2:
# #     print("❌ Usage : python mark_as_done_atlas.py <fichier_csv_annoté>")
# #     sys.exit(1)

# # CSV_FILE = sys.argv[1]
# # if not os.path.exists(CSV_FILE):
# #     print(f"❌ Fichier non trouvé : {CSV_FILE}")
# #     sys.exit(1)

# # print("─" * 70)
# # print("  💾 IMPORT DES ANNOTATIONS")
# # print("─" * 70)

# # # ── 1. Lire et nettoyer le CSV ──────────────────────────────────────────
# # try:
# #     with open(CSV_FILE, "r", encoding="utf-8-sig") as f:
# #         raw = f.read()
# # except Exception as e:
# #     print(f"❌ Erreur lecture : {e}"); sys.exit(1)

# # clean     = re.sub(r"```(?:csv)?\s*", "", raw).strip()
# # lines     = clean.split("\n")
# # hdr_idx   = next((i for i, l in enumerate(lines) if "normalized_arabert" in l.lower()), 0)
# # csv_final = "\n".join(lines[hdr_idx:])

# # clean_file = CSV_FILE.replace(".csv", "_clean.csv")
# # with open(clean_file, "w", encoding="utf-8-sig") as f:
# #     f.write(csv_final)

# # # ── CORRECTION : gère les virgules dans le texte arabe ─────────────────
# # try:
# #     df = pd.read_csv(
# #         clean_file,
# #         encoding="utf-8-sig",
# #         quoting=1,          # ← respecte les guillemets
# #         engine='python',    # ← plus tolérant
# #         on_bad_lines='warn' # ← affiche les lignes problématiques au lieu de planter
# #     )
# #     print(f"  ✅ {len(df)} lignes lues")
# # except Exception as e:
# #     print(f"❌ Erreur lecture CSV : {e}")
# #     sys.exit(1)

# # # Vérifier colonnes requises
# # required = ["mongo_id", "normalized_arabert", "label", "score", "confidence"]
# # missing  = [c for c in required if c not in df.columns]
# # if missing:
# #     print(f"  ⚠️  Colonnes manquantes : {missing}")
# #     use_id_join = "mongo_id" in df.columns
# # else:
# #     use_id_join = True
# #     print("  ✅ Jointure par mongo_id (fiable)")

# # # ── 2. Connexion MongoDB ────────────────────────────────────────────────
# # try:
# #     client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
# #     client.admin.command('ping')
# #     db          = client[config.DB_NAME]
# #     input_coll  = db[config.INPUT_COLL]
# #     output_coll = db[config.OUTPUT_COLL]
# #     print(f"  ✅ Connecté à {config.DB_NAME}")
# # except Exception as e:
# #     print(f"❌ Connexion échouée : {e}"); sys.exit(1)

# # # ── 3. Insérer dans OUTPUT ──────────────────────────────────────────────
# # records = []
# # for _, row in df.iterrows():
# #     rec = dict(row)
# #     rec[config.FLAG_COL]  = True
# #     rec["labeled_date"]   = datetime.now()
# #     rec["import_batch"]   = os.path.basename(CSV_FILE)
# #     records.append(rec)

# # result = output_coll.insert_many(records)
# # print(f"  ✅ {len(result.inserted_ids)} documents insérés dans OUTPUT")

# # # ── 4. Synchroniser INPUT par _id ───────────────────────────────────────
# # matched     = 0
# # not_matched = 0
# # errors      = []

# # for _, row in df.iterrows():
# #     try:
# #         if use_id_join and pd.notna(row.get("mongo_id")):
# #             raw_id = str(row["mongo_id"]).strip()
# #             res = input_coll.update_one(
# #                 {"_id": raw_id},
# #                 {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
# #             )
# #         else:
# #             res = input_coll.update_one(
# #                 {config.TEXT_COL: row["normalized_arabert"]},
# #                 {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
# #             )

# #         if res.matched_count > 0:
# #             matched += 1
# #         else:
# #             not_matched += 1
# #             errors.append(str(row.get("mongo_id", "?"))[:30])

# #     except Exception as e:
# #         not_matched += 1
# #         errors.append(f"ERREUR: {str(e)[:40]}")

# # print(f"  ✅ Synchronisés dans INPUT : {matched}")
# # print(f"  ⚠️  Non trouvés (orphelins) : {not_matched}")
# # if errors[:3]:
# #     print(f"  📄 Exemples : {errors[:3]}")

# # # ── 5. Stats finales ────────────────────────────────────────────────────
# # output_total    = output_coll.count_documents({})
# # input_labeled   = input_coll.count_documents({config.FLAG_COL: True})
# # input_remaining = input_coll.count_documents({
# #     config.TEXT_COL: {"$exists": True, "$ne": ""},
# #     config.FLAG_COL: {"$ne": True}
# # })

# # progress = (output_total / 6000) * 100
# # filled   = min(50, int(50 * output_total / 6000))
# # bar      = "█" * filled + "░" * (50 - filled)

# # print(f"\n  📊 OUTPUT total   : {output_total}")
# # print(f"  📊 INPUT annotés  : {input_labeled}")
# # print(f"  📊 INPUT restants : {input_remaining}")
# # print(f"\n  [{bar}] {progress:.1f}% vers 6000")

# # pipeline = [{"$group": {"_id": "$label", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
# # for lbl in output_coll.aggregate(pipeline):
# #     icon = {"positif": "✅", "neutre": "😐", "negatif": "❌"}.get(str(lbl["_id"]), "📄")
# #     pct  = lbl["count"] / output_total * 100
# #     print(f"     {icon} {str(lbl['_id']):10s} : {lbl['count']:5d} ({pct:.1f}%)")

# # client.close()
# # print("\n  ✅ IMPORT TERMINÉ AVEC SUCCÈS")


# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# mark_as_done_atlas.py – Importe les annotations dans MongoDB Atlas
# """

# import sys, re, os, csv
# import pandas as pd
# from pymongo import MongoClient
# from datetime import datetime
# import config

# if len(sys.argv) < 2:
#     print("❌ Usage : python mark_as_done_atlas.py <fichier_csv_annoté>")
#     sys.exit(1)

# CSV_FILE = sys.argv[1]
# if not os.path.exists(CSV_FILE):
#     print(f"❌ Fichier non trouvé : {CSV_FILE}")
#     sys.exit(1)

# print("─" * 70)
# print("  💾 IMPORT DES ANNOTATIONS")
# print("─" * 70)

# # ── 1. Lire et nettoyer le CSV ──────────────────────────────────────────
# try:
#     with open(CSV_FILE, "r", encoding="utf-8-sig") as f:
#         raw = f.read()
# except UnicodeDecodeError:
#     # fallback si l'encodage n'est pas utf-8-sig
#     with open(CSV_FILE, "r", encoding="utf-8") as f:
#         raw = f.read()
# except Exception as e:
#     print(f"❌ Erreur lecture : {e}")
#     sys.exit(1)

# # Supprimer les balises ```csv ``` que Gemini ajoute parfois
# clean = re.sub(r"```(?:csv)?\s*", "", raw).strip()
# clean = re.sub(r"```\s*$", "", clean).strip()

# lines   = clean.split("\n")
# hdr_idx = next((i for i, l in enumerate(lines) if "normalized_arabert" in l.lower()), 0)
# csv_final = "\n".join(lines[hdr_idx:])

# clean_file = CSV_FILE.replace(".csv", "_clean.csv")
# with open(clean_file, "w", encoding="utf-8-sig") as f:
#     f.write(csv_final)

# # ── 2. Lire le CSV de façon robuste ────────────────────────────────────
# # On essaie d'abord avec QUOTE_ALL (format idéal retourné par Gemini)
# # Si ça échoue, on tente en mode souple avec on_bad_lines='skip'
# try:
#     df = pd.read_csv(
#         clean_file,
#         encoding="utf-8-sig",
#         quotechar='"',
#         quoting=csv.QUOTE_ALL,
#         engine='python',
#         on_bad_lines='skip',
#         dtype=str                # tout en string pour éviter les conversions inattendues
#     )
#     print(f"  ✅ {len(df)} lignes lues")
# except Exception as e:
#     print(f"  ⚠️  Lecture stricte échouée ({e}), tentative souple...")
#     try:
#         df = pd.read_csv(
#             clean_file,
#             encoding="utf-8-sig",
#             quotechar='"',
#             engine='python',
#             on_bad_lines='skip',
#             dtype=str
#         )
#         print(f"  ✅ {len(df)} lignes lues (mode souple)")
#     except Exception as e2:
#         print(f"❌ Impossible de lire le CSV : {e2}")
#         sys.exit(1)

# # Nettoyer les espaces dans les noms de colonnes
# df.columns = df.columns.str.strip()

# # ── 3. Vérifier les colonnes requises ──────────────────────────────────
# required = ["mongo_id", "normalized_arabert", "label", "score", "confidence"]
# missing  = [c for c in required if c not in df.columns]
# if missing:
#     print(f"  ⚠️  Colonnes manquantes : {missing}")
#     print(f"  📋 Colonnes trouvées   : {list(df.columns)}")
#     use_id_join = "mongo_id" in df.columns
# else:
#     use_id_join = True
#     print("  ✅ Jointure par mongo_id (fiable)")

# # Supprimer les lignes sans mongo_id valide
# before = len(df)
# df = df[df["mongo_id"].notna() & (df["mongo_id"].str.strip() != "")]
# after = len(df)
# if before != after:
#     print(f"  ⚠️  {before - after} lignes ignorées (mongo_id vide)")

# # ── 4. Connexion MongoDB ────────────────────────────────────────────────
# try:
#     client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
#     client.admin.command('ping')
#     db          = client[config.DB_NAME]
#     input_coll  = db[config.INPUT_COLL]
#     output_coll = db[config.OUTPUT_COLL]
#     print(f"  ✅ Connecté à {config.DB_NAME}")
# except Exception as e:
#     print(f"❌ Connexion échouée : {e}")
#     sys.exit(1)

# # ── 5. Insérer dans OUTPUT ──────────────────────────────────────────────
# records = []
# for _, row in df.iterrows():
#     rec = {k: v for k, v in row.items() if pd.notna(v)}   # ignore les NaN
#     rec[config.FLAG_COL] = True
#     rec["labeled_date"]  = datetime.now()
#     rec["import_batch"]  = os.path.basename(CSV_FILE)
#     records.append(rec)

# if records:
#     result = output_coll.insert_many(records)
#     print(f"  ✅ {len(result.inserted_ids)} documents insérés dans OUTPUT")
# else:
#     print("  ⚠️  Aucun enregistrement à insérer")
#     client.close()
#     sys.exit(1)

# # ── 6. Synchroniser INPUT par _id ───────────────────────────────────────
# matched     = 0
# not_matched = 0
# errors      = []

# for _, row in df.iterrows():
#     try:
#         if use_id_join and pd.notna(row.get("mongo_id")):
#             raw_id = str(row["mongo_id"]).strip()
#             res = input_coll.update_one(
#                 {"_id": raw_id},
#                 {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
#             )
#         else:
#             res = input_coll.update_one(
#                 {config.TEXT_COL: row["normalized_arabert"]},
#                 {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
#             )

#         if res.matched_count > 0:
#             matched += 1
#         else:
#             not_matched += 1
#             errors.append(str(row.get("mongo_id", "?"))[:30])

#     except Exception as e:
#         not_matched += 1
#         errors.append(f"ERREUR: {str(e)[:40]}")

# print(f"  ✅ Synchronisés dans INPUT : {matched}")
# if not_matched > 0:
#     print(f"  ⚠️  Non trouvés (orphelins) : {not_matched}")
#     if errors[:3]:
#         print(f"  📄 Exemples : {errors[:3]}")

# # ── 7. Stats finales ────────────────────────────────────────────────────
# output_total    = output_coll.count_documents({})
# input_labeled   = input_coll.count_documents({config.FLAG_COL: True})
# input_remaining = input_coll.count_documents({
#     config.TEXT_COL: {"$exists": True, "$ne": ""},
#     config.FLAG_COL: {"$ne": True}
# })

# progress = (output_total / 6000) * 100
# filled   = min(50, int(50 * output_total / 6000))
# bar      = "█" * filled + "░" * (50 - filled)

# print(f"\n  📊 OUTPUT total   : {output_total}")
# print(f"  📊 INPUT annotés  : {input_labeled}")
# print(f"  📊 INPUT restants : {input_remaining}")
# print(f"\n  [{bar}] {progress:.1f}% vers 6000")

# pipeline = [{"$group": {"_id": "$label", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
# for lbl in output_coll.aggregate(pipeline):
#     icon = {"positif": "✅", "neutre": "😐", "negatif": "❌"}.get(str(lbl["_id"]), "📄")
#     pct  = lbl["count"] / output_total * 100
#     print(f"     {icon} {str(lbl['_id']):10s} : {lbl['count']:5d} ({pct:.1f}%)")

# client.close()
# print("\n  ✅ IMPORT TERMINÉ AVEC SUCCÈS")

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# mark_as_done_atlas.py – Importe les annotations dans MongoDB Atlas
# """

# import sys, pandas as pd, re, os
# from pymongo import MongoClient
# from datetime import datetime
# import config

# if len(sys.argv) < 2:
#     print("❌ Usage : python mark_as_done_atlas.py <fichier_csv_annoté>")
#     sys.exit(1)

# CSV_FILE = sys.argv[1]
# if not os.path.exists(CSV_FILE):
#     print(f"❌ Fichier non trouvé : {CSV_FILE}")
#     sys.exit(1)

# print("─" * 70)
# print("  💾 IMPORT DES ANNOTATIONS")
# print("─" * 70)

# # ── 1. Lire et nettoyer le CSV ──────────────────────────────────────────
# try:
#     with open(CSV_FILE, "r", encoding="utf-8-sig") as f:
#         raw = f.read()
# except Exception as e:
#     print(f"❌ Erreur lecture : {e}"); sys.exit(1)

# clean     = re.sub(r"```(?:csv)?\s*", "", raw).strip()
# lines     = clean.split("\n")
# hdr_idx   = next((i for i, l in enumerate(lines) if "normalized_arabert" in l.lower()), 0)
# csv_final = "\n".join(lines[hdr_idx:])

# clean_file = CSV_FILE.replace(".csv", "_clean.csv")
# with open(clean_file, "w", encoding="utf-8-sig") as f:
#     f.write(csv_final)

# # ── CORRECTION : gère les virgules dans le texte arabe ─────────────────
# try:
#     df = pd.read_csv(
#         clean_file,
#         encoding="utf-8-sig",
#         quoting=1,          # ← respecte les guillemets
#         engine='python',    # ← plus tolérant
#         on_bad_lines='warn' # ← affiche les lignes problématiques au lieu de planter
#     )
#     print(f"  ✅ {len(df)} lignes lues")
# except Exception as e:
#     print(f"❌ Erreur lecture CSV : {e}")
#     sys.exit(1)

# # Vérifier colonnes requises
# required = ["mongo_id", "normalized_arabert", "label", "score", "confidence"]
# missing  = [c for c in required if c not in df.columns]
# if missing:
#     print(f"  ⚠️  Colonnes manquantes : {missing}")
#     use_id_join = "mongo_id" in df.columns
# else:
#     use_id_join = True
#     print("  ✅ Jointure par mongo_id (fiable)")

# # ── 2. Connexion MongoDB ────────────────────────────────────────────────
# try:
#     client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
#     client.admin.command('ping')
#     db          = client[config.DB_NAME]
#     input_coll  = db[config.INPUT_COLL]
#     output_coll = db[config.OUTPUT_COLL]
#     print(f"  ✅ Connecté à {config.DB_NAME}")
# except Exception as e:
#     print(f"❌ Connexion échouée : {e}"); sys.exit(1)

# # ── 3. Insérer dans OUTPUT ──────────────────────────────────────────────
# records = []
# for _, row in df.iterrows():
#     rec = dict(row)
#     rec[config.FLAG_COL]  = True
#     rec["labeled_date"]   = datetime.now()
#     rec["import_batch"]   = os.path.basename(CSV_FILE)
#     records.append(rec)

# result = output_coll.insert_many(records)
# print(f"  ✅ {len(result.inserted_ids)} documents insérés dans OUTPUT")

# # ── 4. Synchroniser INPUT par _id ───────────────────────────────────────
# matched     = 0
# not_matched = 0
# errors      = []

# for _, row in df.iterrows():
#     try:
#         if use_id_join and pd.notna(row.get("mongo_id")):
#             raw_id = str(row["mongo_id"]).strip()
#             res = input_coll.update_one(
#                 {"_id": raw_id},
#                 {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
#             )
#         else:
#             res = input_coll.update_one(
#                 {config.TEXT_COL: row["normalized_arabert"]},
#                 {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
#             )

#         if res.matched_count > 0:
#             matched += 1
#         else:
#             not_matched += 1
#             errors.append(str(row.get("mongo_id", "?"))[:30])

#     except Exception as e:
#         not_matched += 1
#         errors.append(f"ERREUR: {str(e)[:40]}")

# print(f"  ✅ Synchronisés dans INPUT : {matched}")
# print(f"  ⚠️  Non trouvés (orphelins) : {not_matched}")
# if errors[:3]:
#     print(f"  📄 Exemples : {errors[:3]}")

# # ── 5. Stats finales ────────────────────────────────────────────────────
# output_total    = output_coll.count_documents({})
# input_labeled   = input_coll.count_documents({config.FLAG_COL: True})
# input_remaining = input_coll.count_documents({
#     config.TEXT_COL: {"$exists": True, "$ne": ""},
#     config.FLAG_COL: {"$ne": True}
# })

# progress = (output_total / 6000) * 100
# filled   = min(50, int(50 * output_total / 6000))
# bar      = "█" * filled + "░" * (50 - filled)

# print(f"\n  📊 OUTPUT total   : {output_total}")
# print(f"  📊 INPUT annotés  : {input_labeled}")
# print(f"  📊 INPUT restants : {input_remaining}")
# print(f"\n  [{bar}] {progress:.1f}% vers 6000")

# pipeline = [{"$group": {"_id": "$label", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
# for lbl in output_coll.aggregate(pipeline):
#     icon = {"positif": "✅", "neutre": "😐", "negatif": "❌"}.get(str(lbl["_id"]), "📄")
#     pct  = lbl["count"] / output_total * 100
#     print(f"     {icon} {str(lbl['_id']):10s} : {lbl['count']:5d} ({pct:.1f}%)")

# client.close()
# print("\n  ✅ IMPORT TERMINÉ AVEC SUCCÈS")


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mark_as_done_atlas.py – Importe les annotations dans MongoDB Atlas
"""

import sys, re, os, csv
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import config

if len(sys.argv) < 2:
    print("❌ Usage : python mark_as_done_atlas.py <fichier_csv_annoté>")
    sys.exit(1)

CSV_FILE = sys.argv[1]
if not os.path.exists(CSV_FILE):
    print(f"❌ Fichier non trouvé : {CSV_FILE}")
    sys.exit(1)

print("─" * 70)
print("  💾 IMPORT DES ANNOTATIONS")
print("─" * 70)

# ── 1. Lire et nettoyer le CSV ──────────────────────────────────────────
try:
    with open(CSV_FILE, "r", encoding="utf-8-sig") as f:
        raw = f.read()
except UnicodeDecodeError:
    # fallback si l'encodage n'est pas utf-8-sig
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        raw = f.read()
except Exception as e:
    print(f"❌ Erreur lecture : {e}")
    sys.exit(1)

# Supprimer les balises ```csv ``` que Gemini ajoute parfois
clean = re.sub(r"```(?:csv)?\s*", "", raw).strip()
clean = re.sub(r"```\s*$", "", clean).strip()

lines   = clean.split("\n")
hdr_idx = next((i for i, l in enumerate(lines) if "normalized_arabert" in l.lower()), 0)
csv_final = "\n".join(lines[hdr_idx:])

clean_file = CSV_FILE.replace(".csv", "_clean.csv")
with open(clean_file, "w", encoding="utf-8-sig") as f:
    f.write(csv_final)

# ── 2. Lire le CSV de façon robuste ────────────────────────────────────
# On essaie d'abord avec QUOTE_ALL (format idéal retourné par Gemini)
# Si ça échoue, on tente en mode souple avec on_bad_lines='skip'
try:
    df = pd.read_csv(
        clean_file,
        encoding="utf-8-sig",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        engine='python',
        on_bad_lines='skip',
        dtype=str                # tout en string pour éviter les conversions inattendues
    )
    print(f"  ✅ {len(df)} lignes lues")
except Exception as e:
    print(f"  ⚠️  Lecture stricte échouée ({e}), tentative souple...")
    try:
        df = pd.read_csv(
            clean_file,
            encoding="utf-8-sig",
            quotechar='"',
            engine='python',
            on_bad_lines='skip',
            dtype=str
        )
        print(f"  ✅ {len(df)} lignes lues (mode souple)")
    except Exception as e2:
        print(f"❌ Impossible de lire le CSV : {e2}")
        sys.exit(1)

# Nettoyer les espaces dans les noms de colonnes
df.columns = df.columns.str.strip()

# ── 3. Vérifier les colonnes requises ──────────────────────────────────
required = ["mongo_id", "normalized_arabert", "label", "score", "confidence"]
missing  = [c for c in required if c not in df.columns]
if missing:
    print(f"  ⚠️  Colonnes manquantes : {missing}")
    print(f"  📋 Colonnes trouvées   : {list(df.columns)}")
    use_id_join = "mongo_id" in df.columns
else:
    use_id_join = True
    print("  ✅ Jointure par mongo_id (fiable)")

# Supprimer les lignes sans mongo_id valide
before = len(df)
df = df[df["mongo_id"].notna() & (df["mongo_id"].str.strip() != "")]
after = len(df)
if before != after:
    print(f"  ⚠️  {before - after} lignes ignorées (mongo_id vide)")

# ── 4. Connexion MongoDB ────────────────────────────────────────────────
try:
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command('ping')
    db          = client[config.DB_NAME]
    input_coll  = db[config.INPUT_COLL]
    output_coll = db[config.OUTPUT_COLL]
    print(f"  ✅ Connecté à {config.DB_NAME}")
except Exception as e:
    print(f"❌ Connexion échouée : {e}")
    sys.exit(1)

# ── 5. Insérer dans OUTPUT ──────────────────────────────────────────────
# ── 5. Insérer dans OUTPUT (sans doublons par mongo_id) ─────────────────
# Récupérer les mongo_id déjà présents dans OUTPUT
existing_ids = set(output_coll.distinct("mongo_id"))

records     = []
skipped     = 0

for _, row in df.iterrows():
    mid = str(row.get("mongo_id", "")).strip()
    
    # ← VÉRIFICATION : déjà importé ?
    if mid in existing_ids:
        skipped += 1
        continue
    
    rec = {k: v for k, v in row.items() if pd.notna(v)}
    rec[config.FLAG_COL] = True
    rec["labeled_date"]  = datetime.now()
    rec["import_batch"]  = os.path.basename(CSV_FILE)
    records.append(rec)

if skipped > 0:
    print(f"  ⚠️  {skipped} doublons ignorés (mongo_id déjà dans OUTPUT)")

if records:
    result = output_coll.insert_many(records)
    print(f"  ✅ {len(result.inserted_ids)} documents insérés dans OUTPUT")
else:
    print("  ⚠️  Aucun nouveau document à insérer (tout déjà importé ?)")
    client.close()
    sys.exit(0)

# ── 6. Synchroniser INPUT par _id ───────────────────────────────────────
matched     = 0
not_matched = 0
errors      = []

for _, row in df.iterrows():
    try:
        if use_id_join and pd.notna(row.get("mongo_id")):
            raw_id = str(row["mongo_id"]).strip()
            res = input_coll.update_one(
                {"_id": raw_id},
                {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
            )
        else:
            res = input_coll.update_one(
                {config.TEXT_COL: row["normalized_arabert"]},
                {"$set": {config.FLAG_COL: True, "labeled_date": datetime.now()}}
            )

        if res.matched_count > 0:
            matched += 1
        else:
            not_matched += 1
            errors.append(str(row.get("mongo_id", "?"))[:30])

    except Exception as e:
        not_matched += 1
        errors.append(f"ERREUR: {str(e)[:40]}")

print(f"  ✅ Synchronisés dans INPUT : {matched}")
if not_matched > 0:
    print(f"  ⚠️  Non trouvés (orphelins) : {not_matched}")
    if errors[:3]:
        print(f"  📄 Exemples : {errors[:3]}")

# ── 7. Stats finales ────────────────────────────────────────────────────
output_total    = output_coll.count_documents({})
input_labeled   = input_coll.count_documents({config.FLAG_COL: True})
input_remaining = input_coll.count_documents({
    config.TEXT_COL: {"$exists": True, "$ne": ""},
    config.FLAG_COL: {"$ne": True}
})

progress = (output_total / 6000) * 100
filled   = min(50, int(50 * output_total / 6000))
bar      = "█" * filled + "░" * (50 - filled)

print(f"\n  📊 OUTPUT total   : {output_total}")
print(f"  📊 INPUT annotés  : {input_labeled}")
print(f"  📊 INPUT restants : {input_remaining}")
print(f"\n  [{bar}] {progress:.1f}% vers 6000")

pipeline = [{"$group": {"_id": "$label", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
for lbl in output_coll.aggregate(pipeline):
    icon = {"positif": "✅", "neutre": "😐", "negatif": "❌"}.get(str(lbl["_id"]), "📄")
    pct  = lbl["count"] / output_total * 100
    print(f"     {icon} {str(lbl['_id']):10s} : {lbl['count']:5d} ({pct:.1f}%)")

client.close()
print("\n  ✅ IMPORT TERMINÉ AVEC SUCCÈS")