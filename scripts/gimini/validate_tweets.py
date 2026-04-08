# ╔══════════════════════════════════════════════════════════════════╗
# ║  VALIDATION TWEET PAR TWEET — Terminal VS Code                  ║
# ║  Stratégie : conf < 0.75 (tous) + 150 par label (échantillon)   ║
# ║  Multi-annotateur : toi + ta binôme sur le même Atlas           ║
# ║                                                                  ║
# ║  Installation : pip install pymongo                             ║
# ║  Lancer      : python validate_tweets.py                        ║
# ╚══════════════════════════════════════════════════════════════════╝

from pymongo import MongoClient
from bson    import ObjectId
import os
import sys
import random
from datetime import datetime

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG — modifier selon votre setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MONGO_URI   = "mongodb://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net:27017,ac-1ksfahb-shard-00-01.gejzu4a.mongodb.net:27017,ac-1ksfahb-shard-00-02.gejzu4a.mongodb.net:27017/?ssl=true&replicaSet=atlas-mdnqx7-shard-0&authSource=admin&appName=Cluster0"
DB_NAME     = "telecom_algerie_new"
COLLECTION  = "dataset_unifie"

# Votre nom (pour tracer qui a validé quoi dans MongoDB)
ANNOTATEUR  = input("\n  Votre prénom (ex: Sara ou Amina) : ").strip()

# Seuils
CONF_SEUIL_BAS   = 0.75   # tous les tweets sous ce seuil
SAMPLE_PAR_LABEL = 150    # échantillon par label au-dessus du seuil

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONNEXION MONGODB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()
    col = client[DB_NAME][COLLECTION]
    print(f"  ✅ Connecté à MongoDB Atlas ({DB_NAME}.{COLLECTION})")
except Exception as e:
    print(f"  ❌ Connexion échouée : {e}")
    sys.exit(1)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COULEURS TERMINAL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
R  = "\033[91m"
Y  = "\033[93m"
G  = "\033[92m"
B  = "\033[94m"
C  = "\033[96m"
W  = "\033[97m"
DIM= "\033[2m"
BO = "\033[1m"
RS = "\033[0m"

LABEL_COLOR = {
    "negatif" : R,
    "neutre"  : Y,
    "positif" : G,
    "ambigu"  : B,
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ✅ FIX — $convert robuste avec onError=None
# Évite le crash quand confidence contient du texte
# ex: "connexion instable" → null (ignoré proprement)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONVERT_CONF = {
    "$convert": {
        "input"  : "$confidence",
        "to"     : "double",
        "onError": None,
        "onNull" : None
    }
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CHARGER LES TWEETS À VALIDER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def charger_tweets():
    print(f"\n  Chargement des tweets en cours...")

    # ── Partie 1 : tous conf < 0.75 ───────────────────────────
    pipeline_douteux = [
        {"$addFields": {"conf_num": CONVERT_CONF}},
        {"$match": {
            "conf_num": {"$lt": CONF_SEUIL_BAS},
            f"validation.{ANNOTATEUR}": {"$exists": False}
        }},
        {"$sort":    {"conf_num": 1}},
        {"$project": {"conf_num": 0}}
    ]
    douteux = list(col.aggregate(pipeline_douteux))
    print(f"   Douteux (conf < {CONF_SEUIL_BAS})  : {len(douteux):>4} tweets")

    # ── Partie 2 : échantillon 150 par label ──────────────────
    labels = ["negatif", "neutre", "positif"]
    sample = []

    for label in labels:
        pipeline_sample = [
            {"$addFields": {"conf_num": CONVERT_CONF}},
            {"$match": {
                "label"   : label,
                "conf_num": {"$gte": CONF_SEUIL_BAS},
                f"validation.{ANNOTATEUR}": {"$exists": False}
            }},
            {"$sample":  {"size": SAMPLE_PAR_LABEL}},
            {"$project": {"conf_num": 0}}
        ]
        s = list(col.aggregate(pipeline_sample))
        sample.extend(s)
        print(f"   Échantillon '{label}'          : {len(s):>4} tweets")

    # ── Fusion + déduplification ───────────────────────────────
    seen = set()
    tous = []
    for doc in douteux + sample:
        sid = str(doc["_id"])
        if sid not in seen:
            seen.add(sid)
            tous.append(doc)

    random.shuffle(tous)

    print(f"\n  {'─'*50}")
    print(f"   Total après dédup      : {len(tous):>4} tweets")
    print(f"  {'─'*50}")

    return tous

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATS GLOBALES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def afficher_stats():
    total    = col.count_documents({})
    val_moi  = col.count_documents({f"validation.{ANNOTATEUR}": {"$exists": True}})
    val_tous = col.count_documents({"validation": {"$exists": True}})
    desaccord= col.count_documents({"accord_annotateurs": False})

    print(f"""
  ╔══════════════════════════════════════════════╗
  ║  STATS — dataset_unifie                     ║
  ║  Total documents         : {total:>6,}          ║
  ║  Validés par {ANNOTATEUR:<12s}: {val_moi:>6,}          ║
  ║  Validés tous annotateurs: {val_tous:>6,}          ║
  ║  Désaccords à résoudre   : {desaccord:>6,}          ║
  ╚══════════════════════════════════════════════╝""")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AFFICHER UN TWEET
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def afficher_tweet(doc, idx, total):
    os.system('cls' if os.name == 'nt' else 'clear')

    texte = (
        doc.get("Commentaire_Client_Original") or
        doc.get("Commentaire_Client") or
        doc.get("normalized_full") or
        "— texte non disponible —"
    )

    label      = doc.get("label", "?")
    confidence = doc.get("confidence", "?")
    score      = doc.get("score", "?")
    reason     = doc.get("reason", "—")
    source     = doc.get("source", "?")
    col_label  = LABEL_COLOR.get(label, W)

    # Tag douteux / sample / conf invalide
    try:
        conf_f = float(confidence)
        tag    = f"{R}⚠ DOUTEUX{RS}" if conf_f < CONF_SEUIL_BAS else f"{G}● SAMPLE{RS}"
    except:
        tag    = f"{Y}? CONF_INVALIDE{RS}"

    # Barre de progression
    filled = int(40 * idx / total)
    bar    = f"{G}{'█' * filled}{DIM}{'░' * (40 - filled)}{RS}"
    pct    = idx / total * 100

    print(f"\n  {bar}  {C}{idx}/{total}{RS}  ({pct:.0f}%)  {tag}")
    print(f"  {DIM}Annotateur : {ANNOTATEUR}   Source : {source}{RS}")
    print(f"\n  {'═'*60}")
    print(f"\n  {BO}{W}{texte}{RS}\n")
    print(f"  {'─'*60}")
    print(f"  Gemini  →  {col_label}{BO}{label.upper():10s}{RS}"
          f"  conf={C}{confidence}{RS}  score={score}")
    print(f"  Raison  →  {DIM}{reason}{RS}")
    print(f"  {'─'*60}")
    print(f"""
  {BO}Ton choix :{RS}

   {G}[1]{RS}  ✅  Confirmer   '{label}'
   {R}[2]{RS}  🔴  Corriger  → {R}negatif{RS}
   {Y}[3]{RS}  🟡  Corriger  → {Y}neutre{RS}
   {G}[4]{RS}  🟢  Corriger  → {G}positif{RS}
   {B}[5]{RS}  🔵  Marquer   → {B}ambigu{RS}  (tweet ambigu, à exclure)
   {DIM}[s]{RS}  ⏭️   Passer     (décider plus tard)
   {DIM}[q]{RS}  💾  Quitter    et sauvegarder la session
    """)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SAUVEGARDER LA DÉCISION DANS MONGODB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def sauvegarder(doc, nouveau_label, confirme):
    label_original = doc.get("label", "")
    now            = datetime.utcnow().isoformat()

    # Chaque annotateur a son propre sous-document → pas de conflit
    col.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            f"validation.{ANNOTATEUR}": {
                "label"       : nouveau_label,
                "label_gemini": label_original,
                "confirme"    : confirme,
                "date"        : now,
            }
        }}
    )

    # Vérifier si les deux annotateurs ont validé → consensus
    doc_maj     = col.find_one({"_id": doc["_id"]})
    validations = doc_maj.get("validation", {}) if doc_maj else {}

    if len(validations) >= 2:
        labels_valides = [v["label"] for v in validations.values()]
        accord         = len(set(labels_valides)) == 1
        label_final    = labels_valides[0] if accord else "desaccord"

        col.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "valide_2annotateurs": True,
                "accord_annotateurs" : accord,
                "label_final"        : label_final,
                "date_consensus"     : now,
            }}
        )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BOUCLE PRINCIPALE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    os.system('cls' if os.name == 'nt' else 'clear')

    print(f"""
  {'='*60}
  {BO}VALIDATION LABELS GEMINI — Telecom Algérie{RS}
  Annotateur : {C}{BO}{ANNOTATEUR}{RS}
  Stratégie  : conf < {CONF_SEUIL_BAS} (tous) + {SAMPLE_PAR_LABEL}/label (échantillon)
  {'='*60}""")

    afficher_stats()

    tweets = charger_tweets()
    total  = len(tweets)

    if total == 0:
        print(f"\n  {G}✅ Aucun tweet restant à valider pour {ANNOTATEUR} !{RS}")
        print(f"  Tous les tweets ont déjà été traités.\n")
        return

    input(f"\n  Appuie sur {BO}ENTRÉE{RS} pour commencer ({total} tweets)...")

    confirmes = 0
    corriges  = 0
    passes    = 0

    label_map = {
        "1": None,
        "2": "negatif",
        "3": "neutre",
        "4": "positif",
        "5": "ambigu",
    }

    for idx, doc in enumerate(tweets):
        afficher_tweet(doc, idx + 1, total)

        while True:
            try:
                choix = input(f"  {BO}→ {RS}").strip().lower()
            except KeyboardInterrupt:
                choix = "q"

            if choix == "q":
                break

            elif choix == "s":
                passes += 1
                print(f"  {DIM}⏭  Passé{RS}")
                break

            elif choix in label_map:
                label_original = doc.get("label", "")

                if choix == "1":
                    sauvegarder(doc, label_original, confirme=True)
                    confirmes += 1
                    print(f"  {G}✅ Confirmé : {label_original}{RS}")
                else:
                    nouveau = label_map[choix]
                    sauvegarder(doc, nouveau, confirme=False)
                    corriges += 1
                    print(f"  {Y}✏️  Corrigé : {label_original} → {nouveau}{RS}")
                break

            else:
                print(f"  {R}⚠  Entrée invalide. Choisis 1/2/3/4/5/s/q{RS}")

        if choix == "q":
            break

    # ── Résumé final ──────────────────────────────────────────
    traites = confirmes + corriges
    os.system('cls' if os.name == 'nt' else 'clear')

    print(f"""
  {'='*55}
  {BO}SESSION TERMINÉE — {ANNOTATEUR}{RS}
  {'='*55}

   {G}✅ Confirmés  : {confirmes:>4}{RS}
   {Y}✏️  Corrigés   : {corriges:>4}{RS}
   {DIM}⏭️  Passés     : {passes:>4}{RS}
   ──────────────────────────
   📊 Traités    : {traites:>4} / {total}

  {G}💾 Tout est sauvegardé dans MongoDB Atlas.{RS}

  {DIM}Pour voir les désaccords entre vous deux :
  Atlas filter → {{"accord_annotateurs": false}}{RS}
  {'='*55}
    """)

    afficher_stats()


if __name__ == "__main__":
    main()