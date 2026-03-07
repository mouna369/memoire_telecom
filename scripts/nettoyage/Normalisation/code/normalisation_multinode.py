#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# normalisation_multinode_v2.py
# ÉTAPE — Normalisation Arabe (adapté code binôme v3.5)
# SOURCE      : commentaires_sans_emojis
# DESTINATION : commentaires_normalises
#
# CE QU'ON FAIT (emojis + nettoyage déjà faits avant) :
#   ✅ Étape 1 : Unicode arabe (tashkeel + tatweel + variantes)
#   ✅ Étape 2 : Expressions mixtes AR/FR regex
#   ✅ Étape 3 : Corrections françaises
#   ✅ Étape 4 : Abréviations + télécom
#   ✅ Étape 5 : Unités
#   ✅ Étape 6 : Arabizi → arabe
#   ✅ Étape 7 : Dedup tokens
#   ✅ Étape 8 : Stopwords (mode full)
#
# CE QU'ON NE FAIT PAS (déjà traité) :
#   ❌ Emojis       → commentaires_sans_emojis
#   ❌ Ponctuations → étape nettoyage
#   ❌ Chiffres     → étape nettoyage
#   ❌ URLs/@       → étape nettoyage
#
# Spark 4.1.1 | mapPartitions | Workers → MongoDB direct

from pymongo import MongoClient
import os, time, math, json

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_sans_emojis"
COLLECTION_DEST   = "commentaires_normalises"
NB_WORKERS        = 3
SPARK_MASTER      = "spark://spark-master:7077"
DICT_PATH         = "/opt/dictionnaires/master_dict.json"
LING_PATH         = "/opt/dictionnaires/linguistic_rules.json"
RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_normalisation_v2.txt"
MODE              = "full"

# ============================================================
# FONCTION WORKER
# ============================================================
def normaliser_partition(partition):
    """
    Chaque Worker :
    1. Charge master_dict.json
    2. Applique le pipeline de normalisation (sans emojis/nettoyage)
    3. Écrit dans MongoDB
    """
    import sys, re, json, logging
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError
    from typing import Dict, List, Set, Tuple, Optional

    # ── Charger les 2 dictionnaires ──────────────────────────
    with open(DICT_PATH, encoding="utf-8") as f:
        d = json.load(f)
    with open(LING_PATH, encoding="utf-8") as f:
        ling = json.load(f)

    # ── Sections du dictionnaire ─────────────────────────────
    unicode_map   = d["unicode_arabic"]
    # FIX-2 : ajouter ة→ه et ئ→ي si absents
    unicode_map.setdefault("\u0629", "\u0647")   # ة → ه
    unicode_map.setdefault("\u0626", "\u064A")   # ئ → ي
    unicode_map.setdefault("\u0624", "\u0648")   # ؤ → و

    digrams       = d["arabizi_digrams"]
    # FIX-1 : garder seulement chiffres arabizi (3,5,6,7,9)
    monograms     = {k: v for k, v in d["arabizi_monograms"].items()
                     if not (len(k) == 1 and k.isalpha())}
    arabizi_words = d["arabizi_words"]
    arabizi_upper = d["arabizi_upper"]
    abbreviations = d["abbreviations"]
    telecom       = d["telecom_tech"]
    units_map     = d["units"]
    nv            = d["network_variants"]
    net_form      = nv["normalized_form"]
    net_all       = nv["latin"] + nv["arabic"]

    # ── Compiler les patterns regex ──────────────────────────
    def compile_dict(dct, flags=re.UNICODE):
        result = []
        for pat, repl in dct.items():
            try:
                result.append((re.compile(pat, flags), repl))
            except re.error:
                pass
        return result

    mixed_pats = compile_dict(d["mixed_ar_fr_regex"])
    fr_pats    = compile_dict(d["french_corrections_regex"],
                               flags=re.IGNORECASE | re.UNICODE)

    escaped = [re.escape(v) for v in net_all if v]
    net_re  = re.compile(rf'\b({"|".join(escaped)})\b',
                          re.IGNORECASE) if escaped else None

    # Arabizi trié longueur décroissante
    combined      = {**digrams, **monograms}
    arabizi_seq   = sorted(combined.items(), key=lambda x: len(x[0]), reverse=True)
    arabizi_upper_sorted = sorted(arabizi_upper.items(),
                                   key=lambda x: len(x[0]), reverse=True)

    # ── Charger règles linguistiques ─────────────────────────
    CONTRACTIONS = ling["contractions"]["francais"]
    ARABIC_PREFIXES = ling["arabic_prefixes"]["prefixes"]

    INTENTIONAL_REPEATS = set(
        ling["intentional_repeats"]["arabe"] +
        ling["intentional_repeats"]["francais"]
    )

    TECH_TOKENS = set(
        ling["tech_tokens"]["protocoles_reseau"] +
        ling["tech_tokens"]["connectivite"] +
        ling["tech_tokens"]["unites"] +
        ling["tech_tokens"]["operateurs"] +
        ling["tech_tokens"]["materiels"]
    )

    NEGATIONS = set(
        ling["negations"]["arabe_standard"] +
        ling["negations"]["dialecte_algerien"]
    )

    DIALECT_KEEP = set(
        ling["dialect_keep"]["questions"] +
        ling["dialect_keep"]["etat_situation"] +
        ling["dialect_keep"]["connecteurs"] +
        ling["dialect_keep"]["pronoms"] +
        ling["dialect_keep"]["prepositions"] +
        ling["dialect_keep"]["francais_garde"]
    )

    stopwords = set(
        ling["stopwords"]["arabe_standard"] +
        ling["stopwords"]["francais"] +
        ling["stopwords"]["anglais"]
    ) - NEGATIONS - DIALECT_KEEP

    # ── Mots protégés ────────────────────────────────────────
    _all_vals = []
    for v in list(telecom.values()) + list(abbreviations.values()):
        if v: _all_vals.extend(v.split())

    _protected_extra = set(
        ling["protected_words"]["termes_fr_gardes"] +
        ling["protected_words"]["reseaux_sociaux"] +
        ling["protected_words"]["jeux_streaming"]
    )

    protected = {t.lower() for t in (
        _all_vals + list(telecom.keys()) + list(abbreviations.keys())
        + list(TECH_TOKENS) + list(_protected_extra)
    )}

    # ── Patterns compilés ────────────────────────────────────
    RE_DIACRITICS  = re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06ED]")
    RE_TATWEEL     = re.compile(r"\u0640+")
    RE_REP_CHARS   = re.compile(r"(.)\1{2,}")
    RE_WHITESPACE  = re.compile(r"\s+")
    RE_DIGITS_ONLY = re.compile(r"^\d+$")
    RE_PURE_LATIN  = re.compile(r"^[a-zA-Z'\u2019\-]+$")
    RE_ARABIZI_HYB = re.compile(r"^(?=.*[a-zA-Z])(?=.*(?<=[a-zA-Z])[35679]|[35679](?=[a-zA-Z])).+$")
    RE_HAS_ARABIC  = re.compile(r"[\u0600-\u06FF]")
    RE_TRAIL_PUNCT = re.compile(r"^(.*[^!.،,;:؟?])((?:[!.،,;:؟?])+)$")
    RE_NUM_ARABIC  = re.compile(r"^(\d+)([\u0600-\u06FF\u0750-\u077F].*)$")
    RE_UNIT_NOSPACE= re.compile(r"(?<!\w)(\d+)([a-zA-Z/]+(?:ps|/s)?)(?=[\u0600-\u06FF\s,،.!?؟$]|$)", re.IGNORECASE)
    RE_UNIT_SPACE  = re.compile(r"\b(\d+)\s+([a-zA-Z/]+(?:ps|/s)?)\b", re.IGNORECASE)
    RE_CONTRACTION = re.compile(
        r"\b(" + "|".join(re.escape(k) for k in CONTRACTIONS.keys()) + r")\b",
        re.IGNORECASE)
    RE_ARAB_DIGIT  = re.compile(r'([\u0600-\u06FF])(\d)')
    RE_DIGIT_ARAB  = re.compile(r'(\d)([\u0600-\u06FF])')
    RE_SPACED_DIGITS = re.compile(r'(?<![:\-\d])(\d)(?: (\d)){1,6}(?![:\-\d])')
    _prefixes_sorted = sorted(ARABIC_PREFIXES, key=len, reverse=True)
    RE_ARABIC_PREFIX = re.compile(
        r'^(' + "|".join(re.escape(p) for p in _prefixes_sorted) + r')(.+)$')

    # EXTRA_AR_PATTERNS supprimés → déjà dans mixed_ar_fr_regex du dictionnaire

    # ── Fonctions internes ───────────────────────────────────
    def lookup(word, dct):
        lo = word.lower()
        v  = dct.get(lo) or dct.get(word)
        if v is not None:
            return v
        for p in ARABIC_PREFIXES:
            if word.startswith(p) and len(word) > len(p) + 1:
                root = word[len(p):]
                v = dct.get(root.lower()) or dct.get(root)
                if v is not None:
                    return v if " " in v else p + v
        return None

    def is_latin_dominant(text):
        lat = sum(1 for c in text if "a" <= c.lower() <= "z")
        ar  = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
        tot = lat + ar
        return (lat / tot) > (0.55 if len(text) < 20 else 0.60) if tot else False

    def arabizi_convert(token):
        for k, v in arabizi_upper_sorted:
            if token.upper() == k:
                return v
        result = token.lower()
        for extra, ar in [("ee","ي"),("ii","ي"),("oo","و"),("pp","ب")]:
            result = result.replace(extra, ar)
        for seq, ar in arabizi_seq:
            result = result.replace(seq, ar)
        result = re.sub(r'[a-z]', '', result)
        return result

    # ── Étape 1 : Unicode arabe ──────────────────────────────
    def step_unicode_arabic(text):
        text = RE_DIACRITICS.sub("", text)
        text = RE_TATWEEL.sub("", text)
        for variant, canonical in unicode_map.items():
            text = text.replace(variant, canonical)
        return text

    # ── Étape 2 : Mixte AR/FR ────────────────────────────────
    def step_mixed(text):
        for pat, repl in mixed_pats:
            text = pat.sub(repl, text)
        return text

    # ── Étape 3 : Corrections françaises ────────────────────
    def step_french(text):
        text = RE_CONTRACTION.sub(
            lambda m: CONTRACTIONS.get(m.group(0).lower(), m.group(0)), text)
        for pat, repl in fr_pats:
            text = pat.sub(repl, text)
        return text

    # ── Étape 4 : Abréviations + télécom ────────────────────
    def step_abbrev(text):
        latin_dom = is_latin_dominant(text)
        tokens, result = text.split(), []
        i = 0
        while i < len(tokens):
            tok  = tokens[i]
            m    = RE_TRAIL_PUNCT.match(tok)
            core = m.group(1) if m else tok
            trail= m.group(2) if m else ""

            if core in ("غير","مش","ميش","ما","لا"):
                result.append(tok); i += 1; continue

            if core.lower() in TECH_TOKENS:
                result.append(tok); i += 1; continue

            mn = RE_NUM_ARABIC.match(core)
            if mn:
                num, unit = mn.groups()
                repl = lookup(unit, telecom) or unit
                if " " in repl:
                    result += [num]+repl.split()[:-1]+[repl.split()[-1]+trail]
                else:
                    result += [num, repl+trail]
                i += 1; continue

            # FIX-5 : télécom en priorité
            resolved = False
            for dct in (telecom, abbreviations):
                repl = lookup(core, dct)
                if repl is not None:
                    if (latin_dom and dct is telecom
                            and not RE_HAS_ARABIC.search(core)
                            and len(core) <= 4):
                        break
                    if " " in repl:
                        parts = repl.split()
                        result += parts[:-1]+[parts[-1]+trail]
                    else:
                        result.append(repl+trail)
                    resolved = True
                    break

            if not resolved:
                if net_re and net_re.fullmatch(core):
                    result.append(net_form+trail)
                else:
                    result.append(tok)
            i += 1
        return " ".join(result)

    # ── Étape 5 : Unités ─────────────────────────────────────
    def step_units(text):
        def _repl(m):
            num, unit = m.group(1), m.group(2).lower()
            if unit in {"ms","h","s"}: return f"{num}{unit}"
            if len(unit) == 1 and unit not in units_map: return m.group(0)
            norm = units_map.get(unit)
            return f"{num} {norm}" if norm else m.group(0)
        text = RE_UNIT_SPACE.sub(_repl, text)
        text = RE_UNIT_NOSPACE.sub(_repl, text)
        return text

    # ── Étape 6 : Arabizi ────────────────────────────────────
    def step_arabizi(text):
        latin_dom = is_latin_dominant(text)
        result    = []
        for tok in text.split():
            lo = tok.lower()
            if RE_HAS_ARABIC.search(tok):
                result.append(tok); continue
            if lo in protected or lo in TECH_TOKENS:
                result.append(tok); continue
            if RE_DIGITS_ONLY.match(tok):
                result.append(tok); continue
            if RE_NUM_ARABIC.match(tok):
                result.append(tok); continue
            if RE_PURE_LATIN.match(tok):
                w = arabizi_words.get(lo)
                if w: result.append(w); continue
                for k, v in arabizi_upper_sorted:
                    if tok.upper() == k:
                        result.append(v); break
                else:
                    if latin_dom: result.append(tok); continue
                    result.append(arabizi_convert(tok))
                continue
            if RE_ARABIZI_HYB.match(tok):
                result.append(arabizi_convert(tok)); continue
            result.append(tok)
        return " ".join(result)

    # ── Étape 7 : Nettoyage léger ────────────────────────────
    def step_cleanup(text):
        text = RE_ARAB_DIGIT.sub(r'\1 \2', text)
        text = RE_DIGIT_ARAB.sub(r'\1 \2', text)
        text = RE_SPACED_DIGITS.sub(lambda m: m.group(0).replace(" ",""), text)
        text = RE_REP_CHARS.sub(lambda m: m.group(1)*2, text)
        return RE_WHITESPACE.sub(" ", text).strip()

    # ── Étape 8 : Dedup tokens ───────────────────────────────
    def step_dedup(text):
        tokens = text.split()
        if len(tokens) < 2:
            return text
        result = [tokens[0]]
        for i in range(1, len(tokens)):
            prev, curr = result[-1], tokens[i]
            prev_lo, curr_lo = prev.lower(), curr.lower()
            if curr_lo == prev_lo:
                if curr_lo in INTENTIONAL_REPEATS:
                    result.append(curr)
                continue
            has_ar_curr = RE_HAS_ARABIC.search(curr)
            has_ar_prev = RE_HAS_ARABIC.search(prev)
            if has_ar_prev and has_ar_curr:
                m = RE_ARABIC_PREFIX.match(prev)
                if m and m.group(2) == curr:
                    result.append(curr); continue
            if not has_ar_curr and len(curr) >= 3 and len(curr) < len(prev):
                if prev_lo.endswith(curr_lo): continue
            if has_ar_curr and len(curr) >= 4 and len(curr) < len(prev):
                if prev.endswith(curr):
                    m = RE_ARABIC_PREFIX.match(prev)
                    if not m: continue
            result.append(curr)
        return " ".join(result)

    # ── Étape 9 : Stopwords ──────────────────────────────────
    def step_stopwords(text):
        keep = NEGATIONS | DIALECT_KEEP
        result = []
        for w in text.split():
            lo = w.lower()
            if (w.isdigit() or w in keep or lo in keep
                    or lo not in stopwords):
                result.append(w)
            elif len(w) == 1 and RE_HAS_ARABIC.match(w):
                result.append(w)
        return " ".join(result)

    # ── Pipeline complet ─────────────────────────────────────
    def normalize(text):
        if not isinstance(text, str) or not text.strip():
            return ""
        try:
            text = step_unicode_arabic(text)   # Étape 1
            text = step_mixed(text)            # Étape 2
            text = step_french(text)           # Étape 3
            text = step_abbrev(text)           # Étape 4
            text = step_units(text)            # Étape 5
            text = step_arabizi(text)          # Étape 6
            text = step_cleanup(text)          # Étape 7
            text = step_dedup(text)            # Étape 8
            text = step_stopwords(text)        # Étape 9
        except Exception:
            pass
        return text.strip()

    # ── Connexion MongoDB Worker ─────────────────────────────
    try:
        client     = MongoClient("mongodb://mongodb_pfe:27017/",
                                 serverSelectionTimeoutMS=5000)
        db         = client["telecom_algerie"]
        collection = db["commentaires_normalises"]
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed"}
        return

    # ── Traitement + écriture ────────────────────────────────
    batch         = []
    docs_traites  = 0
    docs_modifies = 0

    for row in partition:
        commentaire_original  = row.get("Commentaire_Client", "") or ""
        commentaire_normalise = normalize(commentaire_original)

        if commentaire_normalise != commentaire_original:
            docs_modifies += 1

        doc = {
            "_id"                   : row.get("_id"),
            "Commentaire_Client"    : commentaire_normalise,
            "emojis_originaux"      : row.get("emojis_originaux", []),
            "emojis_sentiment"      : row.get("emojis_sentiment", []),
            "commentaire_moderateur": row.get("commentaire_moderateur"),
            "date"                  : row.get("date"),
            "source"                : row.get("source"),
            "moderateur"            : row.get("moderateur"),
            "metadata"              : row.get("metadata"),
            "statut"                : row.get("statut"),
        }
        batch.append(InsertOne(doc))
        docs_traites += 1

        if len(batch) >= 1000:
            try:
                collection.bulk_write(batch, ordered=False)
            except BulkWriteError:
                pass
            batch = []

    if batch:
        try:
            collection.bulk_write(batch, ordered=False)
        except BulkWriteError:
            pass

    client.close()
    yield {
        "docs_traites" : docs_traites,
        "docs_modifies": docs_modifies,
        "statut"       : "ok"
    }

# ============================================================
# LECTURE DISTRIBUÉE
# ============================================================
def lire_partition_depuis_mongo(partition_info):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client     = MongoClient("mongodb://mongodb_pfe:27017/",
                                 serverSelectionTimeoutMS=5000)
        db         = client["telecom_algerie"]
        collection = db["commentaires_sans_emojis"]

        curseur = collection.find(
            {},
            {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
             "emojis_originaux": 1, "emojis_sentiment": 1,
             "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
        ).skip(item["skip"]).limit(item["limit"])

        for doc in curseur:
            doc["_id"] = str(doc["_id"])
            yield doc

        client.close()

# ============================================================
# PIPELINE SPARK
# ============================================================
from pyspark.sql import SparkSession
from datetime import datetime

temps_debut = time.time()

print("="*70)
print("✨ NORMALISATION ARABE v2 — Multi-Node Spark")
print(f"   Source      : {COLLECTION_SOURCE}")
print(f"   Destination : {COLLECTION_DEST}")
print("   Dictionnaires :")
print(f"   📖 master_dict.json     → données télécom")
print(f"   📖 linguistic_rules.json → règles linguistiques")
print("   Pipeline (emojis + nettoyage déjà faits) :")
print("   ✅ Étape 1 : Unicode arabe (tashkeel + tatweel + variantes)")
print("   ✅ Étape 2 : Expressions mixtes AR/FR")
print("   ✅ Étape 3 : Corrections françaises")
print("   ✅ Étape 4 : Abréviations + télécom")
print("   ✅ Étape 5 : Unités (mbps/Go/DA...)")
print("   ✅ Étape 6 : Arabizi → arabe")
print("   ✅ Étape 7 : Nettoyage léger")
print("   ✅ Étape 8 : Dedup tokens")
print("   ✅ Étape 9 : Stopwords (mode full)")
print("   Spark 4.1.1 | mapPartitions | 3 Workers → MongoDB direct")
print("="*70)

# 1. Connexion MongoDB Driver
print("\n📂 Connexion MongoDB (Driver)...")
client_driver = MongoClient(MONGO_URI_DRIVER)
db_driver     = client_driver[DB_NAME]
coll_source   = db_driver[COLLECTION_SOURCE]
total_docs    = coll_source.count_documents({})
print(f"✅ {total_docs} documents dans la source")

# 2. Connexion Spark
print("\n⚡ Connexion au cluster Spark...")
temps_spark = time.time()

spark = SparkSession.builder \
    .appName("Normalisation_Arabe_v2_MultiNode") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"✅ Spark connecté en {time.time()-temps_spark:.2f}s")

# 3. Lecture distribuée
print("\n📥 LECTURE DISTRIBUÉE...")
docs_par_worker = math.ceil(total_docs / NB_WORKERS)
plages = [
    {"skip" : i * docs_par_worker,
     "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
    for i in range(NB_WORKERS)
]
for idx, p in enumerate(plages):
    print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

rdd_data = spark.sparkContext \
    .parallelize(plages, NB_WORKERS) \
    .mapPartitions(lire_partition_depuis_mongo)

df_spark     = spark.read.json(rdd_data.map(
    lambda d: json.dumps(
        {k: str(v) if not isinstance(v, (str, int, float, bool, type(None), list)) else v
         for k, v in d.items()}
    )
))
total_lignes = df_spark.count()
print(f"✅ {total_lignes} documents chargés")

# 4. Vider destination
coll_dest = db_driver[COLLECTION_DEST]
coll_dest.delete_many({})
print("\n🧹 Collection destination vidée")

# 5. Normalisation + écriture distribuée
print("\n💾 NORMALISATION + ÉCRITURE DISTRIBUÉE...")
temps_traitement = time.time()

rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(normaliser_partition)

stats          = rdd_stats.collect()
total_inseres  = sum(s.get("docs_traites",  0) for s in stats if s.get("statut") == "ok")
total_modifies = sum(s.get("docs_modifies", 0) for s in stats if s.get("statut") == "ok")
erreurs        = [s for s in stats if "_erreur" in s]

print(f"✅ Traitement terminé en {time.time()-temps_traitement:.2f}s")
if erreurs:
    for e in erreurs:
        print(f"   ⚠️  {e.get('_erreur')}")

# 6. Vérification finale
print("\n🔎 VÉRIFICATION FINALE...")
total_en_dest = coll_dest.count_documents({})
tashkeel_rest = coll_dest.count_documents(
    {"Commentaire_Client": {"$regex": "[\u064B-\u065F\u0670]"}})
tatweel_rest  = coll_dest.count_documents(
    {"Commentaire_Client": {"$regex": "\u0640"}})
succes        = tashkeel_rest == 0 and tatweel_rest == 0

print(f"   ┌──────────────────────────────────────────────────┐")
print(f"   │ Documents insérés        : {total_en_dest:<20} │")
print(f"   │ Documents modifiés       : {total_modifies:<20} │")
print(f"   │ Tashkeel restants        : {tashkeel_rest:<20} │")
print(f"   │ Tatweel restants         : {tatweel_rest:<20} │")
print(f"   │ Statut : {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier':<38} │")
print(f"   └──────────────────────────────────────────────────┘")

# 7. Rapport
temps_total = time.time() - temps_debut
rapport = f"""
{"="*70}
RAPPORT — NORMALISATION ARABE v2 (Multi-Node Spark)
{"="*70}
Date        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode        : full | Spark 4.1.1 | {NB_WORKERS} Workers
Collection  : {DB_NAME}.{COLLECTION_SOURCE} → {COLLECTION_DEST}
Dict        : {DICT_PATH}

PIPELINE (emojis + nettoyage déjà faits) :
   1. Unicode arabe   → tashkeel + tatweel + variantes
   2. Mixte AR/FR     → regex (كونيكسيون → الانترنت)
   3. Corrections FR  → orthographe (probleme → problème)
   4. Abréviations    → télécom (cnx → connexion)
   5. Unités          → normalisées (20mbps → 20 Mbps)
   6. Arabizi         → arabe (bzaf → بزاف)
   7. Nettoyage léger → répétitions + espaces
   8. Dedup tokens    → doublons consécutifs
   9. Stopwords       → mode full (négations préservées)

RÉSULTATS :
   • Total source          : {total_lignes}
   • Documents modifiés    : {total_modifies}
   • Total inséré          : {total_inseres}
   • Tashkeel restants     : {tashkeel_rest}
   • Tatweel restants      : {tatweel_rest}
   • Statut                : {"✅ SUCCÈS" if succes else "⚠️ VÉRIFIER"}

TEMPS :
   • Total                 : {temps_total:.2f}s
   • Vitesse               : {total_lignes/temps_total:.0f} docs/s

STOCKAGE :
   • Source      : {DB_NAME}.{COLLECTION_SOURCE}
   • Destination : {DB_NAME}.{COLLECTION_DEST}
{"="*70}
"""

os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
    f.write(rapport)

spark.stop()
client_driver.close()

print(f"\n✅ Rapport : {RAPPORT_PATH}")
print("\n" + "="*70)
print("📊 RÉSUMÉ FINAL")
print("="*70)
print(f"   📥 Documents source     : {total_lignes}")
print(f"   ✏️  Documents modifiés   : {total_modifies}")
print(f"   📤 Documents insérés    : {total_inseres}")
print(f"   ⏱️  Temps total          : {temps_total:.2f}s")
print(f"   🚀 Vitesse              : {total_lignes/temps_total:.0f} docs/s")
print(f"   📁 Destination          : {DB_NAME}.{COLLECTION_DEST}")
print("="*70)
print("🎉 NORMALISATION v2 TERMINÉE EN MODE MULTI-NODE !")