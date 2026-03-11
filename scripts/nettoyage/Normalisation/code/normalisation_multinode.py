#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# normalisation_multinode_v3.py
# ÉTAPE — Normalisation Arabe v3.8.2 (code binôme intégré)
# SOURCE      : commentaires_sans_emojis
# DESTINATION : commentaires_normalises
#
# NOUVEAUTÉS v3 vs v2 :
#   ✅ FIX-1  : Protection chiffres séparateurs (15:00, 8000 DA)
#   ✅ FIX-2  : Chiffres tronqués (8000→800 corrigé)
#   ✅ FIX-3  : جدا جدا conservé
#   ✅ FIX-4  : Faux doublons suffixes arabes
#   ✅ FIX-5  : Arabizi avant latin_dom (slm/mkch)
#   ✅ FIX-6  : Texte latin dominant protégé
#   ✅ FIX-7  : ئ→ي supprimé (سيئة conservé)
#   ✅ FIX-8  à FIX-26 : dictionnaire enrichi
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
from datetime import datetime

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
RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_normalisation_v3.txt"
MODE              = "full"   # "full" ou "arabert"

# ============================================================
# FONCTION WORKER — TextNormalizer v3.8.2 intégré
# ============================================================
def normaliser_partition(partition):
    """
    Chaque Worker :
    1. Charge master_dict.json + linguistic_rules.json
    2. Initialise TextNormalizer v3.8.2
    3. Normalise chaque commentaire
    4. Écrit dans MongoDB
    """
    import sys, re, json, logging
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError
    from typing import Dict, List, Optional, Set, Tuple
    from pathlib import Path

    # ── Patterns regex ────────────────────────────────────────
    _RE = {
        "diacritics":    re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06ED]"),
        "tatweel":       re.compile(r"\u0640+"),
        # FIX-2 : exclure chiffres de rep_chars
        "rep_chars":     re.compile(r"(?<!\d)(.)\1{2,}(?!\d)"),
        "rep_punct":     re.compile(r"([!?.،:;])\1+"),
        "whitespace":    re.compile(r"\s+"),
        "digits_only":   re.compile(r"^\d+$"),
        "pure_latin":    re.compile(r"^[a-zA-Z'\u2019\-]+$"),
        "arabizi_hyb":   re.compile(r"^(?=.*[a-zA-Z])(?=.*(?<=[a-zA-Z])[35679]|[35679](?=[a-zA-Z])).+$"),
        "has_arabic":    re.compile(r"[\u0600-\u06FF]"),
        "trail_punct":   re.compile(r"^(.*[^!.،,;:؟?])((?:[!.،,;:؟?])+)$"),
        "num_arabic":    re.compile(r"^(\d+)([\u0600-\u06FF\u0750-\u077F].*)$"),
        "unit_nospace":  re.compile(r"(?<!\w)(\d+)([a-zA-Z/]+(?:ps|/s)?)(?=[\u0600-\u06FF\s,،.!?؟$]|$)", re.IGNORECASE),
        "unit_space":    re.compile(r"\b(\d+)\s+([a-zA-Z/]+(?:ps|/s)?)\b", re.IGNORECASE),
        "arab_digit":    re.compile(r"([\u0600-\u06FF])(\d)"),
        "digit_arab":    re.compile(r"(\d)([\u0600-\u06FF])"),
        "spaced_digits": re.compile(r"(?<![:\-\d])(\d)(?: (\d)){1,6}(?![:\-\d])"),
        "arabic_digits_spaced": re.compile(r"(?<![٠-٩])([٠-٩])(?: ([٠-٩])){1,6}(?![٠-٩])"),
        "arabic_prefix": re.compile(r"^(وال|فال|بال|كال|لل|ال|و|ف|ب|ك|ل)(.+)$"),
        # FIX-1 : protection séquences numériques
        "num_separator": re.compile(r"(?<!\d)(\d+)([-:/])(\d+)(?!\d)"),
    }

    _AR_VERB_PREFIXES = {"ت","ي","ن","أ","ا","تت","يت","نت","ست"}
    _AR_CONJ_PREFIXES = {"و","ف","ب","ك","ل","ال","لل","بال","كال","فال","وال"}
    _AR_ALL_PREFIXES  = _AR_VERB_PREFIXES | _AR_CONJ_PREFIXES

    # ── Extra arabizi ─────────────────────────────────────────
    _EXTRA_ARABIZI_WORDS = {
        "nachalh":"إن شاء الله","nchalh":"إن شاء الله","inchalh":"إن شاء الله",
        "inshalah":"إن شاء الله","inshalh":"إن شاء الله","inshaallah":"إن شاء الله",
        "inchalah":"إن شاء الله","inshaalah":"إن شاء الله","inchallah":"إن شاء الله",
        "inshallah":"إن شاء الله","nchallah":"إن شاء الله","nshallah":"إن شاء الله",
        "wlh":"والله","wlhi":"والله","wellah":"والله","wella":"والله",
        "wallah":"والله","wallahi":"والله","wallhy":"والله",
        "flexi":"فليكسي","flexili":"فليكسي","nflexi":"فليكسي","yflexi":"فليكسي",
    }
    _EXTRA_ARABIZI_UPPER = {
        "NACHALH":"إن شاء الله","NCHALH":"إن شاء الله","INCHALH":"إن شاء الله",
        "INSHALAH":"إن شاء الله","INSHALLAH":"إن شاء الله","INCHALAH":"إن شاء الله",
        "INCHALLAH":"إن شاء الله","WLH":"والله","WELLAH":"والله",
        "WALLAH":"والله","WALLAHI":"والله",
    }
    _EXTRA_AR_PATTERNS = [
        (re.compile(r"\bانشاء الله\b"),  "إن شاء الله"),
        (re.compile(r"\bنشاالله\b"),     "إن شاء الله"),
        (re.compile(r"\bانشالله\b"),     "إن شاء الله"),
        (re.compile(r"\bنشالله\b"),      "إن شاء الله"),
        (re.compile(r"\bانشاالله\b"),    "إن شاء الله"),
        (re.compile(r"\bاشالله\b"),      "إن شاء الله"),
        (re.compile(r"\bفكيكسيت\b"),     "فليكسي"),
        (re.compile(r"\bفلكسيلي\b"),     "فليكسي"),
        (re.compile(r"\bفليكسيلي\b"),    "فليكسي"),
        (re.compile(r"\bفليكسيت\b"),     "فليكسي"),
        (re.compile(r"\bنفليكسي\b"),     "فليكسي"),
        (re.compile(r"\bيفليكسي\b"),     "فليكسي"),
        (re.compile(r"\bتفليكسي\b"),     "فليكسي"),
    ]

    # ── Charger les 2 dictionnaires ───────────────────────────
    with open(DICT_PATH, encoding="utf-8") as f:
        d = json.load(f)
    with open(LING_PATH, encoding="utf-8") as f:
        ling = json.load(f)

    # ── Règles linguistiques ──────────────────────────────────
    neg = ling.get("negations", {})
    NEGATIONS = set(neg.get("arabe_standard", [])) | set(neg.get("dialecte_algerien", []))
    NEGATIONS.update({"ما","لا","لم","لن","ليس","غير","مش","ميش","مكانش","ماكش","ماهيش","ماهوش"})

    dk = ling.get("dialect_keep", {})
    DIALECT_KEEP = set()
    for v in dk.values():
        if isinstance(v, list): DIALECT_KEEP.update(v)
    DIALECT_KEEP.update({
        "راه","راهي","راني","راك","راهم","واش","كيفاش","وين","علاش","قديش",
        "مزال","كيما","باه","ديما",
        "ya","pas","de","en","avec","depuis","pour","par","sur","sous",
        "dans","entre","sans","vers",
    })

    ir = ling.get("intentional_repeats", {})
    INTENTIONAL_REPEATS = set()
    for v in ir.values():
        if isinstance(v, list): INTENTIONAL_REPEATS.update(v)
    # FIX-3 : جدا ajouté
    INTENTIONAL_REPEATS.update({
        "كيف","شوي","برا","هاك","يا","آه","لا","واو","بزاف","جدا","جد",
        "très","trop","bien","non","oui","si","jamais","encore","mm",
    })

    tt = ling.get("tech_tokens", {})
    TECH_TOKENS = set()
    for v in tt.values():
        if isinstance(v, list): TECH_TOKENS.update(v)
    TECH_TOKENS.update({
        "adsl","vdsl","ftth","fttb","xgs","xgs-pon","pon","dsl","lte","volte",
        "dns","ont","wifi","4g","3g","2g","5g","mbps","kbps","gbps","mbs",
        "idoom","djezzy","mobilis","ooredoo","fibre","fiber","febre","ping",
        "4k","hd","ram","android","ios","serveur","fortnite","netflix",
    })
    TECH_TOKENS.discard("cnx")

    ARABIC_PREFIXES = ling.get("arabic_prefixes", {}).get("prefixes", [
        "وال","فال","بال","كال","لل","ال","و","ف","ب","ك","ل"
    ])
    ARABIC_PREFIXES = sorted(ARABIC_PREFIXES, key=len, reverse=True)

    ct = ling.get("contractions", {})
    CONTRACTIONS = dict(ct.get("francais", {}))
    CONTRACTIONS.update({
        "j'ai":"je ai","j\u2019ai":"je ai",
        "c'est":"ce est","c\u2019est":"ce est",
        "n'est":"ne est","n\u2019est":"ne est",
        "n'a":"ne a","n\u2019a":"ne a",
        "qu'il":"que il","qu\u2019il":"que il",
        "qu'on":"que on","qu\u2019on":"que on",
        "l'internet":"le internet","d'internet":"de internet",
        "s'il":"si il",
    })

    pw = ling.get("protected_words", {})
    PROTECTED = set()
    for v in pw.values():
        if isinstance(v, list): PROTECTED.update(t.lower() for t in v)
    PROTECTED.update({
        "internet","connexion","problème","réseau","service","optique",
        "ping","gaming","game","live","speed","high","low","lag","stream",
        "facebook","whatsapp","youtube","instagram","bonjour","merci","salut",
        "normal","bravo","message","solution","compte","temps","même","comme",
        "chaque","alors","avant","depuis","juste","vraiment","lente","mois",
        "plusieurs","pas","on","ne","fait","rien","tout","fois","bien","moi",
        "encore","niveau","il","ils","de","me","le","la","les","du","une","un",
        "qui","que","dans","sur","pour","par","avec","sans","sont","est",
        "etait","avait","peut","font","avez","avons",
        "prime","netflix","amazon","fortnite","twinbox","ram","heures",
        "bonus","nokia","tenda","google","starlink",
    })

    sw_section = ling.get("stopwords", {})
    stopwords = set()
    for v in sw_section.values():
        if isinstance(v, list): stopwords.update(v)
    # FIX-24 : retrait de certains mots anglais utiles
    stopwords -= {"we","get","want","good","bad","very","all","so"}
    stopwords = stopwords - NEGATIONS - DIALECT_KEEP

    # ── Sections du dictionnaire ──────────────────────────────
    unicode_map = d["unicode_arabic"]
    unicode_map.setdefault("\u0629", "\u0647")   # ة → ه
    # FIX-7 : NE PAS ajouter ئ → ي (causait سيئة→سييه)
    unicode_map.setdefault("\u0624", "\u0648")   # ؤ → و

    digrams   = d["arabizi_digrams"]
    monograms = {k: v for k, v in d["arabizi_monograms"].items()
                 if not (len(k) == 1 and k.isalpha())}

    arabizi_words = {**d["arabizi_words"], **_EXTRA_ARABIZI_WORDS}
    arabizi_words = {k: v for k, v in arabizi_words.items() if not k.startswith("_")}
    arabizi_upper = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}

    abbreviations = d["abbreviations"]
    telecom       = d["telecom_tech"]
    units_map     = d["units"]

    nv       = d["network_variants"]
    net_form = nv["normalized_form"]
    net_all  = nv["latin"] + nv["arabic"]

    mixed_clean = {k: v for k, v in d["mixed_ar_fr_regex"].items()
                   if not k.startswith("_")}

    def compile_dict(dct, flags=re.UNICODE):
        result = []
        for pat, repl in dct.items():
            try: result.append((re.compile(pat, flags), repl))
            except: pass
        return result

    mixed_pats = compile_dict(mixed_clean)
    fr_pats    = compile_dict(d["french_corrections_regex"], flags=re.IGNORECASE|re.UNICODE)

    escaped = [re.escape(v) for v in net_all if v]
    net_re  = re.compile(rf'\b({"|".join(escaped)})\b', re.IGNORECASE) if escaped else None

    combined       = {**digrams, **monograms}
    arabizi_seq    = sorted(combined.items(), key=lambda x: len(x[0]), reverse=True)
    arabizi_upper_sorted = sorted(arabizi_upper.items(), key=lambda x: len(x[0]), reverse=True)

    _all_vals = []
    for v in list(telecom.values()) + list(abbreviations.values()):
        if v: _all_vals.extend(v.split())
    PROTECTED.update(t.lower() for t in (
        _all_vals + list(telecom.keys()) + list(abbreviations.keys()) + list(TECH_TOKENS)
    ))

    RE_CONTRACTION = re.compile(
        r"\b(" + "|".join(sorted([re.escape(k) for k in CONTRACTIONS if "'" in k or "\u2019" in k],
                                  key=len, reverse=True)) + r")\b", re.IGNORECASE)

    # ── FIX-1 : Protection chiffres ───────────────────────────
    def protect_numbers(text):
        protected = {}
        counter   = [0]
        def _p(m):
            key = f"__NP{counter[0]}__"
            counter[0] += 1
            protected[key] = m.group(0)
            return key
        text = _RE["num_separator"].sub(_p, text)
        text = re.sub(r"(?<!\d)(\d+)\s*/\s*(\d+)(?!\d)", _p, text)
        return protected, text

    # ── Helpers ───────────────────────────────────────────────
    def split_prefix(word):
        for p in ARABIC_PREFIXES:
            if word.startswith(p) and len(word) > len(p) + 1:
                return p, word[len(p):]
        return "", word

    def lookup(word, dct):
        lo = word.lower()
        v  = dct.get(lo) or dct.get(word)
        if v is not None: return v
        pref, root = split_prefix(word)
        if pref:
            v = dct.get(root.lower()) or dct.get(root)
            if v is not None:
                return v if " " in v else pref + v
        return None

    def is_latin_dominant(text):
        lat = sum(1 for c in text if "a" <= c.lower() <= "z")
        ar  = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
        tot = lat + ar
        return (lat / tot) > (0.55 if len(text) < 20 else 0.60) if tot else False

    # ── Étapes ────────────────────────────────────────────────
    def step_unicode_arabic(text):
        text = _RE["diacritics"].sub("", text)
        text = _RE["tatweel"].sub("", text)
        for variant, canonical in unicode_map.items():
            text = text.replace(variant, canonical)
        return text

    def step_mixed(text):
        for pat, repl in _EXTRA_AR_PATTERNS:
            text = pat.sub(repl, text)
        for pat, repl in mixed_pats:
            text = pat.sub(repl, text)
        text = _RE["tatweel"].sub("", text)  # tatweel résiduel
        return text

    def step_french(text):
        text = RE_CONTRACTION.sub(
            lambda m: CONTRACTIONS.get(m.group(0).lower(), m.group(0)), text)
        for pat, repl in fr_pats:
            text = pat.sub(repl, text)
        return text

    def step_units(text):
        def _repl(m):
            num, unit = m.group(1), m.group(2).lower()
            KEEP_ATTACHED = {"ms","h","s"}
            if unit in KEEP_ATTACHED: return f"{num}{unit}"
            if len(unit) == 1 and unit not in units_map: return m.group(0)
            norm = units_map.get(unit)
            if norm is None: return m.group(0)
            return f"{num} {norm}"
        text = _RE["unit_space"].sub(_repl, text)
        text = _RE["unit_nospace"].sub(_repl, text)
        return text

    def step_abbrev(text):
        latin_dom = is_latin_dominant(text)
        tokens, result = text.split(), []
        i = 0
        while i < len(tokens):
            tok  = tokens[i]
            m    = _RE["trail_punct"].match(tok)
            core, trail = (m.group(1), m.group(2)) if m else (tok, "")

            if core in NEGATIONS:
                result.append(tok); i += 1; continue
            if core.lower() in TECH_TOKENS:
                result.append(tok); i += 1; continue

            mn = _RE["num_arabic"].match(core)
            if mn:
                num, unit = mn.groups()
                repl = lookup(unit, telecom) or unit
                if " " in repl:
                    result += [num] + repl.split()[:-1] + [repl.split()[-1] + trail]
                else:
                    result += [num, repl + trail]
                i += 1; continue

            resolved = False
            for dct in (telecom, abbreviations):
                repl = lookup(core, dct)
                if repl is not None:
                    if (latin_dom and dct is telecom
                            and not _RE["has_arabic"].search(core)
                            and len(core) <= 4):
                        break
                    if " " in repl:
                        parts = repl.split()
                        result += parts[:-1] + [parts[-1] + trail]
                    else:
                        result.append(repl + trail)
                    resolved = True; break

            if not resolved:
                if net_re and net_re.fullmatch(core):
                    result.append(net_form + trail)
                else:
                    result.append(tok)
            i += 1
        return " ".join(result)

    def arabizi_convert(token):
        for k, v in arabizi_upper_sorted:
            if token.upper() == k: return v
        result = token.lower()
        for extra, ar in [("ee","ي"),("ii","ي"),("oo","و"),("pp","ب")]:
            result = result.replace(extra, ar)
        for seq, ar in arabizi_seq:
            result = result.replace(seq, ar)
        result = re.sub(r"[a-z]", "", result)
        return result

    def step_arabizi(text):
        latin_dom = is_latin_dominant(text)
        result = []
        for tok in text.split():
            lo = tok.lower()
            if lo in TECH_TOKENS or lo in PROTECTED:
                result.append(tok); continue
            if _RE["has_arabic"].search(tok):
                result.append(tok); continue
            if _RE["digits_only"].match(tok):
                result.append(tok); continue
            if _RE["num_arabic"].match(tok):
                result.append(tok); continue

            if _RE["pure_latin"].match(tok):
                # FIX-5 : arabizi_words EN PREMIER
                w = arabizi_words.get(lo)
                if w:
                    # FIX-BUG-E : tokens courts en latin-dominant conservés
                    if latin_dom and len(lo) <= 3 and not any(c.isdigit() for c in lo):
                        result.append(tok)
                    else:
                        result.append(w)
                    continue
                for k, v in arabizi_upper_sorted:
                    if tok.upper() == k:
                        result.append(v); break
                else:
                    if latin_dom:
                        result.append(tok); continue
                    result.append(arabizi_convert(tok))
                continue

            # FIX-6 : hybrides dans texte latin-dominant
            if _RE["arabizi_hyb"].match(tok):
                w = arabizi_words.get(lo)
                if w: result.append(w); continue
                for k, v in arabizi_upper_sorted:
                    if tok.upper() == k:
                        result.append(v); break
                else:
                    if latin_dom:
                        result.append(tok)
                    else:
                        result.append(arabizi_convert(tok))
                continue

            result.append(tok)
        return " ".join(result)

    def step_cleanup(text):
        text = _RE["arab_digit"].sub(r"\1 \2", text)
        text = _RE["digit_arab"].sub(r"\1 \2", text)
        def _fuse(m): return m.group(0).replace(" ", "")
        text = _RE["spaced_digits"].sub(_fuse, text)
        text = _RE["arabic_digits_spaced"].sub(_fuse, text)
        # FIX-2 : rep_chars exclut chiffres
        text = _RE["rep_chars"].sub(lambda m: m.group(1) * 2, text)
        text = _RE["rep_punct"].sub(lambda m: m.group(1), text)
        text = _RE["whitespace"].sub(" ", text).strip()
        return text

    # FIX-4 : dedup avec guard suffixe
    def dedup_tokens(text):
        tokens = text.split()
        if len(tokens) < 2: return text
        result = [tokens[0]]
        for i in range(1, len(tokens)):
            prev, curr = result[-1], tokens[i]
            prev_lo, curr_lo = prev.lower(), curr.lower()

            if curr_lo == prev_lo:
                if curr_lo in INTENTIONAL_REPEATS:
                    result.append(curr)
                continue

            has_ar_curr = bool(_RE["has_arabic"].search(curr))

            if (not has_ar_curr and len(curr) >= 4
                    and len(curr) < len(prev) and prev_lo.endswith(curr_lo)):
                continue

            # FIX-4 : guard len(stripped) >= 2
            if (has_ar_curr and len(curr) >= 5
                    and len(curr) < len(prev) and prev.endswith(curr)):
                m = _RE["arabic_prefix"].match(prev)
                if m and m.group(2) == curr:
                    result.append(curr); continue
                stripped = prev[:len(prev) - len(curr)]
                if stripped in _AR_ALL_PREFIXES and len(stripped) >= 2:
                    result.append(curr); continue
                result.append(curr); continue

            result.append(curr)
        return " ".join(result)

    def step_stopwords(text):
        if not stopwords: return text
        keep = NEGATIONS | DIALECT_KEEP
        result = []
        for w in text.split():
            lo = w.lower()
            if (w.isdigit() or w in keep or lo in keep
                    or lo not in stopwords):
                result.append(w)
            elif len(w) == 1 and _RE["has_arabic"].match(w):
                result.append(w)
        return " ".join(result)

    # ── Normalisation complète ────────────────────────────────
    def normalize(text):
        if not isinstance(text, str) or not text.strip():
            return ""
        try:
            # FIX-1 : protection immédiate
            protected_nums, text = protect_numbers(text)

            text = step_unicode_arabic(text)
            text = step_mixed(text)
            text = step_french(text)
            text = step_abbrev(text)
            text = step_units(text)
            text = step_arabizi(text)
            text = step_cleanup(text)
            text = dedup_tokens(text)

            if MODE == "full":
                text = step_stopwords(text)

            # FIX-1 : restauration finale
            for key, val in protected_nums.items():
                text = text.replace(key, val)

        except Exception as e:
            pass
        return text.strip()

    # ── Connexion MongoDB Worker ──────────────────────────────
    MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
    try:
        client     = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
        db         = client[DB_NAME]
        collection = db[COLLECTION_DEST]
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed"}
        return

    # ── Traitement de la partition ────────────────────────────
    batch         = []
    docs_traites  = 0
    docs_modifies = 0
    tashkeel_rest = 0
    tatweel_rest  = 0

    RE_TASHKEEL = re.compile(r"[\u064B-\u065F\u0670]")
    RE_TATWEEL  = re.compile(r"\u0640")

    for row in partition:
        doc = dict(row) if hasattr(row, 'asDict') else row
        texte_original = doc.get("Commentaire_Client", "") or ""
        texte_normalise = normalize(texte_original)

        if texte_normalise != texte_original:
            docs_modifies += 1

        if RE_TASHKEEL.search(texte_normalise):
            tashkeel_rest += 1
        if RE_TATWEEL.search(texte_normalise):
            tatweel_rest += 1

        doc_out = {
            k: v for k, v in doc.items()
            if k != "Commentaire_Client"
        }
        doc_out["Commentaire_Client"] = texte_normalise
        doc_out["emojis_originaux"]   = doc.get("emojis_originaux", [])
        doc_out["emojis_sentiment"]   = doc.get("emojis_sentiment", [])

        batch.append(InsertOne(doc_out))
        docs_traites += 1

        if len(batch) >= 500:
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
        "tashkeel_rest": tashkeel_rest,
        "tatweel_rest" : tatweel_rest,
        "statut"       : "ok"
    }


# ============================================================
# PIPELINE SPARK
# ============================================================
from pyspark.sql import SparkSession
import time, math, json
from datetime import datetime

temps_debut = time.time()

print("=" * 70)
print("✨ NORMALISATION ARABE v3.8.2 — Multi-Node Spark")
print(f"   Source      : {COLLECTION_SOURCE}")
print(f"   Destination : {COLLECTION_DEST}")
print(f"   Mode        : {MODE}")
print("   Dictionnaires :")
print(f"   📖 master_dict.json      → données télécom")
print(f"   📖 linguistic_rules.json → règles linguistiques")
print("   Nouveautés v3 :")
print("   ✅ FIX-1 : Protection chiffres (15:00 / 8000 DA)")
print("   ✅ FIX-2 : Chiffres tronqués corrigés")
print("   ✅ FIX-3 : جدا جدا conservé")
print("   ✅ FIX-4 : Faux doublons suffixes arabes")
print("   ✅ FIX-5 : Arabizi avant latin_dom")
print("   ✅ FIX-6 : Texte latin dominant protégé")
print("   ✅ FIX-7 : ئ→ي supprimé (سيئة conservé)")
print("   ✅ FIX-8 à FIX-26 : dictionnaire enrichi")
print("   Spark 4.1.1 | mapPartitions | 3 Workers → MongoDB direct")
print("=" * 70)

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
    .appName("Normalisation_v3_MultiNode") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "6") \
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

def lire_partition(partition_info):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient
    for item in partition_info:
        client = MongoClient("mongodb://mongodb_pfe:27017/",
                             serverSelectionTimeoutMS=5000)
        curseur = client[DB_NAME][COLLECTION_SOURCE].find({}).skip(item["skip"]).limit(item["limit"])
        for doc in curseur:
            doc["_id"] = str(doc["_id"])
            yield doc
        client.close()

rdd_data = spark.sparkContext \
    .parallelize(plages, NB_WORKERS) \
    .mapPartitions(lire_partition)

df_spark     = spark.read.json(rdd_data.map(
    lambda d: json.dumps(
        {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
         for k, v in d.items()}
    )
))
total_lignes = df_spark.count()
print(f"✅ {total_lignes} documents chargés")

# 4. Vider destination
coll_dest = db_driver[COLLECTION_DEST]
coll_dest.delete_many({})
print("\n🧹 Collection destination vidée")

# 5. Normalisation + écriture
print("\n💾 NORMALISATION + ÉCRITURE DISTRIBUÉE...")
temps_traitement = time.time()

rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(normaliser_partition)

stats          = rdd_stats.collect()
total_inseres  = sum(s.get("docs_traites",  0) for s in stats if s.get("statut") == "ok")
total_modifies = sum(s.get("docs_modifies", 0) for s in stats if s.get("statut") == "ok")
tashkeel_rest  = sum(s.get("tashkeel_rest", 0) for s in stats if s.get("statut") == "ok")
tatweel_rest   = sum(s.get("tatweel_rest",  0) for s in stats if s.get("statut") == "ok")
erreurs        = [s for s in stats if "_erreur" in s]

print(f"✅ Traitement terminé en {time.time()-temps_traitement:.2f}s")
if erreurs:
    for e in erreurs:
        print(f"   ⚠️  {e.get('_erreur')}")

# 6. Vérification
print("\n🔎 VÉRIFICATION FINALE...")
total_en_dest = coll_dest.count_documents({})
succes        = total_en_dest == total_inseres
statut        = "✅ SUCCÈS TOTAL !" if (succes and tashkeel_rest == 0 and tatweel_rest == 0) else "⚠️ VÉRIFIER"

print(f"   ┌──────────────────────────────────────────────────┐")
print(f"   │ Documents insérés        : {total_inseres:<21}│")
print(f"   │ Documents modifiés       : {total_modifies:<21}│")
print(f"   │ Tashkeel restants        : {tashkeel_rest:<21}│")
print(f"   │ Tatweel restants         : {tatweel_rest:<21}│")
print(f"   │ Statut : {statut:<42}│")
print(f"   └──────────────────────────────────────────────────┘")

# 7. Rapport
temps_total = time.time() - temps_debut
rapport = f"""
{"="*70}
RAPPORT — NORMALISATION ARABE v3.8.2 (Multi-Node Spark)
{"="*70}
Date        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode        : {MODE} | Spark 4.1.1 | {NB_WORKERS} Workers
Collection  : {DB_NAME}.{COLLECTION_SOURCE} → {COLLECTION_DEST}
Dict        : {DICT_PATH}
Rules       : {LING_PATH}

NOUVEAUTÉS v3 vs v2 :
   ✅ FIX-1  : Protection chiffres séparateurs (15:00 / 8000 DA)
   ✅ FIX-2  : Chiffres tronqués corrigés
   ✅ FIX-3  : جدا جدا conservé
   ✅ FIX-4  : Faux doublons suffixes arabes
   ✅ FIX-5  : Arabizi avant latin_dom (slm/mkch)
   ✅ FIX-6  : Texte latin dominant protégé
   ✅ FIX-7  : ئ→ي supprimé (سيئة conservé)
   ✅ FIX-8→26 : dictionnaire enrichi

RÉSULTATS :
   • Total source          : {total_lignes}
   • Documents modifiés    : {total_modifies}
   • Total inséré          : {total_inseres}
   • Tashkeel restants     : {tashkeel_rest}
   • Tatweel restants      : {tatweel_rest}
   • Statut                : {statut}

TEMPS :
   • Total                 : {temps_total:.2f}s
   • Vitesse               : {total_lignes/temps_total:.0f} docs/s
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
print("🎉 NORMALISATION v3.8.2 TERMINÉE EN MODE MULTI-NODE !")