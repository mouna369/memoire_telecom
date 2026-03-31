# # # #!/usr/bin/env python3
# # # # -*- coding: utf-8 -*-

# # # """
# # # normalisation_multinode_v4.py
# # # ====================================
# # # Normaliseur unifié pour corpus télécom DZ (25K commentaires).
# # # Version MULTI-NŒUDS avec Spark pour traitement distribué.
# # # Intègre TextNormalizer v3.9.3 (code binôme).

# # # Compatible : AraBERT  |  CAMeL-BERT  |  MarBERT  |  DziriBERT

# # # Modes :
# # #   "arabert"  → normalisation légère, sans stopwords  (Transformers)
# # #   "full"     → normalisation complète + stopwords     (ML classique / stats)
# # # """

# # # from pyspark.sql import SparkSession
# # # from pymongo import MongoClient, InsertOne
# # # from pymongo.errors import BulkWriteError
# # # from datetime import datetime
# # # import os, time, math, json, re, logging
# # # from pathlib import Path
# # # from typing import Dict, List, Optional, Set, Tuple
# # # from collections import defaultdict

# # # # ============================================================
# # # # CONFIGURATION
# # # # ============================================================
# # # MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# # # MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# # # DB_NAME           = "telecom_algerie"
# # # COLLECTION_SOURCE = "commentaires_sans_emojis"   # ← mis à jour (pas emojis)
# # # COLLECTION_DEST   = "commentaires_normalises"
# # # BATCH_SIZE        = 1000
# # # NB_WORKERS        = 3
# # # SPARK_MASTER      = "spark://spark-master:7077"
# # # RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_normalisation_multinode.txt"

# # # MASTER_DICT_PATH = Path("/opt/dictionnaires/master_dict.json")
# # # RULES_DICT_PATH  = Path("/opt/dictionnaires/linguistic_rules.json")

# # # # ============================================================
# # # # TEXT NORMALIZER v3.9.3 (code binôme — tous les FIX inclus)
# # # # ============================================================

# # # _RE: Dict[str, re.Pattern] = {
# # #     "diacritics":    re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06ED]"),
# # #     "tatweel":       re.compile(r"\u0640+"),
# # #     "rep_chars":     re.compile(r"(?<!\d)(.)\1{2,}(?!\d)"),
# # #     "rep_punct":     re.compile(r"([!?.،:;])\1+"),
# # #     "whitespace":    re.compile(r"\s+"),
# # #     "digits_only":   re.compile(r"^\d+$"),
# # #     "pure_latin":    re.compile(r"^[a-zA-Z'\u2019\-]+$"),
# # #     "arabizi_hyb":   re.compile(r"^(?=.*[a-zA-Z])(?=.*(?<=[a-zA-Z])[35679]|[35679](?=[a-zA-Z])).+$"),
# # #     "has_arabic":    re.compile(r"[\u0600-\u06FF]"),
# # #     "trail_punct":   re.compile(r"^(.*[^!.،,;:؟?])((?:[!.،,;:؟?])+)$"),
# # #     "num_arabic":    re.compile(r"^(\d+)([\u0600-\u06FF\u0750-\u077F].*)$"),
# # #     "unit_nospace":  re.compile(r"(?<!\w)(\d+)([a-zA-Z/]+(?:ps|/s)?)(?=[\u0600-\u06FF\s,،.!?؟$]|$)", re.IGNORECASE),
# # #     "unit_space":    re.compile(r"\b(\d+)\s+([a-zA-Z/]+(?:ps|/s)?)\b", re.IGNORECASE),
# # #     "arab_digit":    re.compile(r"([\u0600-\u06FF])(\d)"),
# # #     "digit_arab":    re.compile(r"(\d)([\u0600-\u06FF])"),
# # #     "spaced_digits": re.compile(r"(?<![:\-\d])(\d)(?: (\d)){1,6}(?![:\-\d])"),
# # #     "arabic_digits_spaced": re.compile(r"(?<![٠-٩])([٠-٩])(?: ([٠-٩])){1,6}(?![٠-٩])"),
# # #     "arabic_prefix": re.compile(r"^(وال|فال|بال|كال|لل|ال|و|ف|ب|ك|ل)(.+)$"),
# # #     "num_separator": re.compile(r"(?<!\d)(\d+)([-:/])(\d+)(?!\d)"),
# # #     "ar_then_lat":   re.compile(r"([\u0600-\u06FF])([a-zA-Z])"),
# # #     "lat_then_ar":   re.compile(r"([a-zA-Z])([\u0600-\u06FF])"),
# # #     "regex_key":     re.compile(r"[\\^$*+?.()\[\]{}|]"),
# # #     "wifi_version":  re.compile(r"\bwifi\d+\b", re.IGNORECASE),  # FIX-42
# # # }

# # # _AR_VERB_PREFIXES: Set[str] = {"ت", "ي", "ن", "أ", "ا", "تت", "يت", "نت", "ست"}
# # # _AR_CONJ_PREFIXES: Set[str] = {"و", "ف", "ب", "ك", "ل", "ال", "لل", "بال", "كال", "فال", "وال"}
# # # _AR_ALL_PREFIXES: Set[str]  = _AR_VERB_PREFIXES | _AR_CONJ_PREFIXES

# # # # FIX-38/43 : clés techniques protégées
# # # _ABBREV_KEYS_TO_PROTECT: Set[str] = {
# # #     "adsl", "vdsl", "ftth", "fttb", "xgs", "xgs-pon", "pon", "dsl",
# # #     "lte", "volte", "dns", "ont", "wifi", "wify", "wi-fi", "wlan",
# # #     "4g", "3g", "2g", "5g", "cnx", "rdm",
# # #     "mbps", "kbps", "gbps", "mbs", "mb/s", "gb/s", "kb/s",
# # #     "idoom", "djezzy", "mobilis", "ooredoo",
# # #     "fibre", "fiber", "febre", "ping", "hd", "fhd", "uhd",
# # #     "ram", "rom", "cpu", "gpu", "android", "ios",
# # #     "4k", "8k", "2k", "netflix", "amazon", "fortnite",
# # #     "nokia", "tenda", "zte", "huawei", "xiaomi",
# # #     "iptv", "ip80", "ethernet", "gigabit", "starlink",
# # #     "reset", "re-provision", "reprovision",
# # #     "qoe", "speedtest", "backbone", "throughput",
# # #     "routing", "peering", "cdn", "olt", "qos", "jitter", "gpon",
# # #     "ms",       # FIX-38
# # #     "xg-spon",  # FIX-43
# # #     "xg-s-pon", # FIX-43
# # # }

# # # _ABBREV_AMBIGUOUS_SKIP: Set[str] = {
# # #     "re", "co", "da", "an", "ko", "mo", "go", "gb", "mb", "kb",
# # #     "bla", "li", "ki", "w", "at", "db", "tt", "pr", "pk", "pq", "nn",
# # #     "cc", "cv", "fb", "yt", "ok",
# # # }

# # # # FIX-39 : composés protégés
# # # _PROTECTED_COMPOUNDS: List[str] = [
# # #     "my idoom", "my ooredoo", "my djezzy", "my mobilis",
# # #     "no transaction", "no transction",
# # #     "fibre optique", "core network", "traffic shaping",
# # #     "prime video", "prime vidéo", "google tv", "tv box", "twin box", "mi box",
# # # ]
# # # _COMPOUND_PATTERNS: List[Tuple[re.Pattern, str]] = [
# # #     (re.compile(re.escape(c), re.IGNORECASE), c)
# # #     for c in sorted(_PROTECTED_COMPOUNDS, key=len, reverse=True)
# # # ]

# # # # FIX-39/40/44 : mots arabizi supplémentaires
# # # _EXTRA_ARABIZI_WORDS: Dict[str, str] = {
# # #     "nachalh": "إن شاء الله", "nchalh": "إن شاء الله",
# # #     "inchalh": "إن شاء الله", "inshalah": "إن شاء الله",
# # #     "inshalh": "إن شاء الله", "inshaallah": "إن شاء الله",
# # #     "inchalah": "إن شاء الله", "inshaalah": "إن شاء الله",
# # #     "inchallah": "إن شاء الله", "inshallah": "إن شاء الله",
# # #     "nchallah": "إن شاء الله", "nshallah": "إن شاء الله",
# # #     "wlh": "والله", "wlhi": "والله", "wellah": "والله",
# # #     "wella": "والله", "wallah": "والله", "wallahi": "والله",
# # #     "wallhy": "والله",
# # #     "flexi": "فليكسي", "flexili": "فليكسي",
# # #     "nflexi": "فليكسي", "yflexi": "فليكسي",
# # #     "my": "my",               # FIX-39
# # #     "promotion": "promotion", # FIX-40
# # #     "promo": "promo",         # FIX-40
# # #     # FIX-44
# # #     "n9drou": "نقدرو", "nkhlsou": "نخلصو", "nl9a": "نلقا",
# # #     "chhar": "شهر", "fibr": "fibre optique", "psq": "parce que",
# # #     "ndkhol": "ندخل", "nkhles": "نخلص",
# # #     "khir": "خير", "khorda": "خردة",
# # #     "ytl3": "يطلع", "boost": "boost", "fel": "في ال", "balak": "بالاك",
# # #     "khoya": "خويا", "khouya": "خويا", "dyal": "ديال", "dial": "ديال",
# # #     "bhal": "بحال", "mazel": "مزال", "sahbi": "صاحبي",
# # #     "3andek": "عندك", "3andi": "عندي", "hadchi": "هذا الشيء",
# # #     "wakha": "واخا", "rabi": "ربي", "yehdi": "يهدي",
# # #     "yehdikoum": "يهديكم", "mn": "من", "fi": "في",
# # #     "hdra": "هدرة", "khdma": "خدمة",
# # # }

# # # _EXTRA_ARABIZI_UPPER: Dict[str, str] = {
# # #     "NACHALH": "إن شاء الله", "NCHALH": "إن شاء الله",
# # #     "INCHALH": "إن شاء الله", "INSHALAH": "إن شاء الله",
# # #     "INSHALLAH": "إن شاء الله", "INCHALAH": "إن شاء الله",
# # #     "INCHALLAH": "إن شاء الله",
# # #     "WLH": "والله", "WELLAH": "والله", "WALLAH": "والله", "WALLAHI": "والله",
# # # }

# # # _EXTRA_AR_PATTERNS: List[Tuple[re.Pattern, str]] = [
# # #     (re.compile(r"\bانشاء الله\b"),  "إن شاء الله"),
# # #     (re.compile(r"\bنشاالله\b"),     "إن شاء الله"),
# # #     (re.compile(r"\bانشالله\b"),     "إن شاء الله"),
# # #     (re.compile(r"\bنشالله\b"),      "إن شاء الله"),
# # #     (re.compile(r"\bانشاالله\b"),    "إن شاء الله"),
# # #     (re.compile(r"\bاشالله\b"),      "إن شاء الله"),
# # #     (re.compile(r"\bفكيكسيت\b"),     "فليكسي"),
# # #     (re.compile(r"\bفلكسيلي\b"),     "فليكسي"),
# # #     (re.compile(r"\bفليكسيلي\b"),    "فليكسي"),
# # #     (re.compile(r"\bفليكسيت\b"),     "فليكسي"),
# # #     (re.compile(r"\bنفليكسي\b"),     "فليكسي"),
# # #     (re.compile(r"\bيفليكسي\b"),     "فليكسي"),
# # #     (re.compile(r"\bتفليكسي\b"),     "فليكسي"),
# # # ]

# # # HORS_SCOPE_KEYWORDS: List[str] = [
# # #     "embauche", "entretien d'embauche", "recrutement", "offre d'emploi",
# # #     "sarl maxim", "hashtag#",
# # #     "code du travail", "loi 90-11", "comité de participation",
# # #     "عدل3", "عدل 3", "حق_الطعون", "مراجعة_الملفات",
# # #     "المقصيون_من_عدل", "الشفافية_في_عدل",
# # # ]


# # # def _load_rules(rules_path):
# # #     if not rules_path.exists():
# # #         return {}
# # #     with open(rules_path, encoding="utf-8") as f:
# # #         return json.load(f)

# # # def _build_negations(rules):
# # #     neg = rules.get("negations", {})
# # #     result = set(neg.get("arabe_standard", []))
# # #     result.update(neg.get("dialecte_algerien", []))
# # #     result.update({"ما", "لا", "لم", "لن", "ليس", "غير", "مش", "ميش",
# # #                    "مكانش", "ماكش", "ماهيش", "ماهوش"})
# # #     return result

# # # def _build_dialect_keep(rules):
# # #     dk = rules.get("dialect_keep", {})
# # #     result = set()
# # #     for v in dk.values():
# # #         if isinstance(v, list):
# # #             result.update(v)
# # #     result.update({"راه", "راهي", "راني", "راك", "راهم", "واش", "كيفاش", "وين",
# # #                    "علاش", "قديش", "مزال", "كيما", "باه", "ديما",
# # #                    "ya", "pas", "de", "en", "avec", "depuis", "pour",
# # #                    "par", "sur", "sous", "dans", "entre", "sans", "vers"})
# # #     return result

# # # def _build_intentional_repeats(rules):
# # #     ir = rules.get("intentional_repeats", {})
# # #     result = set()
# # #     for v in ir.values():
# # #         if isinstance(v, list):
# # #             result.update(v)
# # #     result.update({"كيف", "شوي", "برا", "هاك", "يا", "آه", "لا", "واو",
# # #                    "بزاف", "مزال", "كل", "نعم", "جدا",
# # #                    "très", "trop", "bien", "non", "oui", "si", "jamais", "encore"})
# # #     return result

# # # def _build_tech_tokens(rules):
# # #     tt = rules.get("tech_tokens", {})
# # #     result = set()
# # #     for v in tt.values():
# # #         if isinstance(v, list):
# # #             result.update(t.lower() for t in v)
# # #     result.discard("cnx")
# # #     result.update(_ABBREV_KEYS_TO_PROTECT)
# # #     return result

# # # def _build_arabic_prefixes(rules):
# # #     ap = rules.get("arabic_prefixes", {})
# # #     prefixes = ap.get("prefixes", [])
# # #     if not prefixes:
# # #         prefixes = ["وال", "فال", "بال", "كال", "لل", "ال", "و", "ف", "ب", "ك", "ل"]
# # #     return sorted(prefixes, key=len, reverse=True)

# # # def _build_contractions(rules):
# # #     ct = rules.get("contractions", {})
# # #     result = dict(ct.get("francais", {}))
# # #     result.update({
# # #         "j'ai": "je ai", "j\u2019ai": "je ai",
# # #         "c'est": "ce est", "c\u2019est": "ce est",
# # #         "n'est": "ne est", "n\u2019est": "ne est",
# # #         "n'a": "ne a", "n\u2019a": "ne a",
# # #         "qu'il": "que il", "qu\u2019il": "que il",
# # #         "qu'on": "que on", "qu\u2019on": "que on",
# # #     })
# # #     return result

# # # def _build_protected_words(rules):
# # #     pw = rules.get("protected_words", {})
# # #     result = set()
# # #     for v in pw.values():
# # #         if isinstance(v, list):
# # #             result.update(t.lower() for t in v)
# # #     result.update(_ABBREV_KEYS_TO_PROTECT)
# # #     result.update({
# # #         "internet", "connexion", "problème", "réseau", "service",
# # #         "optique", "ping", "gaming", "game", "live", "speed",
# # #         "high", "low", "lag", "stream",
# # #         "facebook", "whatsapp", "youtube", "instagram",
# # #         "bonjour", "merci", "salut", "normal", "bravo",
# # #         "message", "solution", "compte", "temps",
# # #         "même", "comme", "chaque", "alors",
# # #         "avant", "depuis", "juste", "vraiment",
# # #         "lente", "mois", "plusieurs",
# # #         "pas", "on", "ne", "fait", "rien", "tout", "fois", "bien",
# # #         "moi", "encore", "niveau",
# # #         "promotion", "promo",
# # #         "my idoom", "my ooredoo", "my djezzy", "my mobilis",
# # #         "no transaction", "no transction",
# # #     })
# # #     return result

# # # def _build_stopwords_from_rules(rules, negations, dialect_keep):
# # #     sw_section = rules.get("stopwords", {})
# # #     sw = set()
# # #     for v in sw_section.values():
# # #         if isinstance(v, list):
# # #             sw.update(v)
# # #     if not sw:
# # #         sw = {
# # #             "le", "la", "les", "l", "un", "une", "des", "du", "de", "et",
# # #             "ou", "mais", "donc", "car", "ni", "or", "ce", "cet", "cette",
# # #             "ces", "mon", "ton", "son", "notre", "votre", "leur", "leurs",
# # #             "je", "tu", "il", "elle", "nous", "vous", "ils", "elles",
# # #             "me", "te", "se", "lui", "y", "en", "que", "qui", "quoi", "dont",
# # #             "est", "sont", "être", "avoir", "a", "ai",
# # #             "avec", "sans", "sur", "sous", "dans", "par", "pour",
# # #             "très", "plus", "moins", "aussi", "bien", "tout", "pas", "ne", "on", "si",
# # #             "i", "we", "our", "you", "your", "he", "she", "it", "its",
# # #             "they", "their", "am", "is", "are", "was", "were",
# # #             "have", "has", "had", "do", "does", "did",
# # #             "will", "would", "could", "should", "a", "an", "the",
# # #             "and", "or", "but", "if", "in", "on", "at", "by", "for",
# # #             "with", "to", "from", "not", "no",
# # #             "إلى", "عن", "مع", "كان", "كانت", "هذا", "هذه", "ذلك", "تلك",
# # #             "هو", "هي", "هم", "هن", "ثم", "أو", "إن", "إذا", "لو", "قد",
# # #             "لكن", "بل", "حتى", "ضد", "أن", "التي", "الذي", "الذين",
# # #         }
# # #     try:
# # #         from nltk.corpus import stopwords as _sw
# # #         sw.update(_sw.words("arabic"))
# # #     except Exception:
# # #         pass
# # #     return sw - negations - dialect_keep

# # # def _compile_dict(d, flags=re.UNICODE):
# # #     result = []
# # #     for pat, repl in d.items():
# # #         try:
# # #             result.append((re.compile(pat, flags), repl))
# # #         except re.error:
# # #             pass
# # #     return result

# # # def _build_contraction_re(contractions):
# # #     apostrophe_keys = [k for k in contractions if "'" in k or "\u2019" in k]
# # #     if not apostrophe_keys:
# # #         return re.compile(r"(?!)")
# # #     escaped = sorted([re.escape(k) for k in apostrophe_keys], key=len, reverse=True)
# # #     return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)


# # # class TextNormalizer:
# # #     """TextNormalizer v3.9.3 — compatible Spark (FIX-38 à FIX-44 inclus)"""

# # #     def __init__(self, mode="arabert", dict_path=None, rules_path=None, remove_stopwords=False):
# # #         assert mode in ("arabert", "full")
# # #         self.mode      = mode
# # #         self.remove_sw = remove_stopwords or (mode == "full")

# # #         dict_path  = dict_path  or MASTER_DICT_PATH
# # #         rules_path = rules_path or RULES_DICT_PATH

# # #         with open(dict_path, encoding="utf-8") as f:
# # #             d = json.load(f)

# # #         rules = _load_rules(rules_path)

# # #         self._negations       = _build_negations(rules)
# # #         self._dialect_keep    = _build_dialect_keep(rules)
# # #         self._intentional_rep = _build_intentional_repeats(rules)
# # #         self._tech_tokens     = _build_tech_tokens(rules)
# # #         self._arabic_prefixes = _build_arabic_prefixes(rules)
# # #         self._contractions    = _build_contractions(rules)
# # #         self._contraction_re  = _build_contraction_re(self._contractions)

# # #         self.unicode_map = d["unicode_arabic"]
# # #         self.unicode_map.setdefault("\u0629", "\u0647")
# # #         self.unicode_map.setdefault("\u0624", "\u0648")

# # #         self.digrams   = d["arabizi_digrams"]
# # #         self.monograms = {k: v for k, v in d["arabizi_monograms"].items()
# # #                           if not (len(k) == 1 and k.isalpha())}

# # #         self.arabizi_words = {**d["arabizi_words"], **_EXTRA_ARABIZI_WORDS}
# # #         self.arabizi_words = {k: v for k, v in self.arabizi_words.items()
# # #                                if not k.startswith("_")}
# # #         self.arabizi_upper = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}
# # #         self.emojis        = d["emojis"]
# # #         self.abbreviations = d["abbreviations"]
# # #         self.telecom       = {k: v for k, v in d["telecom_tech"].items()
# # #                                if not _RE["regex_key"].search(k)}
# # #         self.units_map     = d["units"]

# # #         nv             = d["network_variants"]
# # #         self._net_form = nv["normalized_form"]
# # #         self._net_all  = nv["latin"] + nv["arabic"]

# # #         mixed_clean      = {k: v for k, v in d["mixed_ar_fr_regex"].items()
# # #                              if not k.startswith("_")}
# # #         self._mixed_pats = _compile_dict(mixed_clean)
# # #         self._fr_pats    = _compile_dict(d["french_corrections_regex"],
# # #                                          flags=re.IGNORECASE | re.UNICODE)

# # #         escaped_net  = [re.escape(v) for v in self._net_all if v]
# # #         self._net_re = re.compile(
# # #             rf'\b({"|".join(escaped_net)})\b', re.IGNORECASE
# # #         ) if escaped_net else None

# # #         combined                   = {**self.digrams, **self.monograms}
# # #         self._arabizi_seq          = sorted(combined.items(), key=lambda x: len(x[0]), reverse=True)
# # #         self._arabizi_upper_sorted = sorted(self.arabizi_upper.items(), key=lambda x: len(x[0]), reverse=True)

# # #         _all_vals = []
# # #         for v in list(self.telecom.values()) + list(self.abbreviations.values()):
# # #             if v:
# # #                 _all_vals.extend(v.split())

# # #         self._protected = _build_protected_words(rules)
# # #         self._protected.update(t.lower() for t in _all_vals)
# # #         self._protected.update(self._tech_tokens)

# # #         self._stopwords = (
# # #             _build_stopwords_from_rules(rules, self._negations, self._dialect_keep)
# # #             if self.remove_sw else set()
# # #         )

# # #     def _split_prefix(self, word):
# # #         for p in self._arabic_prefixes:
# # #             if word.startswith(p) and len(word) > len(p) + 1:
# # #                 return p, word[len(p):]
# # #         return "", word

# # #     def _lookup(self, word, dct):
# # #         lo = word.lower()
# # #         v  = dct.get(lo) or dct.get(word)
# # #         if v is not None:
# # #             return v
# # #         pref, root = self._split_prefix(word)
# # #         if pref:
# # #             v = dct.get(root.lower()) or dct.get(root)
# # #             if v is not None:
# # #                 return v if " " in v else pref + v
# # #         return None

# # #     def _is_latin_dominant(self, text):
# # #         lat = sum(1 for c in text if "a" <= c.lower() <= "z")
# # #         ar  = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
# # #         tot = lat + ar
# # #         if not tot:
# # #             return False
# # #         threshold = 0.70 if len(text) < 30 else 0.75
# # #         return (lat / tot) > threshold

# # #     @staticmethod
# # #     def _is_proper_noun_token(tok):
# # #         """FIX-41"""
# # #         if len(tok) < 3 or not _RE["pure_latin"].match(tok):
# # #             return False
# # #         return tok[0].isupper() and not tok.isupper()

# # #     @staticmethod
# # #     def _is_hors_scope(text):
# # #         if not text:
# # #             return False
# # #         lo = text.lower()
# # #         return any(kw.lower() in lo for kw in HORS_SCOPE_KEYWORDS)

# # #     def _protect_numbers(self, text):
# # #         protected = {}
# # #         counter   = [0]
# # #         def _protect(m):
# # #             key = f"__NP{counter[0]}__"
# # #             counter[0] += 1
# # #             protected[key] = m.group(0)
# # #             return key
# # #         text = _RE["num_separator"].sub(_protect, text)
# # #         text = re.sub(r"(?<!\d)(\d+)\s*/\s*(\d+)(?!\d)", _protect, text)
# # #         return protected, text

# # #     def _protect_compounds_pre(self, text):
# # #         """FIX-39"""
# # #         placeholders = {}
# # #         counter      = [0]
# # #         for pat, original in _COMPOUND_PATTERNS:
# # #             if pat.search(text):
# # #                 ph = f"__CPD{counter[0]}__"
# # #                 counter[0] += 1
# # #                 placeholders[ph] = original
# # #                 text = pat.sub(ph, text)
# # #         return placeholders, text

# # #     def _dedup_tokens(self, text):
# # #         tokens = text.split()
# # #         if len(tokens) < 2:
# # #             return text
# # #         result = [tokens[0]]
# # #         for i in range(1, len(tokens)):
# # #             prev, curr       = result[-1], tokens[i]
# # #             prev_lo, curr_lo = prev.lower(), curr.lower()
# # #             if curr_lo == prev_lo:
# # #                 if curr_lo in self._intentional_rep:
# # #                     result.append(curr)
# # #                 continue
# # #             has_ar_curr = bool(_RE["has_arabic"].search(curr))
# # #             if (not has_ar_curr and len(curr) >= 4
# # #                     and len(curr) < len(prev) and prev_lo.endswith(curr_lo)):
# # #                 continue
# # #             if (has_ar_curr and len(curr) >= 5
# # #                     and len(curr) < len(prev) and prev.endswith(curr)):
# # #                 m = _RE["arabic_prefix"].match(prev)
# # #                 if m and m.group(2) == curr:
# # #                     result.append(curr); continue
# # #                 stripped = prev[:len(prev)-len(curr)]
# # #                 if stripped in _AR_ALL_PREFIXES and len(stripped) >= 2:
# # #                     result.append(curr); continue
# # #                 result.append(curr); continue
# # #             result.append(curr)
# # #         return " ".join(result)

# # #     def normalize(self, text):
# # #         if not isinstance(text, str) or not text.strip():
# # #             return ""
# # #         try:
# # #             protected_nums, text = self._protect_numbers(text)
# # #             protected_cpd,  text = self._protect_compounds_pre(text)  # FIX-39

# # #             text = self._step_emojis(text)
# # #             text = self._step_unicode_arabic(text)
# # #             text = self._step_extra_ar(text)

# # #             text = _RE["wifi_version"].sub(lambda m: m.group(0).lower(), text)  # FIX-42

# # #             for pat, repl in self._mixed_pats:
# # #                 text = pat.sub(repl, text)
# # #             text = self._step_french(text)
# # #             text = self._step_abbrev(text)
# # #             text = self._step_units(text)
# # #             text = self._step_split_mixed_tokens(text)
# # #             text = self._step_arabizi(text)
# # #             text = self._step_cleanup(text)
# # #             text = self._dedup_tokens(text)
# # #             if self.remove_sw:
# # #                 text = self._step_stopwords(text)

# # #             for ph, original in protected_cpd.items():
# # #                 text = text.replace(ph, original)
# # #             for key, val in protected_nums.items():
# # #                 text = text.replace(key, val)

# # #         except Exception:
# # #             return ""
# # #         return text.strip()

# # #     def _step_emojis(self, text):
# # #         for emoji, word in self.emojis.items():
# # #             if emoji in text:
# # #                 text = text.replace(emoji, f" {word} ")
# # #         return text

# # #     def _step_extra_ar(self, text):
# # #         for pat, repl in _EXTRA_AR_PATTERNS:
# # #             text = pat.sub(repl, text)
# # #         return text

# # #     def _step_units(self, text):
# # #         def _repl(m):
# # #             num, unit = m.group(1), m.group(2).lower()
# # #             KEEP_ATTACHED = {"ms", "h", "s"}
# # #             if unit in KEEP_ATTACHED:
# # #                 return f"{num}{unit}"
# # #             if len(unit) == 1 and unit not in self.units_map:
# # #                 return m.group(0)
# # #             norm = self.units_map.get(unit)
# # #             if norm is None:
# # #                 return m.group(0)
# # #             return f"{num} {norm}"
# # #         text = _RE["unit_space"].sub(_repl, text)
# # #         text = _RE["unit_nospace"].sub(_repl, text)
# # #         return text

# # #     def _step_abbrev(self, text):
# # #         latin_dom      = self._is_latin_dominant(text)
# # #         tokens, result = text.split(), []
# # #         i = 0
# # #         while i < len(tokens):
# # #             tok  = tokens[i]
# # #             m    = _RE["trail_punct"].match(tok)
# # #             core, trail = (m.group(1), m.group(2)) if m else (tok, "")
# # #             lo_core = core.lower()

# # #             if core.startswith("__CPD") and core.endswith("__"):
# # #                 result.append(tok); i += 1; continue
# # #             if core in self._negations:
# # #                 result.append(tok); i += 1; continue
# # #             if (not _RE["has_arabic"].search(core)
# # #                     and not _RE["digits_only"].match(core)
# # #                     and lo_core in self.arabizi_words):
# # #                 result.append(tok); i += 1; continue
# # #             if lo_core in self._tech_tokens:
# # #                 result.append(tok); i += 1; continue

# # #             mn = _RE["num_arabic"].match(core)
# # #             if mn:
# # #                 num, unit = mn.groups()
# # #                 repl = self._lookup(unit, self.telecom) or unit
# # #                 if " " in repl:
# # #                     result += [num] + repl.split()[:-1] + [repl.split()[-1] + trail]
# # #                 else:
# # #                     result += [num, repl + trail]
# # #                 i += 1; continue

# # #             resolved = False
# # #             for dct in (self.telecom, self.abbreviations):
# # #                 if (dct is self.abbreviations
# # #                         and lo_core in _ABBREV_AMBIGUOUS_SKIP
# # #                         and latin_dom):
# # #                     continue
# # #                 repl = self._lookup(core, dct)
# # #                 if repl is not None:
# # #                     if (latin_dom and dct is self.telecom
# # #                             and not _RE["has_arabic"].search(core)
# # #                             and len(core) <= 4
# # #                             and lo_core not in _ABBREV_KEYS_TO_PROTECT):
# # #                         break
# # #                     if " " in repl:
# # #                         parts = repl.split()
# # #                         result += parts[:-1] + [parts[-1] + trail]
# # #                     else:
# # #                         result.append(repl + trail)
# # #                     resolved = True
# # #                     break

# # #             if not resolved:
# # #                 if self._net_re and self._net_re.fullmatch(core):
# # #                     result.append(self._net_form + trail)
# # #                 else:
# # #                     result.append(tok)
# # #             i += 1
# # #         return " ".join(result)

# # #     def _step_french(self, text):
# # #         text = self._contraction_re.sub(
# # #             lambda m: self._contractions.get(m.group(0).lower(), m.group(0)), text)
# # #         for pat, repl in self._fr_pats:
# # #             text = pat.sub(repl, text)
# # #         return text

# # #     def _step_split_mixed_tokens(self, text):
# # #         text = _RE["ar_then_lat"].sub(r"\1 \2", text)
# # #         text = _RE["lat_then_ar"].sub(r"\1 \2", text)
# # #         return text

# # #     def _step_arabizi(self, text):
# # #         latin_dom = self._is_latin_dominant(text)
# # #         result    = []
# # #         for tok in text.split():
# # #             lo = tok.lower()
# # #             if tok.startswith("__CPD") and tok.endswith("__"):
# # #                 result.append(tok); continue
# # #             if lo in self._tech_tokens:
# # #                 result.append(tok); continue
# # #             if _RE["has_arabic"].search(tok):
# # #                 result.append(tok); continue
# # #             if _RE["digits_only"].match(tok):
# # #                 result.append(tok); continue
# # #             if _RE["num_arabic"].match(tok):
# # #                 result.append(tok); continue

# # #             if _RE["pure_latin"].match(tok):
# # #                 _AMBIGUOUS_SHORT = {"ki", "el", "da", "li", "w", "dz"}
# # #                 w = self.arabizi_words.get(lo)
# # #                 if w:
# # #                     if latin_dom and len(lo) <= 2 and lo in _AMBIGUOUS_SHORT:
# # #                         result.append(tok)
# # #                     else:
# # #                         result.append(w)
# # #                     continue
# # #                 for k, v in self._arabizi_upper_sorted:
# # #                     if tok.upper() == k:
# # #                         result.append(v); break
# # #                 else:
# # #                     if self._is_proper_noun_token(tok):  # FIX-41
# # #                         result.append(tok); continue
# # #                     if latin_dom:
# # #                         result.append(tok); continue
# # #                     result.append(self._arabizi_convert(tok))
# # #                 continue

# # #             if _RE["arabizi_hyb"].match(tok):
# # #                 w = self.arabizi_words.get(lo)
# # #                 if w:
# # #                     result.append(w); continue
# # #                 for k, v in self._arabizi_upper_sorted:
# # #                     if tok.upper() == k:
# # #                         result.append(v); break
# # #                 else:
# # #                     if latin_dom:
# # #                         result.append(tok)
# # #                     else:
# # #                         result.append(self._arabizi_convert(tok))
# # #                 continue

# # #             result.append(tok)
# # #         return " ".join(result)

# # #     def _arabizi_convert(self, token):
# # #         for k, v in self._arabizi_upper_sorted:
# # #             if token.upper() == k:
# # #                 return v
# # #         result = token.lower()
# # #         for extra, ar in [("ee", "ي"), ("ii", "ي"), ("oo", "و"), ("pp", "ب")]:
# # #             result = result.replace(extra, ar)
# # #         for seq, ar in self._arabizi_seq:
# # #             result = result.replace(seq, ar)
# # #         result = re.sub(r"[a-z]", "", result)
# # #         return result

# # #     def _step_unicode_arabic(self, text):
# # #         text = _RE["diacritics"].sub("", text)
# # #         text = _RE["tatweel"].sub("", text)
# # #         for variant, canonical in self.unicode_map.items():
# # #             text = text.replace(variant, canonical)
# # #         return text

# # #     def _step_cleanup(self, text):
# # #         text = _RE["arab_digit"].sub(r"\1 \2", text)
# # #         text = _RE["digit_arab"].sub(r"\1 \2", text)
# # #         def _fuse_spaced(m):
# # #             return m.group(0).replace(" ", "")
# # #         text = _RE["spaced_digits"].sub(_fuse_spaced, text)
# # #         text = _RE["arabic_digits_spaced"].sub(_fuse_spaced, text)
# # #         text = _RE["rep_chars"].sub(lambda m: m.group(1) * 2, text)
# # #         text = _RE["rep_punct"].sub(lambda m: m.group(1), text)
# # #         text = _RE["whitespace"].sub(" ", text).strip()
# # #         return text

# # #     def _step_stopwords(self, text):
# # #         if not self._stopwords:
# # #             return text
# # #         placeholders = {}
# # #         for i, compound in enumerate(_PROTECTED_COMPOUNDS):
# # #             pattern = re.compile(re.escape(compound), re.IGNORECASE)
# # #             if pattern.search(text):
# # #                 ph = f"__SWC{i}__"
# # #                 placeholders[ph] = compound
# # #                 text = pattern.sub(ph, text)
# # #         keep   = self._negations | self._dialect_keep
# # #         result = []
# # #         for w in text.split():
# # #             lo = w.lower()
# # #             if w.startswith("__") and w.endswith("__"):
# # #                 result.append(w); continue
# # #             if (w.isdigit() or w in keep or lo in keep
# # #                     or lo not in self._stopwords):
# # #                 result.append(w)
# # #             elif len(w) == 1 and _RE["has_arabic"].match(w):
# # #                 result.append(w)
# # #         text = " ".join(result)
# # #         for ph, original in placeholders.items():
# # #             text = text.replace(ph, original)
# # #         return text


# # # # ============================================================
# # # # FONCTIONS SPARK DISTRIBUÉES
# # # # ============================================================

# # # def normaliser_partition(partition):
# # #     import sys
# # #     sys.path.insert(0, '/opt/pymongo_libs')
# # #     from pymongo import MongoClient, InsertOne
# # #     from pymongo.errors import BulkWriteError

# # #     try:
# # #         normalizer = TextNormalizer(mode="arabert")
# # #         client     = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
# # #         db         = client[DB_NAME]
# # #         collection = db[COLLECTION_DEST]
# # #     except Exception as e:
# # #         yield {"_erreur": str(e), "statut": "connexion_failed"}
# # #         return

# # #     batch        = []
# # #     docs_traites = 0
# # #     docs_hors    = 0

# # #     for row in partition:
# # #         texte_original = row.get("Commentaire_Client", "") or ""

# # #         if TextNormalizer._is_hors_scope(texte_original):
# # #             docs_hors += 1
# # #             continue

# # #         commentaire_normalise = normalizer.normalize(texte_original)
# # #         moderateur_normalise  = normalizer.normalize(row.get("commentaire_moderateur", "") or "")

# # #         doc = {
# # #             "_id"                    : row.get("_id"),
# # #             "Commentaire_Client"     : commentaire_normalise,
# # #             "commentaire_moderateur" : moderateur_normalise,
# # #             "date"                   : row.get("date"),
# # #             "source"                 : row.get("source"),
# # #             "moderateur"             : row.get("moderateur"),
# # #             "metadata"               : row.get("metadata"),
# # #             "statut"                 : row.get("statut"),
# # #         }
# # #         batch.append(InsertOne(doc))
# # #         docs_traites += 1

# # #         if len(batch) >= BATCH_SIZE:
# # #             try:
# # #                 collection.bulk_write(batch, ordered=False)
# # #             except BulkWriteError:
# # #                 pass
# # #             batch = []

# # #     if batch:
# # #         try:
# # #             collection.bulk_write(batch, ordered=False)
# # #         except BulkWriteError:
# # #             pass

# # #     client.close()
# # #     yield {"docs_traites": docs_traites, "docs_hors_scope": docs_hors, "statut": "ok"}


# # # def lire_partition_depuis_mongo(partition_info):
# # #     import sys
# # #     sys.path.insert(0, '/opt/pymongo_libs')
# # #     from pymongo import MongoClient

# # #     for item in partition_info:
# # #         client     = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
# # #         db         = client[DB_NAME]
# # #         collection = db[COLLECTION_SOURCE]
# # #         curseur    = collection.find(
# # #             {},
# # #             {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
# # #              "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
# # #         ).skip(item["skip"]).limit(item["limit"])
# # #         for doc in curseur:
# # #             doc["_id"] = str(doc["_id"])
# # #             yield doc
# # #         client.close()


# # # # ============================================================
# # # # PIPELINE PRINCIPAL
# # # # ============================================================
# # # temps_debut_global = time.time()

# # # print("=" * 70)
# # # print("🔤 NORMALISATION MULTI-NŒUDS v3.9.3")
# # # print("   FIX-38 : ms protégé (unité latence) ✅")
# # # print("   FIX-39 : my idoom/djezzy/mobilis/ooredoo ✅")
# # # print("   FIX-40 : بروموسيو → promotion ✅")
# # # print("   FIX-41 : noms propres préservés ✅")
# # # print("   FIX-42 : wifi5/wifi6/wifi7 protégé ✅")
# # # print("   FIX-43 : XG-SPON protégé ✅")
# # # print("   FIX-44 : arabizi hybride converti ✅")
# # # print(f"   Source : {COLLECTION_SOURCE}")
# # # print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# # # print("=" * 70)

# # # # 1. CONNEXION MONGODB DRIVER
# # # print("\n📂 Connexion MongoDB (Driver)...")
# # # try:
# # #     client_driver = MongoClient(MONGO_URI_DRIVER)
# # #     db_driver     = client_driver[DB_NAME]
# # #     coll_source   = db_driver[COLLECTION_SOURCE]
# # #     total_docs    = coll_source.count_documents({})
# # #     print(f"✅ MongoDB connecté — {total_docs} documents dans la source")
# # # except Exception as e:
# # #     print(f"❌ Erreur MongoDB : {e}")
# # #     exit(1)

# # # # 2. CONNEXION SPARK
# # # print("\n⚡ Connexion au cluster Spark...")
# # # temps_debut_spark = time.time()
# # # spark = SparkSession.builder \
# # #     .appName("Normalisation_MultiNode_v393") \
# # #     .master(SPARK_MASTER) \
# # #     .config("spark.executor.memory", "2g") \
# # #     .config("spark.executor.cores", "2") \
# # #     .config("spark.sql.shuffle.partitions", "4") \
# # #     .getOrCreate()
# # # spark.sparkContext.setLogLevel("WARN")
# # # temps_fin_spark = time.time()
# # # print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")

# # # # 3. LECTURE DISTRIBUÉE
# # # print("\n📥 LECTURE DISTRIBUÉE...")
# # # temps_debut_chargement = time.time()
# # # docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# # # plages = [
# # #     {"skip": i * docs_par_worker,
# # #      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
# # #     for i in range(NB_WORKERS)
# # # ]
# # # for idx, p in enumerate(plages):
# # #     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

# # # rdd_data = spark.sparkContext \
# # #     .parallelize(plages, NB_WORKERS) \
# # #     .mapPartitions(lire_partition_depuis_mongo)

# # # df_spark = spark.read.json(rdd_data.map(
# # #     lambda d: json.dumps(
# # #         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
# # #          for k, v in d.items()}
# # #     )
# # # ))
# # # total_lignes = df_spark.count()
# # # temps_fin_chargement = time.time()
# # # print(f"✅ {total_lignes} documents chargés en {temps_fin_chargement - temps_debut_chargement:.2f}s")

# # # # 4. VIDER DESTINATION
# # # coll_dest = db_driver[COLLECTION_DEST]
# # # coll_dest.delete_many({})
# # # print("\n🧹 Collection destination vidée")

# # # # 5. NORMALISATION DISTRIBUÉE
# # # print("\n💾 NORMALISATION DISTRIBUÉE...")
# # # temps_debut_traitement = time.time()

# # # rdd_stats = df_spark.rdd \
# # #     .map(lambda row: row.asDict()) \
# # #     .mapPartitions(normaliser_partition)

# # # stats_ecriture = rdd_stats.collect()
# # # total_inseres  = sum(s.get("docs_traites",    0) for s in stats_ecriture if s.get("statut") == "ok")
# # # total_hors     = sum(s.get("docs_hors_scope", 0) for s in stats_ecriture if s.get("statut") == "ok")
# # # erreurs        = [s for s in stats_ecriture if "_erreur" in s]

# # # temps_fin_traitement = time.time()
# # # print(f"✅ Normalisation terminée en {temps_fin_traitement - temps_debut_traitement:.2f}s")
# # # print(f"📦 {total_inseres} documents normalisés")
# # # print(f"🚫 {total_hors} documents hors scope filtrés")
# # # if erreurs:
# # #     for e in erreurs:
# # #         print(f"   ⚠️  {e.get('_erreur')}")

# # # spark.stop()

# # # # 6. VÉRIFICATION FINALE
# # # print("\n🔎 VÉRIFICATION FINALE...")
# # # total_en_dest = coll_dest.count_documents({})
# # # succes        = total_en_dest == total_inseres

# # # print(f"   • Documents source         : {total_lignes}")
# # # print(f"   • Documents normalisés     : {total_inseres}")
# # # print(f"   • Documents hors scope     : {total_hors}")
# # # print(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")

# # # # 7. RAPPORT
# # # temps_fin_global = time.time()
# # # temps_total      = temps_fin_global - temps_debut_global

# # # lignes_rapport = []
# # # lignes_rapport.append("=" * 70)
# # # lignes_rapport.append("🔤 NORMALISATION MULTI-NŒUDS v3.9.3")
# # # lignes_rapport.append(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# # # lignes_rapport.append(f"   Mode : Spark 4.1.1 | {NB_WORKERS} Workers | MongoDB direct")
# # # lignes_rapport.append("=" * 70)
# # # lignes_rapport.append("\nCORRECTIFS APPLIQUÉS :")
# # # lignes_rapport.append("   FIX-38 : ms protégé comme unité de latence")
# # # lignes_rapport.append("   FIX-39 : my idoom/djezzy/mobilis/ooredoo protégé")
# # # lignes_rapport.append("   FIX-40 : بروموسيو → promotion conservé")
# # # lignes_rapport.append("   FIX-41 : noms propres latins préservés")
# # # lignes_rapport.append("   FIX-42 : wifi5/wifi6/wifi7 protégé")
# # # lignes_rapport.append("   FIX-43 : XG-SPON protégé")
# # # lignes_rapport.append("   FIX-44 : arabizi hybride/pur converti (n9drou, nkhlsou...)")
# # # lignes_rapport.append(f"\n📊 RÉSULTATS :")
# # # lignes_rapport.append(f"   ┌────────────────────────────────────────────┐")
# # # lignes_rapport.append(f"   │ Documents source          : {total_lignes:<15} │")
# # # lignes_rapport.append(f"   │ Documents normalisés      : {total_inseres:<15} │")
# # # lignes_rapport.append(f"   │ Documents hors scope      : {total_hors:<15} │")
# # # lignes_rapport.append(f"   │ Taux de succès            : {total_inseres/total_lignes*100:<14.2f}% │")
# # # lignes_rapport.append(f"   └────────────────────────────────────────────┘")
# # # lignes_rapport.append(f"\n⏱️  TEMPS :")
# # # lignes_rapport.append(f"   • Connexion Spark  : {temps_fin_spark - temps_debut_spark:.2f}s")
# # # lignes_rapport.append(f"   • Chargement       : {temps_fin_chargement - temps_debut_chargement:.2f}s")
# # # lignes_rapport.append(f"   • Normalisation    : {temps_fin_traitement - temps_debut_traitement:.2f}s")
# # # lignes_rapport.append(f"   • TOTAL            : {temps_total:.2f}s ({total_lignes/temps_total:.0f} doc/s)")
# # # lignes_rapport.append(f"\n📁 STOCKAGE :")
# # # lignes_rapport.append(f"   • Source      : {DB_NAME}.{COLLECTION_SOURCE}")
# # # lignes_rapport.append(f"   • Destination : {DB_NAME}.{COLLECTION_DEST}")
# # # lignes_rapport.append(f"   • Statut      : {'✅ SUCCÈS' if succes else '⚠️ VÉRIFIER'}")
# # # lignes_rapport.append("=" * 70)

# # # rapport_texte = "\n".join(lignes_rapport)
# # # os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# # # with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
# # #     f.write(rapport_texte)
# # # print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

# # # print("\n" + "=" * 70)
# # # print("📊 RÉSUMÉ FINAL")
# # # print("=" * 70)
# # # print(f"   📥 Documents source        : {total_lignes}")
# # # print(f"   📤 Documents normalisés    : {total_inseres}")
# # # print(f"   🚫 Hors scope filtrés      : {total_hors}")
# # # print(f"   ⏱️  Temps total             : {temps_total:.2f}s")
# # # print(f"   🚀 Vitesse                 : {total_lignes/temps_total:.0f} docs/s")
# # # print(f"   📁 Collection dest.        : {DB_NAME}.{COLLECTION_DEST}")
# # # print("=" * 70)
# # # print(f"   Statut : {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")
# # # print("=" * 70)
# # # print("🎉 NORMALISATION TERMINÉE EN MODE MULTI-NŒUDS !")

# # # client_driver.close()
# # # print("🔌 Connexions fermées proprement")

# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-

# # """
# # normalisation_multinode_v5.py
# # ====================================
# # Normaliseur unifié pour corpus télécom DZ (25K commentaires).
# # Version MULTI-NŒUDS avec Spark pour traitement distribué.
# # Intègre TextNormalizer v3.9.5 (code binôme — version single-node).

# # Compatible : AraBERT  |  CAMeL-BERT  |  MarBERT  |  DziriBERT

# # Modes :
# #   "arabert"  → normalisation légère, sans stopwords  (Transformers)
# #   "full"     → normalisation complète + stopwords     (ML classique / stats)

# # ═══════════════════════════════════════════════════════════════
# # Changelog v3.9.5 (binôme) — intégré ici :
# # FIX-45 : Arabizi latin-dominant non convertis (bdlna, khalso, khalsoo,
# #          draham, tbgho, rbnii, haw, chwala) ajoutés dans _EXTRA_ARABIZI_WORDS.
# # FIX-46 : الgaming / الgame → coupure artificielle corrigée via
# #          _step_ar_latin_compounds() exécutée AVANT _step_split_mixed_tokens.
# # FIX-47 : Nouveau paramètre `preserve_latin_darja` pour préserver Darja-latine.
# #          Quand True : TOUS les mots latin-Darja sont gardés en latin.

# # Tous les correctifs antérieurs conservés (FIX-38 à FIX-44).
# # ═══════════════════════════════════════════════════════════════
# # """

# # from __future__ import annotations

# # from pyspark.sql import SparkSession
# # from pymongo import MongoClient, InsertOne
# # from pymongo.errors import BulkWriteError
# # from datetime import datetime
# # import os, time, math, json, re, logging
# # from pathlib import Path
# # from typing import Dict, List, Optional, Set, Tuple
# # from collections import defaultdict

# # logger = logging.getLogger(__name__)

# # # ============================================================
# # # CONFIGURATION
# # # ============================================================
# # MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# # MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# # DB_NAME           = "telecom_algerie"
# # COLLECTION_SOURCE = "commentaires_sans_emojis"
# # COLLECTION_DEST   = "commentaires_normalises"
# # BATCH_SIZE        = 1000
# # NB_WORKERS        = 3
# # SPARK_MASTER      = "spark://spark-master:7077"
# # RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_normalisation_multinode.txt"

# # MASTER_DICT_PATH = Path("/opt/dictionnaires/master_dict.json")
# # RULES_DICT_PATH  = Path("/opt/dictionnaires/linguistic_rules.json")

# # # ============================================================
# # # TEXT NORMALIZER v3.9.5 — FUSION multi-nœuds + binôme
# # # FIX-38 à FIX-47 inclus
# # # ============================================================

# # _RE: Dict[str, re.Pattern] = {
# #     "diacritics":    re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06ED]"),
# #     "tatweel":       re.compile(r"\u0640+"),
# #     "rep_chars":     re.compile(r"(?<!\d)(.)\1{2,}(?!\d)"),
# #     "rep_punct":     re.compile(r"([!?.،:;])\1+"),
# #     "whitespace":    re.compile(r"\s+"),
# #     "digits_only":   re.compile(r"^\d+$"),
# #     "pure_latin":    re.compile(r"^[a-zA-Z'\u2019\-]+$"),
# #     "arabizi_hyb":   re.compile(r"^(?=.*[a-zA-Z])(?=.*(?<=[a-zA-Z])[35679]|[35679](?=[a-zA-Z])).+$"),
# #     "has_arabic":    re.compile(r"[\u0600-\u06FF]"),
# #     "trail_punct":   re.compile(r"^(.*[^!.،,;:؟?])((?:[!.،,;:؟?])+)$"),
# #     "num_arabic":    re.compile(r"^(\d+)([\u0600-\u06FF\u0750-\u077F].*)$"),
# #     "unit_nospace":  re.compile(r"(?<!\w)(\d+)([a-zA-Z/]+(?:ps|/s)?)(?=[\u0600-\u06FF\s,،.!?؟$]|$)", re.IGNORECASE),
# #     "unit_space":    re.compile(r"\b(\d+)\s+([a-zA-Z/]+(?:ps|/s)?)\b", re.IGNORECASE),
# #     "arab_digit":    re.compile(r"([\u0600-\u06FF])(\d)"),
# #     "digit_arab":    re.compile(r"(\d)([\u0600-\u06FF])"),
# #     "spaced_digits": re.compile(r"(?<![:\-\d])(\d)(?: (\d)){1,6}(?![:\-\d])"),
# #     "arabic_digits_spaced": re.compile(r"(?<![٠-٩])([٠-٩])(?: ([٠-٩])){1,6}(?![٠-٩])"),
# #     "arabic_prefix": re.compile(r"^(وال|فال|بال|كال|لل|ال|و|ف|ب|ك|ل)(.+)$"),
# #     "num_separator": re.compile(r"(?<!\d)(\d+)([-:/])(\d+)(?!\d)"),
# #     "ar_then_lat":   re.compile(r"([\u0600-\u06FF])([a-zA-Z])"),
# #     "lat_then_ar":   re.compile(r"([a-zA-Z])([\u0600-\u06FF])"),
# #     "regex_key":     re.compile(r"[\\^$*+?.()\[\]{}|]"),
# #     "wifi_version":  re.compile(r"\bwifi\d+\b", re.IGNORECASE),  # FIX-42
# # }

# # _AR_VERB_PREFIXES: Set[str] = {"ت", "ي", "ن", "أ", "ا", "تت", "يت", "نت", "ست"}
# # _AR_CONJ_PREFIXES: Set[str] = {"و", "ف", "ب", "ك", "ل", "ال", "لل", "بال", "كال", "فال", "وال"}
# # _AR_ALL_PREFIXES: Set[str]  = _AR_VERB_PREFIXES | _AR_CONJ_PREFIXES

# # # FIX-38/43 : clés techniques protégées
# # _ABBREV_KEYS_TO_PROTECT: Set[str] = {
# #     "adsl", "vdsl", "ftth", "fttb", "xgs", "xgs-pon", "pon", "dsl",
# #     "lte", "volte", "dns", "ont", "wifi", "wify", "wi-fi", "wlan",
# #     "4g", "3g", "2g", "5g", "cnx", "rdm",
# #     "mbps", "kbps", "gbps", "mbs", "mb/s", "gb/s", "kb/s",
# #     "idoom", "djezzy", "mobilis", "ooredoo",
# #     "fibre", "fiber", "febre", "ping", "hd", "fhd", "uhd",
# #     "ram", "rom", "cpu", "gpu", "android", "ios",
# #     "4k", "8k", "2k", "netflix", "amazon", "fortnite",
# #     "nokia", "tenda", "zte", "huawei", "xiaomi",
# #     "iptv", "ip80", "ethernet", "gigabit", "starlink",
# #     "reset", "re-provision", "reprovision",
# #     "qoe", "speedtest", "backbone", "throughput",
# #     "routing", "peering", "cdn", "olt", "qos", "jitter", "gpon",
# #     "ms",       # FIX-38
# #     "xg-spon",  # FIX-43
# #     "xg-s-pon", # FIX-43
# # }

# # _ABBREV_AMBIGUOUS_SKIP: Set[str] = {
# #     "re", "co", "da", "an", "ko", "mo", "go", "gb", "mb", "kb",
# #     "bla", "li", "ki", "w", "at", "db", "tt", "pr", "pk", "pq", "nn",
# #     "cc", "cv", "fb", "yt", "ok",
# # }

# # # FIX-39 : composés protégés
# # _PROTECTED_COMPOUNDS: List[str] = [
# #     "my idoom", "my ooredoo", "my djezzy", "my mobilis",
# #     "no transaction", "no transction",
# #     "fibre optique", "core network", "traffic shaping",
# #     "prime video", "prime vidéo", "google tv", "tv box", "twin box", "mi box",
# # ]
# # _COMPOUND_PATTERNS: List[Tuple[re.Pattern, str]] = [
# #     (re.compile(re.escape(c), re.IGNORECASE), c)
# #     for c in sorted(_PROTECTED_COMPOUNDS, key=len, reverse=True)
# # ]

# # # FIX-39/40/44/45 : mots arabizi supplémentaires
# # _EXTRA_ARABIZI_WORDS: Dict[str, str] = {
# #     # ── Religiosité / serments ────────────────────────────────────────────────
# #     "nachalh":    "إن شاء الله", "nchalh":     "إن شاء الله",
# #     "inchalh":    "إن شاء الله", "inshalah":   "إن شاء الله",
# #     "inshalh":    "إن شاء الله", "inshaallah": "إن شاء الله",
# #     "inchalah":   "إن شاء الله", "inshaalah":  "إن شاء الله",
# #     "inchallah":  "إن شاء الله", "inshallah":  "إن شاء الله",
# #     "nchallah":   "إن شاء الله", "nshallah":   "إن شاء الله",
# #     "wlh":    "والله", "wlhi":    "والله", "wellah":  "والله",
# #     "wella":  "والله", "wallah":  "والله", "wallahi": "والله",
# #     "wallhy": "والله",
# #     "flexi":  "فليكسي", "flexili": "فليكسي",
# #     "nflexi": "فليكسي", "yflexi":  "فليكسي",
# #     # ── FIX-39 : "my" seul → préserver ───────────────────────────────────────
# #     "my": "my",
# #     # ── FIX-40 : promotion préservé ───────────────────────────────────────────
# #     "promotion": "promotion",
# #     "promo":     "promo",
# #     # ── FIX-44 : arabizi hybride/pur fréquents ────────────────────────────────
# #     "n9drou":  "نقدرو",  "nkhlsou": "نخلصو",
# #     "nl9a":    "نلقا",   "chhar":   "شهر",
# #     "fibr":    "fibre optique",  "psq":   "parce que",
# #     "ndkhol":  "ندخل",   "nkhles": "نخلص",
# #     "khir":    "خير",    "khorda": "خردة",
# #     "ytl3":    "يطلع",   "boost":  "boost",
# #     "fel":     "في ال",  "balak":  "بالاك",
# #     # ── FIX-45 : arabizi manquants texte latin-dominant ───────────────────────
# #     "bdlna":   "بدّلنا",
# #     "khalso":  "خلصو",
# #     "khalsoo": "خلصو",
# #     "draham":  "دراهم",
# #     "tbgho":   "تبغو",
# #     "rbnii":   "ربّي",
# #     "haw":     "هاو",
# #     "chwala":  "شوالة",
# #     # ── Autres arabizi fréquents ──────────────────────────────────────────────
# #     "khoya":     "خويا",    "khouya":     "خويا",
# #     "dyal":      "ديال",    "dial":       "ديال",
# #     "bhal":      "بحال",    "mazel":      "مزال",
# #     "sahbi":     "صاحبي",   "3andek":     "عندك",
# #     "3andi":     "عندي",    "hadchi":     "هذا الشيء",
# #     "wakha":     "واخا",    "rabi":       "ربي",
# #     "yehdi":     "يهدي",    "yehdikoum":  "يهديكم",
# #     "mn":        "من",      "fi":         "في",
# #     "hdra":      "هدرة",    "khdma":      "خدمة",
# # }

# # _EXTRA_ARABIZI_UPPER: Dict[str, str] = {
# #     "NACHALH":   "إن شاء الله", "NCHALH":    "إن شاء الله",
# #     "INCHALH":   "إن شاء الله", "INSHALAH":  "إن شاء الله",
# #     "INSHALLAH": "إن شاء الله", "INCHALAH":  "إن شاء الله",
# #     "INCHALLAH": "إن شاء الله",
# #     "WLH":    "والله", "WELLAH":  "والله",
# #     "WALLAH": "والله", "WALLAHI": "والله",
# #     # FIX-45 : variantes majuscules
# #     "BDLNA":   "بدّلنا", "KHALSO":  "خلصو",
# #     "KHALSOO": "خلصو",   "DRAHAM":  "دراهم",
# #     "TBGHO":   "تبغو",   "RBNII":   "ربّي",
# #     "HAW":     "هاو",    "CHWALA":  "شوالة",
# # }

# # _EXTRA_AR_PATTERNS: List[Tuple[re.Pattern, str]] = [
# #     (re.compile(r"\bانشاء الله\b"),  "إن شاء الله"),
# #     (re.compile(r"\bنشاالله\b"),     "إن شاء الله"),
# #     (re.compile(r"\bانشالله\b"),     "إن شاء الله"),
# #     (re.compile(r"\bنشالله\b"),      "إن شاء الله"),
# #     (re.compile(r"\bانشاالله\b"),    "إن شاء الله"),
# #     (re.compile(r"\bاشالله\b"),      "إن شاء الله"),
# #     (re.compile(r"\bفكيكسيت\b"),     "فليكسي"),
# #     (re.compile(r"\bفلكسيلي\b"),     "فليكسي"),
# #     (re.compile(r"\bفليكسيلي\b"),    "فليكسي"),
# #     (re.compile(r"\bفليكسيت\b"),     "فليكسي"),
# #     (re.compile(r"\bنفليكسي\b"),     "فليكسي"),
# #     (re.compile(r"\bيفليكسي\b"),     "فليكسي"),
# #     (re.compile(r"\bتفليكسي\b"),     "فليكسي"),
# # ]

# # # ── FIX-46 : composés arabe+latin à résoudre AVANT _step_split_mixed_tokens ───
# # _MIXED_AR_LATIN_COMPOUNDS: List[Tuple[re.Pattern, str]] = [
# #     (re.compile(r"\bالgaming\b",  re.IGNORECASE), "الألعاب"),
# #     (re.compile(r"\bالgame\b",    re.IGNORECASE), "اللعبة"),
# #     (re.compile(r"\bالstream\b",  re.IGNORECASE), "البث المباشر"),
# #     (re.compile(r"\bالlag\b",     re.IGNORECASE), "التأخير"),
# #     (re.compile(r"\bالping\b",    re.IGNORECASE), "ping"),
# #     (re.compile(r"\bالwifi\b",    re.IGNORECASE), "واي فاي"),
# #     (re.compile(r"\bالweb\b",     re.IGNORECASE), "الانترنت"),
# # ]

# # HORS_SCOPE_KEYWORDS: List[str] = [
# #     "embauche", "entretien d'embauche", "recrutement", "offre d'emploi",
# #     "sarl maxim", "hashtag#",
# #     "code du travail", "loi 90-11", "comité de participation",
# #     "عدل3", "عدل 3", "حق_الطعون", "مراجعة_الملفات",
# #     "المقصيون_من_عدل", "الشفافية_في_عدل",
# # ]


# # # ── Fonctions de construction (hors classe) ────────────────────────────────────
# # def _load_rules(rules_path: Path) -> dict:
# #     if not rules_path.exists():
# #         return {}
# #     with open(rules_path, encoding="utf-8") as f:
# #         return json.load(f)

# # def _build_negations(rules: dict) -> Set[str]:
# #     neg = rules.get("negations", {})
# #     result = set(neg.get("arabe_standard", []))
# #     result.update(neg.get("dialecte_algerien", []))
# #     result.update({"ما", "لا", "لم", "لن", "ليس", "غير", "مش", "ميش",
# #                    "مكانش", "ماكش", "ماهيش", "ماهوش"})
# #     return result

# # def _build_dialect_keep(rules: dict) -> Set[str]:
# #     dk = rules.get("dialect_keep", {})
# #     result: Set[str] = set()
# #     for v in dk.values():
# #         if isinstance(v, list):
# #             result.update(v)
# #     result.update({
# #         "راه", "راهي", "راني", "راك", "راهم", "واش", "كيفاش", "وين",
# #         "علاش", "قديش", "مزال", "كيما", "باه", "ديما",
# #         "ya", "pas", "de", "en", "avec", "depuis", "pour",
# #         "par", "sur", "sous", "dans", "entre", "sans", "vers",
# #     })
# #     return result

# # def _build_intentional_repeats(rules: dict) -> Set[str]:
# #     ir = rules.get("intentional_repeats", {})
# #     result: Set[str] = set()
# #     for v in ir.values():
# #         if isinstance(v, list):
# #             result.update(v)
# #     result.update({
# #         "كيف", "شوي", "برا", "هاك", "يا", "آه", "لا", "واو",
# #         "بزاف", "مزال", "كل", "نعم", "جدا",
# #         "très", "trop", "bien", "non", "oui", "si", "jamais", "encore",
# #     })
# #     return result

# # def _build_tech_tokens(rules: dict) -> Set[str]:
# #     tt = rules.get("tech_tokens", {})
# #     result: Set[str] = set()
# #     for v in tt.values():
# #         if isinstance(v, list):
# #             result.update(t.lower() for t in v)
# #     result.discard("cnx")
# #     result.update(_ABBREV_KEYS_TO_PROTECT)
# #     return result

# # def _build_arabic_prefixes(rules: dict) -> List[str]:
# #     ap = rules.get("arabic_prefixes", {})
# #     prefixes = ap.get("prefixes", [])
# #     if not prefixes:
# #         prefixes = ["وال", "فال", "بال", "كال", "لل", "ال", "و", "ف", "ب", "ك", "ل"]
# #     return sorted(prefixes, key=len, reverse=True)

# # def _build_contractions(rules: dict) -> Dict[str, str]:
# #     ct = rules.get("contractions", {})
# #     result = dict(ct.get("francais", {}))
# #     result.update({
# #         "j'ai":  "je ai",   "j\u2019ai":  "je ai",
# #         "c'est": "ce est",  "c\u2019est": "ce est",
# #         "n'est": "ne est",  "n\u2019est": "ne est",
# #         "n'a":   "ne a",    "n\u2019a":   "ne a",
# #         "qu'il": "que il",  "qu\u2019il": "que il",
# #         "qu'on": "que on",  "qu\u2019on": "que on",
# #     })
# #     return result

# # def _build_protected_words(rules: dict) -> Set[str]:
# #     pw = rules.get("protected_words", {})
# #     result: Set[str] = set()
# #     for v in pw.values():
# #         if isinstance(v, list):
# #             result.update(t.lower() for t in v)
# #     result.update(_ABBREV_KEYS_TO_PROTECT)
# #     result.update({
# #         "internet", "connexion", "problème", "réseau", "service",
# #         "optique", "ping", "gaming", "game", "live", "speed",
# #         "high", "low", "lag", "stream",
# #         "facebook", "whatsapp", "youtube", "instagram",
# #         "bonjour", "merci", "salut", "normal", "bravo",
# #         "message", "solution", "compte", "temps",
# #         "même", "comme", "chaque", "alors",
# #         "avant", "depuis", "juste", "vraiment",
# #         "lente", "mois", "plusieurs",
# #         "pas", "on", "ne", "fait", "rien", "tout", "fois", "bien",
# #         "moi", "encore", "niveau",
# #         "promotion", "promo",
# #         "my idoom", "my ooredoo", "my djezzy", "my mobilis",
# #         "no transaction", "no transction",
# #     })
# #     return result

# # def _build_stopwords_from_rules(rules: dict, negations: Set[str], dialect_keep: Set[str]) -> Set[str]:
# #     sw_section = rules.get("stopwords", {})
# #     sw: Set[str] = set()
# #     for v in sw_section.values():
# #         if isinstance(v, list):
# #             sw.update(v)
# #     if not sw:
# #         sw = {
# #             "le", "la", "les", "l", "un", "une", "des", "du", "de", "et",
# #             "ou", "mais", "donc", "car", "ni", "or", "ce", "cet", "cette",
# #             "ces", "mon", "ton", "son", "notre", "votre", "leur", "leurs",
# #             "ma", "ta", "sa", "mes", "tes", "ses",
# #             "je", "tu", "il", "elle", "nous", "vous", "ils", "elles",
# #             "me", "te", "se", "lui", "y", "en", "que", "qui", "quoi", "dont",
# #             "est", "sont", "être", "avoir", "a", "ai",
# #             "avec", "sans", "sur", "sous", "dans", "par", "pour",
# #             "très", "plus", "moins", "aussi", "bien", "tout",
# #             "pas", "ne", "on", "si",
# #             "i", "we", "our", "you", "your", "he", "she", "it",
# #             "its", "they", "their", "am", "is", "are", "was", "were",
# #             "have", "has", "had", "do", "does", "did",
# #             "will", "would", "could", "should", "a", "an", "the",
# #             "and", "or", "but", "if", "in", "on", "at", "by", "for",
# #             "with", "to", "from", "not", "no",
# #             "إلى", "عن", "مع", "كان", "كانت", "هذا", "هذه", "ذلك", "تلك",
# #             "هو", "هي", "هم", "هن", "ثم", "أو", "إن", "إذا", "لو", "قد",
# #             "لكن", "بل", "حتى", "ضد", "أن", "التي", "الذي", "الذين",
# #         }
# #     try:
# #         from nltk.corpus import stopwords as _sw
# #         sw.update(_sw.words("arabic"))
# #     except Exception:
# #         pass
# #     return sw - negations - dialect_keep

# # def _compile_dict(d: dict, flags: int = re.UNICODE) -> List[Tuple[re.Pattern, str]]:
# #     result = []
# #     for pat, repl in d.items():
# #         try:
# #             result.append((re.compile(pat, flags), repl))
# #         except re.error:
# #             pass
# #     return result

# # def _build_contraction_re(contractions: Dict[str, str]) -> re.Pattern:
# #     apostrophe_keys = [k for k in contractions if "'" in k or "\u2019" in k]
# #     if not apostrophe_keys:
# #         return re.compile(r"(?!)")
# #     escaped = sorted([re.escape(k) for k in apostrophe_keys], key=len, reverse=True)
# #     return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)

# # def _build_plain_dict(raw: dict) -> Dict[str, str]:
# #     return {k: v for k, v in raw.items() if not _RE["regex_key"].search(k)}


# # # ============================================================
# # # CLASSE TextNormalizer v3.9.5
# # # ============================================================
# # class TextNormalizer:
# #     """
# #     TextNormalizer v3.9.5 — compatible Spark (FIX-38 à FIX-47 inclus)

# #     Paramètres
# #     ----------
# #     mode : "arabert" | "full"
# #     dict_path : chemin vers master_dict.json
# #     rules_path : chemin vers linguistic_rules.json
# #     remove_stopwords : force la suppression des stopwords (sinon géré par mode)
# #     disable_arabizi : désactive la conversion arabizi → arabe
# #     preserve_latin_darja : FIX-47 — conserve toute la Darja-latine en l'état
# #     """

# #     def __init__(
# #         self,
# #         mode: str = "arabert",
# #         dict_path: Path = MASTER_DICT_PATH,
# #         rules_path: Path = RULES_DICT_PATH,
# #         remove_stopwords: bool = False,
# #         disable_arabizi: bool = False,
# #         preserve_latin_darja: bool = False,  # ✅ FIX-47
# #     ):
# #         assert mode in ("arabert", "full"), "mode doit être 'arabert' ou 'full'"
# #         self.mode                 = mode
# #         self.remove_sw            = remove_stopwords or (mode == "full")
# #         self.disable_arabizi      = disable_arabizi
# #         self.preserve_latin_darja = preserve_latin_darja  # ✅ FIX-47

# #         with open(dict_path, encoding="utf-8") as f:
# #             d = json.load(f)

# #         rules = _load_rules(rules_path)

# #         self._negations          = _build_negations(rules)
# #         self._dialect_keep       = _build_dialect_keep(rules)
# #         self._intentional_rep    = _build_intentional_repeats(rules)
# #         self._tech_tokens        = _build_tech_tokens(rules)
# #         self._arabic_prefixes    = _build_arabic_prefixes(rules)
# #         self._contractions       = _build_contractions(rules)
# #         self._contraction_re     = _build_contraction_re(self._contractions)

# #         self.unicode_map: Dict[str, str] = d["unicode_arabic"]
# #         self.unicode_map.setdefault("\u0629", "\u0647")
# #         self.unicode_map.setdefault("\u0624", "\u0648")

# #         self.digrams: Dict[str, str] = d["arabizi_digrams"]
# #         self.monograms: Dict[str, str] = {
# #             k: v for k, v in d["arabizi_monograms"].items()
# #             if not (len(k) == 1 and k.isalpha())
# #         }

# #         raw_arabizi = d["arabizi_words"]
# #         self.arabizi_words: Dict[str, str] = {**raw_arabizi, **_EXTRA_ARABIZI_WORDS}
# #         self.arabizi_words = {k: v for k, v in self.arabizi_words.items()
# #                               if not k.startswith("_")}

# #         self.arabizi_upper: Dict[str, str] = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}
# #         self.emojis:        Dict[str, str] = d["emojis"]
# #         self.abbreviations: Dict[str, str] = d["abbreviations"]
# #         self.telecom: Dict[str, str]       = _build_plain_dict(d["telecom_tech"])
# #         self.units_map: Dict[str, str]     = d["units"]

# #         nv = d["network_variants"]
# #         self._net_form: str       = nv["normalized_form"]
# #         self._net_all:  List[str] = nv["latin"] + nv["arabic"]

# #         mixed_clean = {k: v for k, v in d["mixed_ar_fr_regex"].items()
# #                        if not k.startswith("_")}
# #         self._mixed_pats: List[Tuple[re.Pattern, str]] = _compile_dict(mixed_clean)
# #         self._fr_pats:    List[Tuple[re.Pattern, str]] = _compile_dict(
# #             d["french_corrections_regex"], flags=re.IGNORECASE | re.UNICODE
# #         )

# #         escaped_net = [re.escape(v) for v in self._net_all if v]
# #         self._net_re = re.compile(
# #             rf'\b({"|".join(escaped_net)})\b', re.IGNORECASE
# #         ) if escaped_net else None

# #         combined = {**self.digrams, **self.monograms}
# #         self._arabizi_seq: List[Tuple[str, str]] = sorted(
# #             combined.items(), key=lambda x: len(x[0]), reverse=True
# #         )
# #         self._arabizi_upper_sorted: List[Tuple[str, str]] = sorted(
# #             self.arabizi_upper.items(), key=lambda x: len(x[0]), reverse=True
# #         )

# #         _all_vals: List[str] = []
# #         for v in list(self.telecom.values()) + list(self.abbreviations.values()):
# #             if v:
# #                 _all_vals.extend(v.split())

# #         self._protected: Set[str] = _build_protected_words(rules)
# #         self._protected.update(t.lower() for t in _all_vals)
# #         self._protected.update(self._tech_tokens)

# #         self._stopwords: Set[str] = (
# #             _build_stopwords_from_rules(rules, self._negations, self._dialect_keep)
# #             if self.remove_sw else set()
# #         )

# #     # ── Helpers ───────────────────────────────────────────────────────────────
# #     def _split_prefix(self, word: str) -> Tuple[str, str]:
# #         for p in self._arabic_prefixes:
# #             if word.startswith(p) and len(word) > len(p) + 1:
# #                 return p, word[len(p):]
# #         return "", word

# #     def _lookup(self, word: str, dct: Dict[str, str]) -> Optional[str]:
# #         lo = word.lower()
# #         v  = dct.get(lo) or dct.get(word)
# #         if v is not None:
# #             return v
# #         pref, root = self._split_prefix(word)
# #         if pref:
# #             v = dct.get(root.lower()) or dct.get(root)
# #             if v is not None:
# #                 return v if " " in v else pref + v
# #         return None

# #     def _is_latin_dominant(self, text: str) -> bool:
# #         lat = sum(1 for c in text if "a" <= c.lower() <= "z")
# #         ar  = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
# #         tot = lat + ar
# #         if not tot:
# #             return False
# #         threshold = 0.70 if len(text) < 30 else 0.75
# #         return (lat / tot) > threshold

# #     @staticmethod
# #     def _is_proper_noun_token(tok: str) -> bool:
# #         """FIX-41 : heuristique nom propre."""
# #         if len(tok) < 3 or not _RE["pure_latin"].match(tok):
# #             return False
# #         return tok[0].isupper() and not tok.isupper()

# #     @staticmethod
# #     def _is_hors_scope(text: str) -> bool:
# #         if not text:
# #             return False
# #         lo = text.lower()
# #         return any(kw.lower() in lo for kw in HORS_SCOPE_KEYWORDS)

# #     def _protect_numbers(self, text: str) -> Tuple[Dict[str, str], str]:
# #         protected: Dict[str, str] = {}
# #         counter = [0]
# #         def _protect(m: re.Match) -> str:
# #             key = f"__NP{counter[0]}__"
# #             counter[0] += 1
# #             protected[key] = m.group(0)
# #             return key
# #         text = _RE["num_separator"].sub(_protect, text)
# #         text = re.sub(r"(?<!\d)(\d+)\s*/\s*(\d+)(?!\d)", _protect, text)
# #         return protected, text

# #     def _protect_compounds_pre(self, text: str) -> Tuple[Dict[str, str], str]:
# #         """FIX-39 : protège les composés avant tout le pipeline."""
# #         placeholders: Dict[str, str] = {}
# #         counter = [0]
# #         for pat, original in _COMPOUND_PATTERNS:
# #             if pat.search(text):
# #                 ph = f"__CPD{counter[0]}__"
# #                 counter[0] += 1
# #                 placeholders[ph] = original
# #                 text = pat.sub(ph, text)
# #         return placeholders, text

# #     def _dedup_tokens(self, text: str) -> str:
# #         tokens = text.split()
# #         if len(tokens) < 2:
# #             return text
# #         result = [tokens[0]]
# #         for i in range(1, len(tokens)):
# #             prev, curr       = result[-1], tokens[i]
# #             prev_lo, curr_lo = prev.lower(), curr.lower()
# #             if curr_lo == prev_lo:
# #                 if curr_lo in self._intentional_rep:
# #                     result.append(curr)
# #                 continue
# #             has_ar_curr = bool(_RE["has_arabic"].search(curr))
# #             if (not has_ar_curr and len(curr) >= 4
# #                     and len(curr) < len(prev) and prev_lo.endswith(curr_lo)):
# #                 continue
# #             if (has_ar_curr and len(curr) >= 5
# #                     and len(curr) < len(prev) and prev.endswith(curr)):
# #                 m = _RE["arabic_prefix"].match(prev)
# #                 if m and m.group(2) == curr:
# #                     result.append(curr); continue
# #                 stripped = prev[:len(prev)-len(curr)]
# #                 if stripped in _AR_ALL_PREFIXES and len(stripped) >= 2:
# #                     result.append(curr); continue
# #                 result.append(curr); continue
# #             result.append(curr)
# #         return " ".join(result)

# #     # ── Pipeline principal ────────────────────────────────────────────────────
# #     def normalize(self, text: str) -> str:
# #         if not isinstance(text, str) or not text.strip():
# #             return ""
# #         try:
# #             protected_nums, text = self._protect_numbers(text)
# #             protected_cpd,  text = self._protect_compounds_pre(text)

# #             text = self._step_emojis(text)
# #             text = self._step_unicode_arabic(text)
# #             text = self._step_extra_ar(text)
# #             text = self._step_ar_latin_compounds(text)          # FIX-46 (NOUVEAU)
# #             text = _RE["wifi_version"].sub(                     # FIX-42
# #                 lambda m: m.group(0).lower(), text
# #             )
# #             for pat, repl in self._mixed_pats:
# #                 text = pat.sub(repl, text)
# #             text = self._step_french(text)
# #             text = self._step_abbrev(text)
# #             text = self._step_units(text)
# #             text = self._step_split_mixed_tokens(text)
# #             text = self._step_arabizi(text)
# #             text = self._step_cleanup(text)
# #             text = self._dedup_tokens(text)
# #             if self.remove_sw:
# #                 text = self._step_stopwords(text)

# #             for ph, original in protected_cpd.items():
# #                 text = text.replace(ph, original)
# #             for key, val in protected_nums.items():
# #                 text = text.replace(key, val)

# #         except Exception:
# #             return ""
# #         return text.strip()

# #     # ── Étapes individuelles ──────────────────────────────────────────────────
# #     def _step_emojis(self, text: str) -> str:
# #         for emoji, word in self.emojis.items():
# #             if emoji in text:
# #                 text = text.replace(emoji, f" {word} ")
# #         return text

# #     def _step_extra_ar(self, text: str) -> str:
# #         for pat, repl in _EXTRA_AR_PATTERNS:
# #             text = pat.sub(repl, text)
# #         return text

# #     def _step_ar_latin_compounds(self, text: str) -> str:
# #         """
# #         FIX-46 : normalise les composés arabe+latin collés (ex: الgaming)
# #         AVANT que _step_split_mixed_tokens ne les découpe artificiellement.
# #         """
# #         for pat, repl in _MIXED_AR_LATIN_COMPOUNDS:
# #             text = pat.sub(repl, text)
# #         return text

# #     def _step_units(self, text: str) -> str:
# #         def _repl(m: re.Match) -> str:
# #             num, unit = m.group(1), m.group(2).lower()
# #             KEEP_ATTACHED = {"ms", "h", "s"}
# #             if unit in KEEP_ATTACHED:
# #                 return f"{num}{unit}"
# #             if len(unit) == 1 and unit not in self.units_map:
# #                 return m.group(0)
# #             norm = self.units_map.get(unit)
# #             if norm is None:
# #                 return m.group(0)
# #             return f"{num} {norm}"
# #         text = _RE["unit_space"].sub(_repl, text)
# #         text = _RE["unit_nospace"].sub(_repl, text)
# #         return text

# #     def _step_abbrev(self, text: str) -> str:
# #         latin_dom      = self._is_latin_dominant(text)
# #         tokens, result = text.split(), []
# #         i = 0
# #         while i < len(tokens):
# #             tok  = tokens[i]
# #             m    = _RE["trail_punct"].match(tok)
# #             core, trail = (m.group(1), m.group(2)) if m else (tok, "")
# #             lo_core = core.lower()

# #             if core.startswith("__CPD") and core.endswith("__"):
# #                 result.append(tok); i += 1; continue
# #             if core in self._negations:
# #                 result.append(tok); i += 1; continue
# #             if (not _RE["has_arabic"].search(core)
# #                     and not _RE["digits_only"].match(core)
# #                     and lo_core in self.arabizi_words):
# #                 result.append(tok); i += 1; continue
# #             if lo_core in self._tech_tokens:
# #                 result.append(tok); i += 1; continue

# #             mn = _RE["num_arabic"].match(core)
# #             if mn:
# #                 num, unit = mn.groups()
# #                 repl = self._lookup(unit, self.telecom) or unit
# #                 if " " in repl:
# #                     result += [num] + repl.split()[:-1] + [repl.split()[-1] + trail]
# #                 else:
# #                     result += [num, repl + trail]
# #                 i += 1; continue

# #             resolved = False
# #             for dct in (self.telecom, self.abbreviations):
# #                 if (dct is self.abbreviations
# #                         and lo_core in _ABBREV_AMBIGUOUS_SKIP
# #                         and latin_dom):
# #                     continue
# #                 repl = self._lookup(core, dct)
# #                 if repl is not None:
# #                     if (latin_dom and dct is self.telecom
# #                             and not _RE["has_arabic"].search(core)
# #                             and len(core) <= 4
# #                             and lo_core not in _ABBREV_KEYS_TO_PROTECT):
# #                         break
# #                     if " " in repl:
# #                         parts = repl.split()
# #                         result += parts[:-1] + [parts[-1] + trail]
# #                     else:
# #                         result.append(repl + trail)
# #                     resolved = True
# #                     break

# #             if not resolved:
# #                 if self._net_re and self._net_re.fullmatch(core):
# #                     result.append(self._net_form + trail)
# #                 else:
# #                     result.append(tok)
# #             i += 1
# #         return " ".join(result)

# #     def _step_french(self, text: str) -> str:
# #         text = self._contraction_re.sub(
# #             lambda m: self._contractions.get(m.group(0).lower(), m.group(0)), text
# #         )
# #         for pat, repl in self._fr_pats:
# #             text = pat.sub(repl, text)
# #         return text

# #     def _step_split_mixed_tokens(self, text: str) -> str:
# #         text = _RE["ar_then_lat"].sub(r"\1 \2", text)
# #         text = _RE["lat_then_ar"].sub(r"\1 \2", text)
# #         return text

# #     def _step_arabizi(self, text: str) -> str:
# #         """
# #         ✅ FIX-47 : si preserve_latin_darja=True, tous les tokens latins/arabizi
# #         sont conservés tels quels (aucune conversion vers l'arabe).
# #         """
# #         latin_dom = self._is_latin_dominant(text)
# #         result    = []
# #         for tok in text.split():
# #             lo = tok.lower()

# #             if tok.startswith("__CPD") and tok.endswith("__"):
# #                 result.append(tok); continue
# #             if lo in self._tech_tokens:
# #                 result.append(tok); continue
# #             if _RE["has_arabic"].search(tok):
# #                 result.append(tok); continue
# #             if _RE["digits_only"].match(tok):
# #                 result.append(tok); continue
# #             if _RE["num_arabic"].match(tok):
# #                 result.append(tok); continue

# #             if _RE["pure_latin"].match(tok):
# #                 _AMBIGUOUS_SHORT = {"ki", "el", "da", "li", "w", "dz"}

# #                 # ✅ FIX-47 : preserve_latin_darja → conserve en latin
# #                 if self.preserve_latin_darja:
# #                     result.append(tok); continue

# #                 w = self.arabizi_words.get(lo)
# #                 if w:
# #                     if latin_dom and len(lo) <= 2 and lo in _AMBIGUOUS_SHORT:
# #                         result.append(tok)
# #                     else:
# #                         result.append(w)
# #                     continue
# #                 for k, v in self._arabizi_upper_sorted:
# #                     if tok.upper() == k:
# #                         result.append(v); break
# #                 else:
# #                     if self._is_proper_noun_token(tok):  # FIX-41
# #                         result.append(tok); continue
# #                     if self.disable_arabizi or latin_dom:
# #                         result.append(tok); continue
# #                     result.append(self._arabizi_convert(tok))
# #                 continue

# #             if _RE["arabizi_hyb"].match(tok):
# #                 # ✅ FIX-47
# #                 if self.preserve_latin_darja:
# #                     result.append(tok); continue

# #                 w = self.arabizi_words.get(lo)
# #                 if w:
# #                     result.append(w); continue
# #                 for k, v in self._arabizi_upper_sorted:
# #                     if tok.upper() == k:
# #                         result.append(v); break
# #                 else:
# #                     if self.disable_arabizi or latin_dom:
# #                         result.append(tok)
# #                     else:
# #                         result.append(self._arabizi_convert(tok))
# #                 continue

# #             result.append(tok)
# #         return " ".join(result)

# #     def _arabizi_convert(self, token: str) -> str:
# #         for k, v in self._arabizi_upper_sorted:
# #             if token.upper() == k:
# #                 return v
# #         result = token.lower()
# #         for extra, ar in [("ee", "ي"), ("ii", "ي"), ("oo", "و"), ("pp", "ب")]:
# #             result = result.replace(extra, ar)
# #         for seq, ar in self._arabizi_seq:
# #             result = result.replace(seq, ar)
# #         result = re.sub(r"[a-z]", "", result)
# #         return result

# #     def _step_unicode_arabic(self, text: str) -> str:
# #         text = _RE["diacritics"].sub("", text)
# #         text = _RE["tatweel"].sub("", text)
# #         for variant, canonical in self.unicode_map.items():
# #             text = text.replace(variant, canonical)
# #         return text

# #     def _step_cleanup(self, text: str) -> str:
# #         text = _RE["arab_digit"].sub(r"\1 \2", text)
# #         text = _RE["digit_arab"].sub(r"\1 \2", text)
# #         def _fuse_spaced(m: re.Match) -> str:
# #             return m.group(0).replace(" ", "")
# #         text = _RE["spaced_digits"].sub(_fuse_spaced, text)
# #         text = _RE["arabic_digits_spaced"].sub(_fuse_spaced, text)
# #         text = _RE["rep_chars"].sub(lambda m: m.group(1) * 2, text)
# #         text = _RE["rep_punct"].sub(lambda m: m.group(1), text)
# #         text = _RE["whitespace"].sub(" ", text).strip()
# #         return text

# #     def _step_stopwords(self, text: str) -> str:
# #         if not self._stopwords:
# #             return text
# #         placeholders: Dict[str, str] = {}
# #         for i, compound in enumerate(_PROTECTED_COMPOUNDS):
# #             pattern = re.compile(re.escape(compound), re.IGNORECASE)
# #             if pattern.search(text):
# #                 ph = f"__SWC{i}__"
# #                 placeholders[ph] = compound
# #                 text = pattern.sub(ph, text)
# #         keep   = self._negations | self._dialect_keep
# #         result = []
# #         for w in text.split():
# #             lo = w.lower()
# #             if w.startswith("__") and w.endswith("__"):
# #                 result.append(w); continue
# #             if (w.isdigit() or w in keep or lo in keep
# #                     or lo not in self._stopwords):
# #                 result.append(w)
# #             elif len(w) == 1 and _RE["has_arabic"].match(w):
# #                 result.append(w)
# #         text = " ".join(result)
# #         for ph, original in placeholders.items():
# #             text = text.replace(ph, original)
# #         return text

# #     # ── UDFs Spark ────────────────────────────────────────────────────────────
# #     def spark_udf(self):
# #         from pyspark.sql.functions import udf
# #         from pyspark.sql.types import StringType
# #         norm = self
# #         @udf(returnType=StringType())
# #         def _udf(text):
# #             return norm.normalize(text) if text else ""
# #         return _udf

# #     def spark_hors_scope_udf(self):
# #         from pyspark.sql.functions import udf
# #         from pyspark.sql.types import BooleanType
# #         @udf(returnType=BooleanType())
# #         def _udf(text):
# #             return TextNormalizer._is_hors_scope(text) if text else False
# #         return _udf


# # # ============================================================
# # # FONCTIONS SPARK DISTRIBUÉES
# # # ============================================================

# # def normaliser_partition(partition):
# #     import sys
# #     sys.path.insert(0, '/opt/pymongo_libs')
# #     from pymongo import MongoClient, InsertOne
# #     from pymongo.errors import BulkWriteError

# #     try:
# #         normalizer = TextNormalizer(
# #             mode="arabert",
# #             dict_path=MASTER_DICT_PATH,
# #             rules_path=RULES_DICT_PATH,
# #         )
# #         client     = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
# #         db         = client[DB_NAME]
# #         collection = db[COLLECTION_DEST]
# #     except Exception as e:
# #         yield {"_erreur": str(e), "statut": "connexion_failed"}
# #         return

# #     batch              = []
# #     docs_traites       = 0
# #     docs_masques       = 0   # Filtre 1 — commentaire_moderateur == "masquer"
# #     docs_hors          = 0   # Filtre 2 — hors-scope télécom
# #     docs_vides_apres   = 0   # Filtre 3 — vides après normalisation

# #     for row in partition:
# #         texte_original = row.get("Commentaire_Client", "") or ""

# #         # ── Filtre 1 : masqués par le modérateur ─────────────────────────────
# #         moderateur_val = (row.get("commentaire_moderateur", "") or "").strip().lower()
# #         if moderateur_val == "masquer":
# #             docs_masques += 1
# #             continue

# #         # ── Filtre 2 : hors-scope télécom ────────────────────────────────────
# #         if TextNormalizer._is_hors_scope(texte_original):
# #             docs_hors += 1
# #             continue

# #         commentaire_normalise = normalizer.normalize(texte_original)
# #         moderateur_normalise  = normalizer.normalize(row.get("commentaire_moderateur", "") or "")

# #         # ── Filtre 3 : vides après normalisation ─────────────────────────────
# #         if not commentaire_normalise.strip():
# #             docs_vides_apres += 1
# #             continue

# #         doc = {
# #             "_id"                    : row.get("_id"),
# #             "Commentaire_Client"     : commentaire_normalise,
# #             "commentaire_moderateur" : moderateur_normalise,
# #             "date"                   : row.get("date"),
# #             "source"                 : row.get("source"),
# #             "moderateur"             : row.get("moderateur"),
# #             "metadata"               : row.get("metadata"),
# #             "statut"                 : row.get("statut"),
# #         }
# #         batch.append(InsertOne(doc))
# #         docs_traites += 1

# #         if len(batch) >= BATCH_SIZE:
# #             try:
# #                 collection.bulk_write(batch, ordered=False)
# #             except BulkWriteError:
# #                 pass
# #             batch = []

# #     if batch:
# #         try:
# #             collection.bulk_write(batch, ordered=False)
# #         except BulkWriteError:
# #             pass

# #     client.close()
# #     yield {
# #         "docs_traites"     : docs_traites,
# #         "docs_masques"     : docs_masques,
# #         "docs_hors_scope"  : docs_hors,
# #         "docs_vides_apres" : docs_vides_apres,
# #         "statut"           : "ok",
# #     }


# # def lire_partition_depuis_mongo(partition_info):
# #     import sys
# #     sys.path.insert(0, '/opt/pymongo_libs')
# #     from pymongo import MongoClient

# #     for item in partition_info:
# #         client     = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
# #         db         = client[DB_NAME]
# #         collection = db[COLLECTION_SOURCE]
# #         curseur    = collection.find(
# #             {},
# #             {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
# #              "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
# #         ).skip(item["skip"]).limit(item["limit"])
# #         for doc in curseur:
# #             doc["_id"] = str(doc["_id"])
# #             yield doc
# #         client.close()


# # # ============================================================
# # # PIPELINE PRINCIPAL
# # # ============================================================
# # temps_debut_global = time.time()

# # print("=" * 70)
# # print("🔤 NORMALISATION MULTI-NŒUDS v3.9.5")
# # print("   FIX-38 : ms protégé (unité latence) ✅")
# # print("   FIX-39 : my idoom/djezzy/mobilis/ooredoo ✅")
# # print("   FIX-40 : بروموسيو → promotion ✅")
# # print("   FIX-41 : noms propres préservés ✅")
# # print("   FIX-42 : wifi5/wifi6/wifi7 protégé ✅")
# # print("   FIX-43 : XG-SPON protégé ✅")
# # print("   FIX-44 : arabizi hybride converti ✅")
# # print("   FIX-45 : arabizi latin-dom (bdlna, khalso...) ✅")
# # print("   FIX-46 : الgaming/الgame/الstream corrigés ✅")
# # print("   FIX-47 : preserve_latin_darja disponible ✅")
# # print(f"   Source : {COLLECTION_SOURCE}")
# # print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# # print("=" * 70)

# # # 1. CONNEXION MONGODB DRIVER
# # print("\n📂 Connexion MongoDB (Driver)...")
# # try:
# #     client_driver = MongoClient(MONGO_URI_DRIVER)
# #     db_driver     = client_driver[DB_NAME]
# #     coll_source   = db_driver[COLLECTION_SOURCE]
# #     total_docs    = coll_source.count_documents({})
# #     print(f"✅ MongoDB connecté — {total_docs} documents dans la source")
# # except Exception as e:
# #     print(f"❌ Erreur MongoDB : {e}")
# #     exit(1)

# # # 2. CONNEXION SPARK
# # print("\n⚡️ Connexion au cluster Spark...")
# # temps_debut_spark = time.time()
# # spark = SparkSession.builder \
# #     .appName("Normalisation_MultiNode_v395") \
# #     .master(SPARK_MASTER) \
# #     .config("spark.executor.memory", "2g") \
# #     .config("spark.executor.cores", "2") \
# #     .config("spark.sql.shuffle.partitions", "4") \
# #     .getOrCreate()
# # spark.sparkContext.setLogLevel("WARN")
# # temps_fin_spark = time.time()
# # print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")

# # # 3. LECTURE DISTRIBUÉE
# # print("\n📥 LECTURE DISTRIBUÉE...")
# # temps_debut_chargement = time.time()
# # docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# # plages = [
# #     {"skip": i * docs_par_worker,
# #      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
# #     for i in range(NB_WORKERS)
# # ]
# # for idx, p in enumerate(plages):
# #     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

# # rdd_data = spark.sparkContext \
# #     .parallelize(plages, NB_WORKERS) \
# #     .mapPartitions(lire_partition_depuis_mongo)

# # df_spark = spark.read.json(rdd_data.map(
# #     lambda d: json.dumps(
# #         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
# #          for k, v in d.items()}
# #     )
# # ))
# # total_lignes = df_spark.count()
# # temps_fin_chargement = time.time()
# # print(f"✅ {total_lignes} documents chargés en {temps_fin_chargement - temps_debut_chargement:.2f}s")

# # # 4. VIDER DESTINATION
# # coll_dest = db_driver[COLLECTION_DEST]
# # coll_dest.delete_many({})
# # print("\n🧹 Collection destination vidée")

# # # 5. NORMALISATION DISTRIBUÉE
# # print("\n💾 NORMALISATION DISTRIBUÉE...")
# # temps_debut_traitement = time.time()

# # rdd_stats = df_spark.rdd \
# #     .map(lambda row: row.asDict()) \
# #     .mapPartitions(normaliser_partition)

# # stats_ecriture     = rdd_stats.collect()
# # ok_stats           = [s for s in stats_ecriture if s.get("statut") == "ok"]
# # total_inseres      = sum(s.get("docs_traites",      0) for s in ok_stats)
# # total_masques      = sum(s.get("docs_masques",      0) for s in ok_stats)
# # total_hors         = sum(s.get("docs_hors_scope",   0) for s in ok_stats)
# # total_vides_apres  = sum(s.get("docs_vides_apres",  0) for s in ok_stats)
# # total_filtres      = total_masques + total_hors + total_vides_apres
# # erreurs            = [s for s in stats_ecriture if "_erreur" in s]

# # temps_fin_traitement = time.time()
# # print(f"✅ Normalisation terminée en {temps_fin_traitement - temps_debut_traitement:.2f}s")
# # print(f"📦 {total_inseres} documents normalisés")
# # print(f"🔽 {total_filtres} documents filtrés au total :")
# # print(f"   Filtre 1 — masqués modérateur      : {total_masques}")
# # print(f"   Filtre 2 — hors-scope télécom      : {total_hors}")
# # print(f"   Filtre 3 — vides après norm.       : {total_vides_apres}")
# # if erreurs:
# #     for e in erreurs:
# #         print(f"   ⚠️  {e.get('_erreur')}")

# # spark.stop()

# # # 6. VÉRIFICATION FINALE
# # print("\n🔎 VÉRIFICATION FINALE...")
# # total_en_dest = coll_dest.count_documents({})
# # succes        = total_en_dest == total_inseres

# # print(f"   • Documents source         : {total_lignes}")
# # print(f"   • Documents normalisés     : {total_inseres}")
# # print(f"   • Filtre 1 — masqués modérateur      : {total_masques}")
# # print(f"   • Filtre 2 — hors-scope télécom      : {total_hors}")
# # print(f"   • Filtre 3 — vides après norm.       : {total_vides_apres}")
# # print(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")

# # # 7. RAPPORT
# # temps_fin_global = time.time()
# # temps_total      = temps_fin_global - temps_debut_global

# # lignes_rapport = []
# # lignes_rapport.append("=" * 70)
# # lignes_rapport.append("🔤 NORMALISATION MULTI-NŒUDS v3.9.5")
# # lignes_rapport.append(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# # lignes_rapport.append(f"   Mode : Spark 4.1.1 | {NB_WORKERS} Workers | MongoDB direct")
# # lignes_rapport.append("=" * 70)
# # lignes_rapport.append("\nCORRECTIFS APPLIQUÉS :")
# # lignes_rapport.append("   FIX-38 : ms protégé comme unité de latence")
# # lignes_rapport.append("   FIX-39 : my idoom/djezzy/mobilis/ooredoo protégé")
# # lignes_rapport.append("   FIX-40 : بروموسيو → promotion conservé")
# # lignes_rapport.append("   FIX-41 : noms propres latins préservés")
# # lignes_rapport.append("   FIX-42 : wifi5/wifi6/wifi7 protégé")
# # lignes_rapport.append("   FIX-43 : XG-SPON protégé")
# # lignes_rapport.append("   FIX-44 : arabizi hybride/pur converti (n9drou, nkhlsou...)")
# # lignes_rapport.append("   FIX-45 : arabizi manquants texte latin-dom (bdlna, khalso...)")
# # lignes_rapport.append("   FIX-46 : الgaming/الgame/الstream/الlag/الping/الwifi/الweb corrigés")
# # lignes_rapport.append("   FIX-47 : paramètre preserve_latin_darja disponible")
# # lignes_rapport.append(f"\n📊 RÉSULTATS :")
# # lignes_rapport.append(f"   ┌────────────────────────────────────────────┐")
# # lignes_rapport.append(f"   │ Documents source          : {total_lignes:<15} │")
# # lignes_rapport.append(f"   │ Documents normalisés      : {total_inseres:<15} │")
# # lignes_rapport.append(f"   ├────────────────────────────────────────────┤")
# # lignes_rapport.append(f"   │ Filtre 1 — masqués modér. : {total_masques:<15} │")
# # lignes_rapport.append(f"   │ Filtre 2 — hors-scope     : {total_hors:<15} │")
# # lignes_rapport.append(f"   │ Filtre 3 — vides ap. norm.: {total_vides_apres:<15} │")
# # lignes_rapport.append(f"   │ Total filtré              : {total_filtres:<15} │")
# # lignes_rapport.append(f"   ├────────────────────────────────────────────┤")
# # lignes_rapport.append(f"   │ Taux de succès            : {total_inseres/total_lignes*100:<14.2f}% │")
# # lignes_rapport.append(f"   └────────────────────────────────────────────┘")
# # lignes_rapport.append(f"\n⏱️  TEMPS :")
# # lignes_rapport.append(f"   • Connexion Spark  : {temps_fin_spark - temps_debut_spark:.2f}s")
# # lignes_rapport.append(f"   • Chargement       : {temps_fin_chargement - temps_debut_chargement:.2f}s")
# # lignes_rapport.append(f"   • Normalisation    : {temps_fin_traitement - temps_debut_traitement:.2f}s")
# # lignes_rapport.append(f"   • TOTAL            : {temps_total:.2f}s ({total_lignes/temps_total:.0f} doc/s)")
# # lignes_rapport.append(f"\n📁 STOCKAGE :")
# # lignes_rapport.append(f"   • Source      : {DB_NAME}.{COLLECTION_SOURCE}")
# # lignes_rapport.append(f"   • Destination : {DB_NAME}.{COLLECTION_DEST}")
# # lignes_rapport.append(f"   • Statut      : {'✅ SUCCÈS' if succes else '⚠️ VÉRIFIER'}")
# # lignes_rapport.append("=" * 70)

# # rapport_texte = "\n".join(lignes_rapport)
# # os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# # with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
# #     f.write(rapport_texte)
# # print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

# # print("\n" + "=" * 70)
# # print("📊 RÉSUMÉ FINAL")
# # print("=" * 70)
# # print(f"   📥 Documents source        : {total_lignes}")
# # print(f"   📤 Documents normalisés    : {total_inseres}")
# # print(f"   🔽 Total filtré            : {total_filtres}")
# # print(f"      ├─ Filtre 1 — masqués modérateur : {total_masques}")
# # print(f"      ├─ Filtre 2 — hors-scope télécom : {total_hors}")
# # print(f"      └─ Filtre 3 — vides après norm.  : {total_vides_apres}")
# # print(f"   ⏱️  Temps total             : {temps_total:.2f}s")
# # print(f"   🚀 Vitesse                 : {total_lignes/temps_total:.0f} docs/s")
# # print(f"   📁 Collection dest.        : {DB_NAME}.{COLLECTION_DEST}")
# # print("=" * 70)
# # print(f"   Statut : {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")
# # print("=" * 70)
# # print("🎉 NORMALISATION TERMINÉE EN MODE MULTI-NŒUDS !")

# # client_driver.close()
# # print("🔌 Connexions fermées proprement")

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# normalisation_multinode_v5.py
# ====================================
# Normaliseur unifié pour corpus télécom DZ (26K commentaires).
# Version MULTI-NŒUDS avec Spark pour traitement distribué.
# Intègre TextNormalizer v3.9.5 (code binôme).

# Compatibles : AraBERT | CAMeL-BERT | MarBERT | DziriBERT

# Modes :
#   "arabert"  → normalisation légère, sans stopwords  (Transformers)
#   "full"     → normalisation complète + stopwords     (ML classique / stats)

# FIX-47 : Ajout du paramètre preserve_latin_darja pour préserver la Darja latine.
# """

# from pyspark.sql import SparkSession
# from pymongo import MongoClient, InsertOne
# from pymongo.errors import BulkWriteError
# from datetime import datetime
# import os, time, math, json, re, logging
# from pathlib import Path
# from typing import Dict, List, Optional, Set, Tuple
# from collections import defaultdict

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
# MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
# DB_NAME           = "telecom_algerie"
# COLLECTION_SOURCE = "commentaires_sans_emojis"   
# COLLECTION_DEST   = "commentaires_normalises"
# BATCH_SIZE        = 1000
# NB_WORKERS        = 3
# SPARK_MASTER      = "spark://spark-master:7077"
# RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_normalisation_multinode_v5.txt"

# MASTER_DICT_PATH = Path("/opt/dictionnaires/master_dict.json")
# RULES_DICT_PATH  = Path("/opt/dictionnaires/linguistic_rules.json")

# # Paramètres de normalisation (à modifier selon vos besoins)
# PRESERVE_LATIN_DARJA = True   # ✅ FIX-47 : Garder la Darja en latin (slm, khouya, etc.)
# DISABLE_ARABIZI = False       # Désactiver la conversion arabe (si True, preserve_latin_darja est ignoré)
# MODE = "arabert"              # "arabert" ou "full"
# REMOVE_STOPWORDS = False      # Supprimer les stopwords (force True si MODE="full")

# # ============================================================
# # TEXT NORMALIZER v3.9.5 (code binôme — TOUS LES FIX inclus)
# # ============================================================

# _RE: Dict[str, re.Pattern] = {
#     "diacritics":    re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06ED]"),
#     "tatweel":       re.compile(r"\u0640+"),
#     "rep_chars":     re.compile(r"(?<!\d)(.)\1{2,}(?!\d)"),
#     "rep_punct":     re.compile(r"([!?.،:;])\1+"),
#     "whitespace":    re.compile(r"\s+"),
#     "digits_only":   re.compile(r"^\d+$"),
#     "pure_latin":    re.compile(r"^[a-zA-Z'\u2019\-]+$"),
#     "arabizi_hyb":   re.compile(r"^(?=.*[a-zA-Z])(?=.*(?<=[a-zA-Z])[35679]|[35679](?=[a-zA-Z])).+$"),
#     "has_arabic":    re.compile(r"[\u0600-\u06FF]"),
#     "trail_punct":   re.compile(r"^(.*[^!.،,;:؟?])((?:[!.،,;:؟?])+)$"),
#     "num_arabic":    re.compile(r"^(\d+)([\u0600-\u06FF\u0750-\u077F].*)$"),
#     "unit_nospace":  re.compile(r"(?<!\w)(\d+)([a-zA-Z/]+(?:ps|/s)?)(?=[\u0600-\u06FF\s,،.!?؟$]|$)", re.IGNORECASE),
#     "unit_space":    re.compile(r"\b(\d+)\s+([a-zA-Z/]+(?:ps|/s)?)\b", re.IGNORECASE),
#     "arab_digit":    re.compile(r"([\u0600-\u06FF])(\d)"),
#     "digit_arab":    re.compile(r"(\d)([\u0600-\u06FF])"),
#     "spaced_digits": re.compile(r"(?<![:\-\d])(\d)(?: (\d)){1,6}(?![:\-\d])"),
#     "arabic_digits_spaced": re.compile(r"(?<![٠-٩])([٠-٩])(?: ([٠-٩])){1,6}(?![٠-٩])"),
#     "arabic_prefix": re.compile(r"^(وال|فال|بال|كال|لل|ال|و|ف|ب|ك|ل)(.+)$"),
#     "num_separator": re.compile(r"(?<!\d)(\d+)([-:/])(\d+)(?!\d)"),
#     "ar_then_lat":   re.compile(r"([\u0600-\u06FF])([a-zA-Z])"),
#     "lat_then_ar":   re.compile(r"([a-zA-Z])([\u0600-\u06FF])"),
#     "regex_key":     re.compile(r"[\\^$*+?.()\[\]{}|]"),
#     "wifi_version":  re.compile(r"\bwifi\d+\b", re.IGNORECASE),  # FIX-42
# }

# _AR_VERB_PREFIXES: Set[str] = {"ت", "ي", "ن", "أ", "ا", "تت", "يت", "نت", "ست"}
# _AR_CONJ_PREFIXES: Set[str] = {"و", "ف", "ب", "ك", "ل", "ال", "لل", "بال", "كال", "فال", "وال"}
# _AR_ALL_PREFIXES: Set[str]  = _AR_VERB_PREFIXES | _AR_CONJ_PREFIXES

# # Clés techniques protégées
# _ABBREV_KEYS_TO_PROTECT: Set[str] = {
#     "adsl", "vdsl", "ftth", "fttb", "xgs", "xgs-pon", "pon", "dsl",
#     "lte", "volte", "dns", "ont", "wifi", "wify", "wi-fi", "wlan",
#     "4g", "3g", "2g", "5g", "cnx", "rdm",
#     "mbps", "kbps", "gbps", "mbs", "mb/s", "gb/s", "kb/s",
#     "idoom", "djezzy", "mobilis", "ooredoo",
#     "fibre", "fiber", "febre", "ping", "hd", "fhd", "uhd",
#     "ram", "rom", "cpu", "gpu", "android", "ios",
#     "4k", "8k", "2k", "netflix", "amazon", "fortnite",
#     "nokia", "tenda", "zte", "huawei", "xiaomi",
#     "iptv", "ip80", "ethernet", "gigabit", "starlink",
#     "reset", "re-provision", "reprovision",
#     "qoe", "speedtest", "backbone", "throughput",
#     "routing", "peering", "cdn", "olt", "qos", "jitter", "gpon",
#     "ms",       # FIX-38
#     "xg-spon",  # FIX-43
#     "xg-s-pon", # FIX-43
# }

# _ABBREV_AMBIGUOUS_SKIP: Set[str] = {
#     "re", "co", "da", "an", "ko", "mo", "go", "gb", "mb", "kb",
#     "bla", "li", "ki", "w", "at", "db", "tt", "pr", "pk", "pq", "nn",
#     "cc", "cv", "fb", "yt", "ok",
# }

# # Composés protégés
# _PROTECTED_COMPOUNDS: List[str] = [
#     "my idoom", "my ooredoo", "my djezzy", "my mobilis",
#     "no transaction", "no transction",
#     "fibre optique", "core network", "traffic shaping",
#     "prime video", "prime vidéo", "google tv", "tv box", "twin box", "mi box",
# ]

# # Mots arabizi supplémentaires
# _EXTRA_ARABIZI_WORDS: Dict[str, str] = {
#     "nachalh": "إن شاء الله", "nchalh": "إن شاء الله",
#     "inchalh": "إن شاء الله", "inshalah": "إن شاء الله",
#     "inshalh": "إن شاء الله", "inshaallah": "إن شاء الله",
#     "inchalah": "إن شاء الله", "inshaalah": "إن شاء الله",
#     "inchallah": "إن شاء الله", "inshallah": "إن شاء الله",
#     "nchallah": "إن شاء الله", "nshallah": "إن شاء الله",
#     "wlh": "والله", "wlhi": "والله", "wellah": "والله",
#     "wella": "والله", "wallah": "والله", "wallahi": "والله",
#     "wallhy": "والله",
#     "flexi": "فليكسي", "flexili": "فليكسي",
#     "nflexi": "فليكسي", "yflexi": "فليكسي",
#     "my": "my",
#     "promotion": "promotion",
#     "promo": "promo",
#     # FIX-44
#     "n9drou": "نقدرو", "nkhlsou": "نخلصو", "nl9a": "نلقا",
#     "chhar": "شهر", "fibr": "fibre optique", "psq": "parce que",
#     "ndkhol": "ندخل", "nkhles": "نخلص",
#     "khir": "خير", "khorda": "خردة",
#     "ytl3": "يطلع", "boost": "boost", "fel": "في ال", "balak": "بالاك",
#     # FIX-45
#     "bdlna": "بدّلنا", "khalso": "خلصو", "khalsoo": "خلصو",
#     "draham": "دراهم", "tbgho": "تبغو", "rbnii": "ربّي",
#     "haw": "هاو", "chwala": "شوالة",
#     "khoya": "خويا", "khouya": "خويا", "dyal": "ديال", "dial": "ديال",
#     "bhal": "بحال", "mazel": "مزال", "sahbi": "صاحبي",
#     "3andek": "عندك", "3andi": "عندي", "hadchi": "هذا الشيء",
#     "wakha": "واخا", "rabi": "ربي", "yehdi": "يهدي",
#     "yehdikoum": "يهديكم", "mn": "من", "fi": "في",
#     "hdra": "هدرة", "khdma": "خدمة",
# }

# _EXTRA_ARABIZI_UPPER: Dict[str, str] = {
#     "NACHALH": "إن شاء الله", "NCHALH": "إن شاء الله",
#     "INCHALH": "إن شاء الله", "INSHALAH": "إن شاء الله",
#     "INSHALLAH": "إن شاء الله", "INCHALAH": "إن شاء الله",
#     "INCHALLAH": "إن شاء الله",
#     "WLH": "والله", "WELLAH": "والله", "WALLAH": "والله", "WALLAHI": "والله",
#     "BDLNA": "بدّلنا", "KHALSO": "خلصو", "KHALSOO": "خلصو",
#     "DRAHAM": "دراهم", "TBGHO": "تبغو", "RBNII": "ربّي",
#     "HAW": "هاو", "CHWALA": "شوالة",
# }

# _EXTRA_AR_PATTERNS: List[Tuple[re.Pattern, str]] = [
#     (re.compile(r"\bانشاء الله\b"),  "إن شاء الله"),
#     (re.compile(r"\bنشاالله\b"),     "إن شاء الله"),
#     (re.compile(r"\bانشالله\b"),     "إن شاء الله"),
#     (re.compile(r"\bنشالله\b"),      "إن شاء الله"),
#     (re.compile(r"\bانشاالله\b"),    "إن شاء الله"),
#     (re.compile(r"\bاشالله\b"),      "إن شاء الله"),
#     (re.compile(r"\bفكيكسيت\b"),     "فليكسي"),
#     (re.compile(r"\bفلكسيلي\b"),     "فليكسي"),
#     (re.compile(r"\bفليكسيلي\b"),    "فليكسي"),
#     (re.compile(r"\bفليكسيت\b"),     "فليكسي"),
#     (re.compile(r"\bنفليكسي\b"),     "فليكسي"),
#     (re.compile(r"\bيفليكسي\b"),     "فليكسي"),
#     (re.compile(r"\bتفليكسي\b"),     "فليكسي"),
# ]

# # FIX-46 : composés arabe+latin
# _MIXED_AR_LATIN_COMPOUNDS: List[Tuple[re.Pattern, str]] = [
#     (re.compile(r"\bالgaming\b", re.IGNORECASE), "الألعاب"),
#     (re.compile(r"\bالgame\b",   re.IGNORECASE), "اللعبة"),
#     (re.compile(r"\bالstream\b", re.IGNORECASE), "البث المباشر"),
#     (re.compile(r"\bالlag\b",    re.IGNORECASE), "التأخير"),
#     (re.compile(r"\bالping\b",   re.IGNORECASE), "ping"),
#     (re.compile(r"\bالwifi\b",   re.IGNORECASE), "واي فاي"),
#     (re.compile(r"\bالweb\b",    re.IGNORECASE), "الانترنت"),
# ]

# HORS_SCOPE_KEYWORDS: List[str] = [
#     "embauche", "entretien d'embauche", "recrutement", "offre d'emploi",
#     "sarl maxim", "hashtag#",
#     "code du travail", "loi 90-11", "comité de participation",
#     "عدل3", "عدل 3", "حق_الطعون", "مراجعة_الملفات",
#     "المقصيون_من_عدل", "الشفافية_في_عدل",
# ]

# # Compilation des patterns de composés protégés
# _COMPOUND_PATTERNS: List[Tuple[re.Pattern, str]] = [
#     (re.compile(re.escape(c), re.IGNORECASE), c)
#     for c in sorted(_PROTECTED_COMPOUNDS, key=len, reverse=True)
# ]

# # ============================================================
# # FONCTIONS DE CONSTRUCTION DES RÈGLES
# # ============================================================
# def _load_rules(rules_path):
#     if not rules_path.exists():
#         return {}
#     with open(rules_path, encoding="utf-8") as f:
#         return json.load(f)

# def _build_negations(rules):
#     neg = rules.get("negations", {})
#     result = set(neg.get("arabe_standard", []))
#     result.update(neg.get("dialecte_algerien", []))
#     result.update({"ما", "لا", "لم", "لن", "ليس", "غير", "مش", "ميش",
#                    "مكانش", "ماكش", "ماهيش", "ماهوش"})
#     return result

# def _build_dialect_keep(rules):
#     dk = rules.get("dialect_keep", {})
#     result = set()
#     for v in dk.values():
#         if isinstance(v, list):
#             result.update(v)
#     result.update({"راه", "راهي", "راني", "راك", "راهم", "واش", "كيفاش", "وين",
#                    "علاش", "قديش", "مزال", "كيما", "باه", "ديما",
#                    "ya", "pas", "de", "en", "avec", "depuis", "pour",
#                    "par", "sur", "sous", "dans", "entre", "sans", "vers"})
#     return result

# def _build_intentional_repeats(rules):
#     ir = rules.get("intentional_repeats", {})
#     result = set()
#     for v in ir.values():
#         if isinstance(v, list):
#             result.update(v)
#     result.update({"كيف", "شوي", "برا", "هاك", "يا", "آه", "لا", "واو",
#                    "بزاف", "مزال", "كل", "نعم", "جدا",
#                    "très", "trop", "bien", "non", "oui", "si", "jamais", "encore"})
#     return result

# def _build_tech_tokens(rules):
#     tt = rules.get("tech_tokens", {})
#     result = set()
#     for v in tt.values():
#         if isinstance(v, list):
#             result.update(t.lower() for t in v)
#     result.discard("cnx")
#     result.update(_ABBREV_KEYS_TO_PROTECT)
#     return result

# def _build_arabic_prefixes(rules):
#     ap = rules.get("arabic_prefixes", {})
#     prefixes = ap.get("prefixes", [])
#     if not prefixes:
#         prefixes = ["وال", "فال", "بال", "كال", "لل", "ال", "و", "ف", "ب", "ك", "ل"]
#     return sorted(prefixes, key=len, reverse=True)

# def _build_contractions(rules):
#     ct = rules.get("contractions", {})
#     result = dict(ct.get("francais", {}))
#     result.update({
#         "j'ai": "je ai", "j\u2019ai": "je ai",
#         "c'est": "ce est", "c\u2019est": "ce est",
#         "n'est": "ne est", "n\u2019est": "ne est",
#         "n'a": "ne a", "n\u2019a": "ne a",
#         "qu'il": "que il", "qu\u2019il": "que il",
#         "qu'on": "que on", "qu\u2019on": "que on",
#     })
#     return result

# def _is_regex_key(k: str) -> bool:
#     return bool(_RE["regex_key"].search(k))

# def _build_plain_dict(raw: dict) -> Dict[str, str]:
#     return {k: v for k, v in raw.items() if not _is_regex_key(k)}

# def _build_protected_words(rules):
#     pw = rules.get("protected_words", {})
#     result = set()
#     for v in pw.values():
#         if isinstance(v, list):
#             result.update(t.lower() for t in v)
#     result.update(_ABBREV_KEYS_TO_PROTECT)
#     result.update({
#         "internet", "connexion", "problème", "réseau", "service",
#         "optique", "ping", "gaming", "game", "live", "speed",
#         "high", "low", "lag", "stream",
#         "facebook", "whatsapp", "youtube", "instagram",
#         "bonjour", "merci", "salut", "normal", "bravo",
#         "message", "solution", "compte", "temps",
#         "même", "comme", "chaque", "alors",
#         "avant", "depuis", "juste", "vraiment",
#         "lente", "mois", "plusieurs",
#         "pas", "on", "ne", "fait", "rien", "tout", "fois", "bien",
#         "moi", "encore", "niveau",
#         "promotion", "promo",
#         "my idoom", "my ooredoo", "my djezzy", "my mobilis",
#         "no transaction", "no transction",
#     })
#     return result

# def _build_stopwords_from_rules(rules, negations, dialect_keep):
#     sw_section = rules.get("stopwords", {})
#     sw = set()
#     for v in sw_section.values():
#         if isinstance(v, list):
#             sw.update(v)
#     if not sw:
#         sw = {
#             "le", "la", "les", "l", "un", "une", "des", "du", "de", "et",
#             "ou", "mais", "donc", "car", "ni", "or", "ce", "cet", "cette",
#             "ces", "mon", "ton", "son", "notre", "votre", "leur", "leurs",
#             "ma", "ta", "sa", "mes", "tes", "ses",
#             "je", "tu", "il", "elle", "nous", "vous", "ils", "elles",
#             "me", "te", "se", "lui", "y", "en", "que", "qui", "quoi", "dont",
#             "est", "sont", "être", "avoir", "a", "ai",
#             "avec", "sans", "sur", "sous", "dans", "par", "pour",
#             "très", "plus", "moins", "aussi", "bien", "tout",
#             "pas", "ne", "on", "si",
#             "i", "my", "we", "our", "you", "your", "he", "she", "it",
#             "its", "they", "their", "am", "is", "are", "was", "were",
#             "have", "has", "had", "do", "does", "did",
#             "will", "would", "could", "should", "a", "an", "the",
#             "and", "or", "but", "if", "in", "on", "at", "by", "for",
#             "with", "to", "from", "not", "no",
#             "إلى", "عن", "مع", "كان", "كانت", "هذا", "هذه", "ذلك", "تلك",
#             "هو", "هي", "هم", "هن", "ثم", "أو", "إن", "إذا", "لو", "قد",
#             "لكن", "بل", "حتى", "ضد", "أن", "التي", "الذي", "الذين",
#         }
#     try:
#         from nltk.corpus import stopwords as _sw
#         sw.update(_sw.words("arabic"))
#     except Exception:
#         pass
#     return sw - negations - dialect_keep

# def _build_contraction_re(contractions):
#     apostrophe_keys = [k for k in contractions if "'" in k or "\u2019" in k]
#     if not apostrophe_keys:
#         return re.compile(r"(?!)")
#     escaped = sorted([re.escape(k) for k in apostrophe_keys], key=len, reverse=True)
#     return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)


# class TextNormalizer:
#     """TextNormalizer v3.9.5 — compatible Spark (TOUS LES FIX inclus)"""

#     def __init__(
#         self,
#         mode="arabert",
#         dict_path=None,
#         rules_path=None,
#         remove_stopwords=False,
#         disable_arabizi=False,
#         preserve_latin_darja=False,   # ✅ FIX-47
#     ):
#         assert mode in ("arabert", "full")
#         self.mode = mode
#         self.remove_sw = remove_stopwords or (mode == "full")
#         self.disable_arabizi = disable_arabizi
#         self.preserve_latin_darja = preserve_latin_darja   # ✅ FIX-47

#         dict_path = dict_path or MASTER_DICT_PATH
#         rules_path = rules_path or RULES_DICT_PATH

#         with open(dict_path, encoding="utf-8") as f:
#             d = json.load(f)

#         rules = _load_rules(rules_path)

#         self._negations = _build_negations(rules)
#         self._dialect_keep = _build_dialect_keep(rules)
#         self._intentional_rep = _build_intentional_repeats(rules)
#         self._tech_tokens = _build_tech_tokens(rules)
#         self._arabic_prefixes = _build_arabic_prefixes(rules)
#         self._contractions = _build_contractions(rules)
#         self._contraction_re = _build_contraction_re(self._contractions)

#         self.unicode_map = d["unicode_arabic"]
#         self.unicode_map.setdefault("\u0629", "\u0647")
#         self.unicode_map.setdefault("\u0624", "\u0648")

#         self.digrams = d["arabizi_digrams"]
#         self.monograms = {
#             k: v for k, v in d["arabizi_monograms"].items()
#             if not (len(k) == 1 and k.isalpha())
#         }

#         raw_arabizi = d["arabizi_words"]
#         self.arabizi_words = {**raw_arabizi, **_EXTRA_ARABIZI_WORDS}
#         self.arabizi_words = {k: v for k, v in self.arabizi_words.items()
#                               if not k.startswith("_")}

#         self.arabizi_upper = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}
#         self.emojis = d["emojis"]
#         self.abbreviations = d["abbreviations"]
#         self.telecom = _build_plain_dict(d["telecom_tech"])
#         self.units_map = d["units"]

#         nv = d["network_variants"]
#         self._net_form = nv["normalized_form"]
#         self._net_all = nv["latin"] + nv["arabic"]

#         mixed_clean = {k: v for k, v in d["mixed_ar_fr_regex"].items()
#                        if not k.startswith("_")}
#         self._mixed_pats = self._compile_dict(mixed_clean)
#         self._fr_pats = self._compile_dict(
#             d["french_corrections_regex"],
#             flags=re.IGNORECASE | re.UNICODE
#         )

#         escaped_net = [re.escape(v) for v in self._net_all if v]
#         self._net_re = re.compile(
#             rf'\b({"|".join(escaped_net)})\b', re.IGNORECASE
#         ) if escaped_net else None

#         combined = {**self.digrams, **self.monograms}
#         self._arabizi_seq = sorted(combined.items(), key=lambda x: len(x[0]), reverse=True)
#         self._arabizi_upper_sorted = sorted(self.arabizi_upper.items(), key=lambda x: len(x[0]), reverse=True)

#         _all_vals = []
#         for v in list(self.telecom.values()) + list(self.abbreviations.values()):
#             if v:
#                 _all_vals.extend(v.split())

#         self._protected = _build_protected_words(rules)
#         self._protected.update(t.lower() for t in _all_vals)
#         self._protected.update(self._tech_tokens)

#         self._stopwords = (
#             _build_stopwords_from_rules(rules, self._negations, self._dialect_keep)
#             if self.remove_sw else set()
#         )

#     @staticmethod
#     def _compile_dict(d, flags=re.UNICODE):
#         result = []
#         for pat, repl in d.items():
#             try:
#                 result.append((re.compile(pat, flags), repl))
#             except re.error:
#                 pass
#         return result

#     def _split_prefix(self, word):
#         for p in self._arabic_prefixes:
#             if word.startswith(p) and len(word) > len(p) + 1:
#                 return p, word[len(p):]
#         return "", word

#     def _lookup(self, word, dct):
#         lo = word.lower()
#         v = dct.get(lo) or dct.get(word)
#         if v is not None:
#             return v
#         pref, root = self._split_prefix(word)
#         if pref:
#             v = dct.get(root.lower()) or dct.get(root)
#             if v is not None:
#                 return v if " " in v else pref + v
#         return None

#     def _is_latin_dominant(self, text):
#         lat = sum(1 for c in text if "a" <= c.lower() <= "z")
#         ar = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
#         tot = lat + ar
#         if not tot:
#             return False
#         threshold = 0.70 if len(text) < 30 else 0.75
#         return (lat / tot) > threshold

#     @staticmethod
#     def _is_proper_noun_token(tok):
#         """FIX-41"""
#         if len(tok) < 3 or not _RE["pure_latin"].match(tok):
#             return False
#         return tok[0].isupper() and not tok.isupper()

#     @staticmethod
#     def _is_hors_scope(text):
#         if not text:
#             return False
#         lo = text.lower()
#         return any(kw.lower() in lo for kw in HORS_SCOPE_KEYWORDS)

#     def _protect_numbers(self, text):
#         protected = {}
#         counter = [0]
#         def _protect(m):
#             key = f"__NP{counter[0]}__"
#             counter[0] += 1
#             protected[key] = m.group(0)
#             return key
#         text = _RE["num_separator"].sub(_protect, text)
#         text = re.sub(r"(?<!\d)(\d+)\s*/\s*(\d+)(?!\d)", _protect, text)
#         return protected, text

#     def _protect_compounds_pre(self, text):
#         """FIX-39"""
#         placeholders = {}
#         counter = [0]
#         for pat, original in _COMPOUND_PATTERNS:
#             if pat.search(text):
#                 ph = f"__CPD{counter[0]}__"
#                 counter[0] += 1
#                 placeholders[ph] = original
#                 text = pat.sub(ph, text)
#         return placeholders, text

#     def _dedup_tokens(self, text):
#         tokens = text.split()
#         if len(tokens) < 2:
#             return text
#         result = [tokens[0]]
#         for i in range(1, len(tokens)):
#             prev, curr = result[-1], tokens[i]
#             prev_lo, curr_lo = prev.lower(), curr.lower()
#             if curr_lo == prev_lo:
#                 if curr_lo in self._intentional_rep:
#                     result.append(curr)
#                 continue
#             has_ar_curr = bool(_RE["has_arabic"].search(curr))
#             if (not has_ar_curr and len(curr) >= 4
#                     and len(curr) < len(prev) and prev_lo.endswith(curr_lo)):
#                 continue
#             if (has_ar_curr and len(curr) >= 5
#                     and len(curr) < len(prev) and prev.endswith(curr)):
#                 m = _RE["arabic_prefix"].match(prev)
#                 if m and m.group(2) == curr:
#                     result.append(curr); continue
#                 stripped = prev[:len(prev)-len(curr)]
#                 if stripped in _AR_ALL_PREFIXES and len(stripped) >= 2:
#                     result.append(curr); continue
#                 result.append(curr); continue
#             result.append(curr)
#         return " ".join(result)

#     def normalize(self, text):
#         if not isinstance(text, str) or not text.strip():
#             return ""
#         try:
#             protected_nums, text = self._protect_numbers(text)
#             protected_cpd, text = self._protect_compounds_pre(text)

#             text = self._step_emojis(text)
#             text = self._step_unicode_arabic(text)
#             text = self._step_extra_ar(text)

#             # FIX-46 : résoudre les composés ar+latin AVANT la coupure
#             text = self._step_ar_latin_compounds(text)

#             text = _RE["wifi_version"].sub(lambda m: m.group(0).lower(), text)

#             for pat, repl in self._mixed_pats:
#                 text = pat.sub(repl, text)
#             text = self._step_french(text)
#             text = self._step_abbrev(text)
#             text = self._step_units(text)
#             text = self._step_split_mixed_tokens(text)
#             text = self._step_arabizi(text)
#             text = self._step_cleanup(text)
#             text = self._dedup_tokens(text)
#             if self.remove_sw:
#                 text = self._step_stopwords(text)

#             for ph, original in protected_cpd.items():
#                 text = text.replace(ph, original)
#             for key, val in protected_nums.items():
#                 text = text.replace(key, val)

#         except Exception:
#             return ""
#         return text.strip()

#     def _step_emojis(self, text):
#         for emoji, word in self.emojis.items():
#             if emoji in text:
#                 text = text.replace(emoji, f" {word} ")
#         return text

#     def _step_extra_ar(self, text):
#         for pat, repl in _EXTRA_AR_PATTERNS:
#             text = pat.sub(repl, text)
#         return text

#     def _step_ar_latin_compounds(self, text):
#         """FIX-46"""
#         for pat, repl in _MIXED_AR_LATIN_COMPOUNDS:
#             text = pat.sub(repl, text)
#         return text

#     def _step_units(self, text):
#         def _repl(m):
#             num, unit = m.group(1), m.group(2).lower()
#             KEEP_ATTACHED = {"ms", "h", "s"}
#             if unit in KEEP_ATTACHED:
#                 return f"{num}{unit}"
#             if len(unit) == 1 and unit not in self.units_map:
#                 return m.group(0)
#             norm = self.units_map.get(unit)
#             if norm is None:
#                 return m.group(0)
#             return f"{num} {norm}"
#         text = _RE["unit_space"].sub(_repl, text)
#         text = _RE["unit_nospace"].sub(_repl, text)
#         return text

#     def _step_abbrev(self, text):
#         latin_dom = self._is_latin_dominant(text)
#         tokens, result = text.split(), []
#         i = 0
#         while i < len(tokens):
#             tok = tokens[i]
#             m = _RE["trail_punct"].match(tok)
#             core, trail = (m.group(1), m.group(2)) if m else (tok, "")
#             lo_core = core.lower()

#             if core.startswith("__CPD") and core.endswith("__"):
#                 result.append(tok); i += 1; continue
#             if core in self._negations:
#                 result.append(tok); i += 1; continue
#             if (not _RE["has_arabic"].search(core)
#                     and not _RE["digits_only"].match(core)
#                     and lo_core in self.arabizi_words):
#                 result.append(tok); i += 1; continue
#             if lo_core in self._tech_tokens:
#                 result.append(tok); i += 1; continue

#             mn = _RE["num_arabic"].match(core)
#             if mn:
#                 num, unit = mn.groups()
#                 repl = self._lookup(unit, self.telecom) or unit
#                 if " " in repl:
#                     result += [num] + repl.split()[:-1] + [repl.split()[-1] + trail]
#                 else:
#                     result += [num, repl + trail]
#                 i += 1; continue

#             resolved = False
#             for dct in (self.telecom, self.abbreviations):
#                 if (dct is self.abbreviations
#                         and lo_core in _ABBREV_AMBIGUOUS_SKIP
#                         and latin_dom):
#                     continue
#                 repl = self._lookup(core, dct)
#                 if repl is not None:
#                     if (latin_dom and dct is self.telecom
#                             and not _RE["has_arabic"].search(core)
#                             and len(core) <= 4
#                             and lo_core not in _ABBREV_KEYS_TO_PROTECT):
#                         break
#                     if " " in repl:
#                         parts = repl.split()
#                         result += parts[:-1] + [parts[-1] + trail]
#                     else:
#                         result.append(repl + trail)
#                     resolved = True
#                     break

#             if not resolved:
#                 if self._net_re and self._net_re.fullmatch(core):
#                     result.append(self._net_form + trail)
#                 else:
#                     result.append(tok)
#             i += 1
#         return " ".join(result)

#     def _step_french(self, text):
#         text = self._contraction_re.sub(
#             lambda m: self._contractions.get(m.group(0).lower(), m.group(0)), text)
#         for pat, repl in self._fr_pats:
#             text = pat.sub(repl, text)
#         return text

#     def _step_split_mixed_tokens(self, text):
#         text = _RE["ar_then_lat"].sub(r"\1 \2", text)
#         text = _RE["lat_then_ar"].sub(r"\1 \2", text)
#         return text

#     def _step_arabizi(self, text):
#         latin_dom = self._is_latin_dominant(text)
#         result = []
#         for tok in text.split():
#             lo = tok.lower()

#             # Protections existantes
#             if tok.startswith("__CPD") and tok.endswith("__"):
#                 result.append(tok); continue
#             if lo in self._tech_tokens:
#                 result.append(tok); continue
#             if _RE["has_arabic"].search(tok):
#                 result.append(tok); continue
#             if _RE["digits_only"].match(tok):
#                 result.append(tok); continue
#             if _RE["num_arabic"].match(tok):
#                 result.append(tok); continue

#             if _RE["pure_latin"].match(tok):
#                 _AMBIGUOUS_SHORT = {"ki", "el", "da", "li", "w", "dz"}

#                 # ✅ FIX-47 : preserve_latin_darja → SAUTE TOUS les lookups Arabizi
#                 if self.preserve_latin_darja:
#                     result.append(tok)
#                     continue

#                 w = self.arabizi_words.get(lo)
#                 if w:
#                     if latin_dom and len(lo) <= 2 and lo in _AMBIGUOUS_SHORT:
#                         result.append(tok)
#                     else:
#                         result.append(w)
#                     continue

#                 for k, v in self._arabizi_upper_sorted:
#                     if tok.upper() == k:
#                         result.append(v); break
#                 else:
#                     if self._is_proper_noun_token(tok):
#                         result.append(tok); continue
#                     if self.disable_arabizi or latin_dom:
#                         result.append(tok); continue
#                     result.append(self._arabizi_convert(tok))
#                 continue

#             if _RE["arabizi_hyb"].match(tok):
#                 # ✅ FIX-47 : preserve_latin_darja → garde token original
#                 if self.preserve_latin_darja:
#                     result.append(tok)
#                     continue

#                 w = self.arabizi_words.get(lo)
#                 if w:
#                     result.append(w); continue
#                 for k, v in self._arabizi_upper_sorted:
#                     if tok.upper() == k:
#                         result.append(v); break
#                 else:
#                     if self.disable_arabizi or latin_dom:
#                         result.append(tok)
#                     else:
#                         result.append(self._arabizi_convert(tok))
#                 continue

#             result.append(tok)
#         return " ".join(result)

#     def _arabizi_convert(self, token):
#         for k, v in self._arabizi_upper_sorted:
#             if token.upper() == k:
#                 return v
#         result = token.lower()
#         for extra, ar in [("ee", "ي"), ("ii", "ي"), ("oo", "و"), ("pp", "ب")]:
#             result = result.replace(extra, ar)
#         for seq, ar in self._arabizi_seq:
#             result = result.replace(seq, ar)
#         result = re.sub(r"[a-z]", "", result)
#         return result

#     def _step_unicode_arabic(self, text):
#         text = _RE["diacritics"].sub("", text)
#         text = _RE["tatweel"].sub("", text)
#         for variant, canonical in self.unicode_map.items():
#             text = text.replace(variant, canonical)
#         return text

#     def _step_cleanup(self, text):
#         text = _RE["arab_digit"].sub(r"\1 \2", text)
#         text = _RE["digit_arab"].sub(r"\1 \2", text)
#         def _fuse_spaced(m):
#             return m.group(0).replace(" ", "")
#         text = _RE["spaced_digits"].sub(_fuse_spaced, text)
#         text = _RE["arabic_digits_spaced"].sub(_fuse_spaced, text)
#         text = _RE["rep_chars"].sub(lambda m: m.group(1) * 2, text)
#         text = _RE["rep_punct"].sub(lambda m: m.group(1), text)
#         text = _RE["whitespace"].sub(" ", text).strip()
#         return text

#     def _step_stopwords(self, text):
#         if not self._stopwords:
#             return text
#         placeholders = {}
#         for i, compound in enumerate(_PROTECTED_COMPOUNDS):
#             pattern = re.compile(re.escape(compound), re.IGNORECASE)
#             if pattern.search(text):
#                 ph = f"__SWC{i}__"
#                 placeholders[ph] = compound
#                 text = pattern.sub(ph, text)
#         keep = self._negations | self._dialect_keep
#         result = []
#         for w in text.split():
#             lo = w.lower()
#             if w.startswith("__") and w.endswith("__"):
#                 result.append(w); continue
#             if (w.isdigit() or w in keep or lo in keep
#                     or lo not in self._stopwords):
#                 result.append(w)
#             elif len(w) == 1 and _RE["has_arabic"].match(w):
#                 result.append(w)
#         text = " ".join(result)
#         for ph, original in placeholders.items():
#             text = text.replace(ph, original)
#         return text


# # ============================================================
# # FONCTIONS SPARK DISTRIBUÉES
# # ============================================================

# def normaliser_partition(partition):
#     import sys
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient, InsertOne
#     from pymongo.errors import BulkWriteError

#     try:
#         normalizer = TextNormalizer(
#             mode=MODE,
#             remove_stopwords=REMOVE_STOPWORDS,
#             disable_arabizi=DISABLE_ARABIZI,
#             preserve_latin_darja=PRESERVE_LATIN_DARJA,   # ✅ FIX-47
#         )
#         client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
#         db = client[DB_NAME]
#         collection = db[COLLECTION_DEST]
#     except Exception as e:
#         yield {"_erreur": str(e), "statut": "connexion_failed"}
#         return

#     batch = []
#     docs_traites = 0
#     docs_hors = 0

#     for row in partition:
#         texte_original = row.get("Commentaire_Client", "") or ""

#         if TextNormalizer._is_hors_scope(texte_original):
#             docs_hors += 1
#             continue

#         # Normalisation des deux champs textuels
#         commentaire_normalise = normalizer.normalize(texte_original)
#         moderateur_normalise = normalizer.normalize(row.get("commentaire_moderateur", "") or "")

#         # Construction du document à insérer en conservant TOUTES les colonnes
#         doc = {
#             "_id": row.get("_id"),
#             "Commentaire_Client": commentaire_normalise,
#             "commentaire_moderateur": moderateur_normalise,
#         }
        
#         # Ajouter toutes les autres colonnes existantes (sauf celles déjà traitées)
#         for key, value in row.items():
#             if key not in ["_id", "Commentaire_Client", "commentaire_moderateur"]:
#                 doc[key] = value

#         batch.append(InsertOne(doc))
#         docs_traites += 1

#         if len(batch) >= BATCH_SIZE:
#             try:
#                 collection.bulk_write(batch, ordered=False)
#             except BulkWriteError:
#                 pass
#             batch = []

#     if batch:
#         try:
#             collection.bulk_write(batch, ordered=False)
#         except BulkWriteError:
#             pass

#     client.close()
#     yield {"docs_traites": docs_traites, "docs_hors_scope": docs_hors, "statut": "ok"}


# def lire_partition_depuis_mongo(partition_info):
#     import sys
#     sys.path.insert(0, '/opt/pymongo_libs')
#     from pymongo import MongoClient

#     for item in partition_info:
#         client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
#         db = client[DB_NAME]
#         collection = db[COLLECTION_SOURCE]
#         # ✅ CHANGEMENT CRUCIAL : On récupère TOUTES les colonnes
#         curseur = collection.find({}, {}).skip(item["skip"]).limit(item["limit"])
#         for doc in curseur:
#             doc["_id"] = str(doc["_id"])
#             yield doc
#         client.close()


# # ============================================================
# # PIPELINE PRINCIPAL
# # ============================================================
# temps_debut_global = time.time()

# print("=" * 70)
# print("🔤 NORMALISATION MULTI-NŒUDS v3.9.5")
# print("   FIX-38 : ms protégé (unité latence) ✅")
# print("   FIX-39 : my idoom/djezzy/mobilis/ooredoo ✅")
# print("   FIX-40 : بروموسيو → promotion ✅")
# print("   FIX-41 : noms propres préservés ✅")
# print("   FIX-42 : wifi5/wifi6/wifi7 protégé ✅")
# print("   FIX-43 : XG-SPON protégé ✅")
# print("   FIX-44 : arabizi hybride converti ✅")
# print("   FIX-45 : arabizi manquants (bdlna, khalso, draham...) ✅")
# print("   FIX-46 : الgaming → الألعاب (correction coupure) ✅")
# print(f"   FIX-47 : preserve_latin_darja = {PRESERVE_LATIN_DARJA} ✅")
# print(f"   Mode   : {MODE} | stopwords={REMOVE_STOPWORDS} | disable_arabizi={DISABLE_ARABIZI}")
# print(f"   Source : {COLLECTION_SOURCE}")
# print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
# print("=" * 70)

# # 1. CONNEXION MONGODB DRIVER
# print("\n📂 Connexion MongoDB (Driver)...")
# try:
#     client_driver = MongoClient(MONGO_URI_DRIVER)
#     db_driver = client_driver[DB_NAME]
#     coll_source = db_driver[COLLECTION_SOURCE]
#     total_docs = coll_source.count_documents({})
#     print(f"✅ MongoDB connecté — {total_docs} documents dans la source")
# except Exception as e:
#     print(f"❌ Erreur MongoDB : {e}")
#     exit(1)

# # 2. CONNEXION SPARK
# print("\n⚡ Connexion au cluster Spark...")
# temps_debut_spark = time.time()
# spark = SparkSession.builder \
#     .appName("Normalisation_MultiNode_v395") \
#     .master(SPARK_MASTER) \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.sql.shuffle.partitions", "4") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("WARN")
# temps_fin_spark = time.time()
# print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")

# # 3. LECTURE DISTRIBUÉE
# print("\n📥 LECTURE DISTRIBUÉE...")
# temps_debut_chargement = time.time()
# docs_par_worker = math.ceil(total_docs / NB_WORKERS)
# plages = [
#     {"skip": i * docs_par_worker,
#      "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
#     for i in range(NB_WORKERS)
# ]
# for idx, p in enumerate(plages):
#     print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

# rdd_data = spark.sparkContext \
#     .parallelize(plages, NB_WORKERS) \
#     .mapPartitions(lire_partition_depuis_mongo)

# # Conversion en DataFrame Spark (pour conserver la structure)
# df_spark = spark.read.json(rdd_data.map(
#     lambda d: json.dumps(
#         {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
#          for k, v in d.items()}
#     )
# ))
# total_lignes = df_spark.count()
# temps_fin_chargement = time.time()
# print(f"✅ {total_lignes} documents chargés en {temps_fin_chargement - temps_debut_chargement:.2f}s")

# # 4. VIDER DESTINATION
# coll_dest = db_driver[COLLECTION_DEST]
# coll_dest.delete_many({})
# print("\n🧹 Collection destination vidée")

# # 5. NORMALISATION DISTRIBUÉE
# print("\n💾 NORMALISATION DISTRIBUÉE...")
# temps_debut_traitement = time.time()

# rdd_stats = df_spark.rdd \
#     .map(lambda row: row.asDict()) \
#     .mapPartitions(normaliser_partition)

# stats_ecriture = rdd_stats.collect()
# total_inseres = sum(s.get("docs_traites", 0) for s in stats_ecriture if s.get("statut") == "ok")
# total_hors = sum(s.get("docs_hors_scope", 0) for s in stats_ecriture if s.get("statut") == "ok")
# erreurs = [s for s in stats_ecriture if "_erreur" in s]

# temps_fin_traitement = time.time()
# print(f"✅ Normalisation terminée en {temps_fin_traitement - temps_debut_traitement:.2f}s")
# print(f"📦 {total_inseres} documents normalisés")
# print(f"🚫 {total_hors} documents hors scope filtrés")
# if erreurs:
#     for e in erreurs:
#         print(f"   ⚠️  {e.get('_erreur')}")

# spark.stop()

# # 6. VÉRIFICATION FINALE
# print("\n🔎 VÉRIFICATION FINALE...")
# total_en_dest = coll_dest.count_documents({})
# succes = total_en_dest == total_inseres

# print(f"   • Documents source         : {total_lignes}")
# print(f"   • Documents normalisés     : {total_inseres}")
# print(f"   • Documents hors scope     : {total_hors}")
# print(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")

# # 7. RAPPORT
# temps_fin_global = time.time()
# temps_total = temps_fin_global - temps_debut_global

# lignes_rapport = []
# lignes_rapport.append("=" * 70)
# lignes_rapport.append("🔤 NORMALISATION MULTI-NŒUDS v3.9.5")
# lignes_rapport.append(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# lignes_rapport.append(f"   Mode : Spark 4.1.1 | {NB_WORKERS} Workers | MongoDB direct")
# lignes_rapport.append("=" * 70)
# lignes_rapport.append("\nCORRECTIFS APPLIQUÉS :")
# lignes_rapport.append("   FIX-38 : ms protégé comme unité de latence")
# lignes_rapport.append("   FIX-39 : my idoom/djezzy/mobilis/ooredoo protégé")
# lignes_rapport.append("   FIX-40 : بروموسيو → promotion conservé")
# lignes_rapport.append("   FIX-41 : noms propres latins préservés")
# lignes_rapport.append("   FIX-42 : wifi5/wifi6/wifi7 protégé")
# lignes_rapport.append("   FIX-43 : XG-SPON protégé")
# lignes_rapport.append("   FIX-44 : arabizi hybride/pur converti (n9drou, nkhlsou...)")
# lignes_rapport.append("   FIX-45 : arabizi manquants (bdlna, khalso, draham...)")
# lignes_rapport.append("   FIX-46 : correction الgaming → الألعاب")
# lignes_rapport.append(f"   FIX-47 : preserve_latin_darja = {PRESERVE_LATIN_DARJA}")
# lignes_rapport.append(f"\n📊 RÉSULTATS :")
# lignes_rapport.append(f"   ┌────────────────────────────────────────────┐")
# lignes_rapport.append(f"   │ Documents source          : {total_lignes:<15} │")
# lignes_rapport.append(f"   │ Documents normalisés      : {total_inseres:<15} │")
# lignes_rapport.append(f"   │ Documents hors scope      : {total_hors:<15} │")
# lignes_rapport.append(f"   │ Taux de succès            : {total_inseres/total_lignes*100:<14.2f}% │")
# lignes_rapport.append(f"   └────────────────────────────────────────────┘")
# lignes_rapport.append(f"\n⏱️  TEMPS :")
# lignes_rapport.append(f"   • Connexion Spark  : {temps_fin_spark - temps_debut_spark:.2f}s")
# lignes_rapport.append(f"   • Chargement       : {temps_fin_chargement - temps_debut_chargement:.2f}s")
# lignes_rapport.append(f"   • Normalisation    : {temps_fin_traitement - temps_debut_traitement:.2f}s")
# lignes_rapport.append(f"   • TOTAL            : {temps_total:.2f}s ({total_lignes/temps_total:.0f} doc/s)")
# lignes_rapport.append(f"\n📁 STOCKAGE :")
# lignes_rapport.append(f"   • Source      : {DB_NAME}.{COLLECTION_SOURCE}")
# lignes_rapport.append(f"   • Destination : {DB_NAME}.{COLLECTION_DEST}")
# lignes_rapport.append(f"   • Statut      : {'✅ SUCCÈS' if succes else '⚠️ VÉRIFIER'}")
# lignes_rapport.append("=" * 70)

# rapport_texte = "\n".join(lignes_rapport)
# os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
# with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
#     f.write(rapport_texte)
# print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

# print("\n" + "=" * 70)
# print("📊 RÉSUMÉ FINAL")
# print("=" * 70)
# print(f"   📥 Documents source        : {total_lignes}")
# print(f"   📤 Documents normalisés    : {total_inseres}")
# print(f"   🚫 Hors scope filtrés      : {total_hors}")
# print(f"   ⏱️  Temps total             : {temps_total:.2f}s")
# print(f"   🚀 Vitesse                 : {total_lignes/temps_total:.0f} docs/s")
# print(f"   📁 Collection dest.        : {DB_NAME}.{COLLECTION_DEST}")
# print("=" * 70)
# print(f"   Statut : {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")
# print("=" * 70)
# print("🎉 NORMALISATION TERMINÉE EN MODE MULTI-NŒUDS !")

# client_driver.close()
# print("🔌 Connexions fermées proprement")
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
normalisation_multinode_v8.py
====================================
Normaliseur unifié pour corpus télécom DZ.
Version MULTI-NŒUDS avec Spark pour traitement distribué.
Intègre TextNormalizer v3.9.5 (code binôme).

Génère 2 colonnes de sortie :
- normalized_arabert   : version normalisée pour AraBERT (léger)
- normalized_full      : version normalisée complète (avec stopwords)

Conserve TOUTES les colonnes originales.
Sauvegarde les commentaires hors scope dans un fichier JSON séparé.

MODIFICATIONS SPÉCIALES :
- Les mots religieux (nchallah, wlh, inchallah, etc.) sont TOUJOURS convertis
- Le reste de la Darja latine est préservé (khouya, sahbi, etc.)
- preserve_latin_darja = True par défaut
"""

from pyspark.sql import SparkSession
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime
import os, time, math, json, re, logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_sans_emojis"   
COLLECTION_DEST   = "commentaires_normalises"
BATCH_SIZE        = 1000
NB_WORKERS        = 3
SPARK_MASTER      = "spark://spark-master:7077"
RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/rapport_normalisation_multinode_v8.txt"
HORS_SCOPE_PATH   = "/home/mouna/projet_telecom/scripts/nettoyage/Rapports/hors_scope_comments.json"

MASTER_DICT_PATH = Path("/opt/dictionnaires/master_dict.json")
RULES_DICT_PATH  = Path("/opt/dictionnaires/linguistic_rules.json")

# Paramètres
PRESERVE_LATIN_DARJA = True   # Garder la Darja en latin MAIS convertir les mots religieux
DISABLE_ARABIZI = False       # Désactiver la conversion arabe

# ============================================================
# CONSTANTES SUPPLEMENTAIRES
# ============================================================

# Mots religieux à convertir TOUJOURS (même avec preserve_latin_darja=True)
_RELIGIOUS_WORDS_TO_CONVERT: Set[str] = {
    # Inchallah et variantes
    "nchallah", "nchalh", "nachalh", "inchallah", "inshallah", "inshalah",
    "inshaallah", "inchalah", "inshaalah", "nchallah", "nshallah",
    "inchaalah", "en chaa allah", "enchaallah",
    # Wallah et variantes
    "wlh", "wlhi", "wellah", "wella", "wallah", "wallahi", "wallhy",
    # Autres mots religieux
    "bismillah", "hamdoulah", "alhamdulillah", "mashallah", "subhanallah",
    "astaghfirullah", "rahmatullah", "rahman", "rahim",
    # Français religieux
    "svp", "stp", "s'il vous plait", "s'il te plait",
}

# ============================================================
# TEXT NORMALIZER v3.9.5 (code binôme — TOUS LES FIX inclus)
# ============================================================

_RE: Dict[str, re.Pattern] = {
    "diacritics":    re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06ED]"),
    "tatweel":       re.compile(r"\u0640+"),
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
    "num_separator": re.compile(r"(?<!\d)(\d+)([-:/])(\d+)(?!\d)"),
    "ar_then_lat":   re.compile(r"([\u0600-\u06FF])([a-zA-Z])"),
    "lat_then_ar":   re.compile(r"([a-zA-Z])([\u0600-\u06FF])"),
    "regex_key":     re.compile(r"[\\^$*+?.()\[\]{}|]"),
    "wifi_version":  re.compile(r"\bwifi\d+\b", re.IGNORECASE),
}

_AR_VERB_PREFIXES: Set[str] = {"ت", "ي", "ن", "أ", "ا", "تت", "يت", "نت", "ست"}
_AR_CONJ_PREFIXES: Set[str] = {"و", "ف", "ب", "ك", "ل", "ال", "لل", "بال", "كال", "فال", "وال"}
_AR_ALL_PREFIXES: Set[str]  = _AR_VERB_PREFIXES | _AR_CONJ_PREFIXES

_ABBREV_KEYS_TO_PROTECT: Set[str] = {
    "adsl", "vdsl", "ftth", "fttb", "xgs", "xgs-pon", "pon", "dsl",
    "lte", "volte", "dns", "ont", "wifi", "wify", "wi-fi", "wlan",
    "4g", "3g", "2g", "5g", "cnx", "rdm",
    "mbps", "kbps", "gbps", "mbs", "mb/s", "gb/s", "kb/s",
    "idoom", "djezzy", "mobilis", "ooredoo",
    "fibre", "fiber", "febre", "ping", "hd", "fhd", "uhd",
    "ram", "rom", "cpu", "gpu", "android", "ios",
    "4k", "8k", "2k", "netflix", "amazon", "fortnite",
    "nokia", "tenda", "zte", "huawei", "xiaomi",
    "iptv", "ip80", "ethernet", "gigabit", "starlink",
    "reset", "re-provision", "reprovision",
    "qoe", "speedtest", "backbone", "throughput",
    "routing", "peering", "cdn", "olt", "qos", "jitter", "gpon",
    "ms", "xg-spon", "xg-s-pon",
}

_ABBREV_AMBIGUOUS_SKIP: Set[str] = {
    "re", "co", "da", "an", "ko", "mo", "go", "gb", "mb", "kb",
    "bla", "li", "ki", "w", "at", "db", "tt", "pr", "pk", "pq", "nn",
    "cc", "cv", "fb", "yt", "ok",
}

_PROTECTED_COMPOUNDS: List[str] = [
    "my idoom", "my ooredoo", "my djezzy", "my mobilis",
    "no transaction", "no transction",
    "fibre optique", "core network", "traffic shaping",
    "prime video", "prime vidéo", "google tv", "tv box", "twin box", "mi box",
]

_EXTRA_ARABIZI_WORDS: Dict[str, str] = {
    # Inchallah et variantes
    "nachalh": "إن شاء الله", "nchalh": "إن شاء الله",
    "inchalh": "إن شاء الله", "inshalah": "إن شاء الله",
    "inshalh": "إن شاء الله", "inshaallah": "إن شاء الله",
    "inchalah": "إن شاء الله", "inshaalah": "إن شاء الله",
    "inchallah": "إن شاء الله", "inshallah": "إن شاء الله",
    "nchallah": "إن شاء الله", "nshallah": "إن شاء الله",
    "inchaalah": "إن شاء الله", "en chaa allah": "إن شاء الله", "enchaallah": "إن شاء الله",
    # Wallah et variantes
    "wlh": "والله", "wlhi": "والله", "wellah": "والله",
    "wella": "والله", "wallah": "والله", "wallahi": "والله",
    "wallhy": "والله",
    # Autres mots religieux
    "bismillah": "بسم الله", "hamdoulah": "الحمد لله", 
    "alhamdulillah": "الحمد لله", "mashallah": "ما شاء الله",
    "subhanallah": "سبحان الله", "astaghfirullah": "أستغفر الله",
    "rahmatullah": "رحمة الله", "rahman": "الرحمن", "rahim": "الرحيم",
    # Français religieux
    "svp": "s'il vous plaît", "stp": "s'il te plaît",
    # Autres mots arabizi
    "flexi": "فليكسي", "flexili": "فليكسي",
    "nflexi": "فليكسي", "yflexi": "فليكسي",
    "my": "my", "promotion": "promotion", "promo": "promo",
    "n9drou": "نقدرو", "nkhlsou": "نخلصو", "nl9a": "نلقا",
    "chhar": "شهر", "fibr": "fibre optique", "psq": "parce que",
    "ndkhol": "ندخل", "nkhles": "نخلص",
    "khir": "خير", "khorda": "خردة", "ytl3": "يطلع",
    "boost": "boost", "fel": "في ال", "balak": "بالاك",
    "bdlna": "بدّلنا", "khalso": "خلصو", "khalsoo": "خلصو",
    "draham": "دراهم", "tbgho": "تبغو", "rbnii": "ربّي",
    "haw": "هاو", "chwala": "شوالة",
    "khoya": "خويا", "khouya": "خويا", "dyal": "ديال", "dial": "ديال",
    "bhal": "بحال", "mazel": "مزال", "sahbi": "صاحبي",
    "3andek": "عندك", "3andi": "عندي", "hadchi": "هذا الشيء",
    "wakha": "واخا", "rabi": "ربي", "yehdi": "يهدي",
    "yehdikoum": "يهديكم", "mn": "من", "fi": "في",
    "hdra": "هدرة", "khdma": "خدمة",
}

_EXTRA_ARABIZI_UPPER: Dict[str, str] = {
    "NACHALH": "إن شاء الله", "NCHALH": "إن شاء الله",
    "INCHALH": "إن شاء الله", "INSHALAH": "إن شاء الله",
    "INSHALLAH": "إن شاء الله", "INCHALAH": "إن شاء الله",
    "INCHALLAH": "إن شاء الله",
    "WLH": "والله", "WELLAH": "والله", "WALLAH": "والله", "WALLAHI": "والله",
    "BDLNA": "بدّلنا", "KHALSO": "خلصو", "KHALSOO": "خلصو",
    "DRAHAM": "دراهم", "TBGHO": "تبغو", "RBNII": "ربّي",
    "HAW": "هاو", "CHWALA": "شوالة",
    "SVP": "s'il vous plaît", "STP": "s'il te plaît",
}

_EXTRA_AR_PATTERNS: List[Tuple[re.Pattern, str]] = [
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

_MIXED_AR_LATIN_COMPOUNDS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\bالgaming\b", re.IGNORECASE), "الألعاب"),
    (re.compile(r"\bالgame\b",   re.IGNORECASE), "اللعبة"),
    (re.compile(r"\bالstream\b", re.IGNORECASE), "البث المباشر"),
    (re.compile(r"\bالlag\b",    re.IGNORECASE), "التأخير"),
    (re.compile(r"\bالping\b",   re.IGNORECASE), "ping"),
    (re.compile(r"\bالwifi\b",   re.IGNORECASE), "واي فاي"),
    (re.compile(r"\bالweb\b",    re.IGNORECASE), "الانترنت"),
]

HORS_SCOPE_KEYWORDS: List[str] = [
    "embauche", "entretien d'embauche", "recrutement", "offre d'emploi",
    "sarl maxim", "hashtag#",
    "code du travail", "loi 90-11", "comité de participation",
    "عدل3", "عدل 3", "حق_الطعون", "مراجعة_الملفات",
    "المقصيون_من_عدل", "الشفافية_في_عدل",
]

_COMPOUND_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(re.escape(c), re.IGNORECASE), c)
    for c in sorted(_PROTECTED_COMPOUNDS, key=len, reverse=True)
]

# ============================================================
# FONCTIONS DE CONSTRUCTION DES RÈGLES
# ============================================================
def _load_rules(rules_path):
    if not rules_path.exists():
        return {}
    with open(rules_path, encoding="utf-8") as f:
        return json.load(f)

def _build_negations(rules):
    neg = rules.get("negations", {})
    result = set(neg.get("arabe_standard", []))
    result.update(neg.get("dialecte_algerien", []))
    result.update({"ما", "لا", "لم", "لن", "ليس", "غير", "مش", "ميش",
                   "مكانش", "ماكش", "ماهيش", "ماهوش"})
    return result

def _build_dialect_keep(rules):
    dk = rules.get("dialect_keep", {})
    result = set()
    for v in dk.values():
        if isinstance(v, list):
            result.update(v)
    result.update({"راه", "راهي", "راني", "راك", "راهم", "واش", "كيفاش", "وين",
                   "علاش", "قديش", "مزال", "كيما", "باه", "ديما",
                   "ya", "pas", "de", "en", "avec", "depuis", "pour",
                   "par", "sur", "sous", "dans", "entre", "sans", "vers"})
    return result

def _build_intentional_repeats(rules):
    ir = rules.get("intentional_repeats", {})
    result = set()
    for v in ir.values():
        if isinstance(v, list):
            result.update(v)
    result.update({"كيف", "شوي", "برا", "هاك", "يا", "آه", "لا", "واو",
                   "بزاف", "مزال", "كل", "نعم", "جدا",
                   "très", "trop", "bien", "non", "oui", "si", "jamais", "encore"})
    return result

def _build_tech_tokens(rules):
    tt = rules.get("tech_tokens", {})
    result = set()
    for v in tt.values():
        if isinstance(v, list):
            result.update(t.lower() for t in v)
    result.discard("cnx")
    result.update(_ABBREV_KEYS_TO_PROTECT)
    return result

def _build_arabic_prefixes(rules):
    ap = rules.get("arabic_prefixes", {})
    prefixes = ap.get("prefixes", [])
    if not prefixes:
        prefixes = ["وال", "فال", "بال", "كال", "لل", "ال", "و", "ف", "ب", "ك", "ل"]
    return sorted(prefixes, key=len, reverse=True)

def _build_contractions(rules):
    ct = rules.get("contractions", {})
    result = dict(ct.get("francais", {}))
    result.update({
        "j'ai": "je ai", "j\u2019ai": "je ai",
        "c'est": "ce est", "c\u2019est": "ce est",
        "n'est": "ne est", "n\u2019est": "ne est",
        "n'a": "ne a", "n\u2019a": "ne a",
        "qu'il": "que il", "qu\u2019il": "que il",
        "qu'on": "que on", "qu\u2019on": "que on",
    })
    return result

def _is_regex_key(k: str) -> bool:
    return bool(_RE["regex_key"].search(k))

def _build_plain_dict(raw: dict) -> Dict[str, str]:
    return {k: v for k, v in raw.items() if not _is_regex_key(k)}

def _build_protected_words(rules):
    pw = rules.get("protected_words", {})
    result = set()
    for v in pw.values():
        if isinstance(v, list):
            result.update(t.lower() for t in v)
    result.update(_ABBREV_KEYS_TO_PROTECT)
    result.update({
        "internet", "connexion", "problème", "réseau", "service",
        "optique", "ping", "gaming", "game", "live", "speed",
        "high", "low", "lag", "stream",
        "facebook", "whatsapp", "youtube", "instagram",
        "bonjour", "merci", "salut", "normal", "bravo",
        "message", "solution", "compte", "temps",
        "même", "comme", "chaque", "alors",
        "avant", "depuis", "juste", "vraiment",
        "lente", "mois", "plusieurs",
        "pas", "on", "ne", "fait", "rien", "tout", "fois", "bien",
        "moi", "encore", "niveau",
        "promotion", "promo",
        "my idoom", "my ooredoo", "my djezzy", "my mobilis",
        "no transaction", "no transction",
    })
    return result

def _build_stopwords_from_rules(rules, negations, dialect_keep):
    sw_section = rules.get("stopwords", {})
    sw = set()
    for v in sw_section.values():
        if isinstance(v, list):
            sw.update(v)
    if not sw:
        sw = {
            "le", "la", "les", "l", "un", "une", "des", "du", "de", "et",
            "ou", "mais", "donc", "car", "ni", "or", "ce", "cet", "cette",
            "ces", "mon", "ton", "son", "notre", "votre", "leur", "leurs",
            "ma", "ta", "sa", "mes", "tes", "ses",
            "je", "tu", "il", "elle", "nous", "vous", "ils", "elles",
            "me", "te", "se", "lui", "y", "en", "que", "qui", "quoi", "dont",
            "est", "sont", "être", "avoir", "a", "ai",
            "avec", "sans", "sur", "sous", "dans", "par", "pour",
            "très", "plus", "moins", "aussi", "bien", "tout",
            "pas", "ne", "on", "si",
            "i", "my", "we", "our", "you", "your", "he", "she", "it",
            "its", "they", "their", "am", "is", "are", "was", "were",
            "have", "has", "had", "do", "does", "did",
            "will", "would", "could", "should", "a", "an", "the",
            "and", "or", "but", "if", "in", "on", "at", "by", "for",
            "with", "to", "from", "not", "no",
            "إلى", "عن", "مع", "كان", "كانت", "هذا", "هذه", "ذلك", "تلك",
            "هو", "هي", "هم", "هن", "ثم", "أو", "إن", "إذا", "لو", "قد",
            "لكن", "بل", "حتى", "ضد", "أن", "التي", "الذي", "الذين",
        }
    try:
        from nltk.corpus import stopwords as _sw
        sw.update(_sw.words("arabic"))
    except Exception:
        pass
    return sw - negations - dialect_keep

def _build_contraction_re(contractions):
    apostrophe_keys = [k for k in contractions if "'" in k or "\u2019" in k]
    if not apostrophe_keys:
        return re.compile(r"(?!)")
    escaped = sorted([re.escape(k) for k in apostrophe_keys], key=len, reverse=True)
    return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)


class TextNormalizer:
    """TextNormalizer v3.9.5 — compatible Spark"""

    def __init__(
        self,
        mode="arabert",
        dict_path=None,
        rules_path=None,
        remove_stopwords=False,
        disable_arabizi=False,
        preserve_latin_darja=False,
    ):
        assert mode in ("arabert", "full")
        self.mode = mode
        self.remove_sw = remove_stopwords or (mode == "full")
        self.disable_arabizi = disable_arabizi
        self.preserve_latin_darja = preserve_latin_darja

        dict_path = dict_path or MASTER_DICT_PATH
        rules_path = rules_path or RULES_DICT_PATH

        with open(dict_path, encoding="utf-8") as f:
            d = json.load(f)

        rules = _load_rules(rules_path)

        self._negations = _build_negations(rules)
        self._dialect_keep = _build_dialect_keep(rules)
        self._intentional_rep = _build_intentional_repeats(rules)
        self._tech_tokens = _build_tech_tokens(rules)
        self._arabic_prefixes = _build_arabic_prefixes(rules)
        self._contractions = _build_contractions(rules)
        self._contraction_re = _build_contraction_re(self._contractions)

        self.unicode_map = d["unicode_arabic"]
        self.unicode_map.setdefault("\u0629", "\u0647")
        self.unicode_map.setdefault("\u0624", "\u0648")

        self.digrams = d["arabizi_digrams"]
        self.monograms = {
            k: v for k, v in d["arabizi_monograms"].items()
            if not (len(k) == 1 and k.isalpha())
        }

        raw_arabizi = d["arabizi_words"]
        self.arabizi_words = {**raw_arabizi, **_EXTRA_ARABIZI_WORDS}
        self.arabizi_words = {k: v for k, v in self.arabizi_words.items()
                              if not k.startswith("_")}

        self.arabizi_upper = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}
        self.emojis = d["emojis"]
        self.abbreviations = d["abbreviations"]
        self.telecom = _build_plain_dict(d["telecom_tech"])
        self.units_map = d["units"]

        nv = d["network_variants"]
        self._net_form = nv["normalized_form"]
        self._net_all = nv["latin"] + nv["arabic"]

        mixed_clean = {k: v for k, v in d["mixed_ar_fr_regex"].items()
                       if not k.startswith("_")}
        self._mixed_pats = self._compile_dict(mixed_clean)
        self._fr_pats = self._compile_dict(
            d["french_corrections_regex"],
            flags=re.IGNORECASE | re.UNICODE
        )

        escaped_net = [re.escape(v) for v in self._net_all if v]
        self._net_re = re.compile(
            rf'\b({"|".join(escaped_net)})\b', re.IGNORECASE
        ) if escaped_net else None

        combined = {**self.digrams, **self.monograms}
        self._arabizi_seq = sorted(combined.items(), key=lambda x: len(x[0]), reverse=True)
        self._arabizi_upper_sorted = sorted(self.arabizi_upper.items(), key=lambda x: len(x[0]), reverse=True)

        _all_vals = []
        for v in list(self.telecom.values()) + list(self.abbreviations.values()):
            if v:
                _all_vals.extend(v.split())

        self._protected = _build_protected_words(rules)
        self._protected.update(t.lower() for t in _all_vals)
        self._protected.update(self._tech_tokens)

        self._stopwords = (
            _build_stopwords_from_rules(rules, self._negations, self._dialect_keep)
            if self.remove_sw else set()
        )

    @staticmethod
    def _compile_dict(d, flags=re.UNICODE):
        result = []
        for pat, repl in d.items():
            try:
                result.append((re.compile(pat, flags), repl))
            except re.error:
                pass
        return result

    def _split_prefix(self, word):
        for p in self._arabic_prefixes:
            if word.startswith(p) and len(word) > len(p) + 1:
                return p, word[len(p):]
        return "", word

    def _lookup(self, word, dct):
        lo = word.lower()
        v = dct.get(lo) or dct.get(word)
        if v is not None:
            return v
        pref, root = self._split_prefix(word)
        if pref:
            v = dct.get(root.lower()) or dct.get(root)
            if v is not None:
                return v if " " in v else pref + v
        return None

    def _is_latin_dominant(self, text):
        lat = sum(1 for c in text if "a" <= c.lower() <= "z")
        ar = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
        tot = lat + ar
        if not tot:
            return False
        threshold = 0.70 if len(text) < 30 else 0.75
        return (lat / tot) > threshold

    @staticmethod
    def _is_proper_noun_token(tok):
        if len(tok) < 3 or not _RE["pure_latin"].match(tok):
            return False
        return tok[0].isupper() and not tok.isupper()

    @staticmethod
    def _is_hors_scope(text):
        if not text:
            return False
        lo = text.lower()
        return any(kw.lower() in lo for kw in HORS_SCOPE_KEYWORDS)

    def _protect_numbers(self, text):
        protected = {}
        counter = [0]
        def _protect(m):
            key = f"__NP{counter[0]}__"
            counter[0] += 1
            protected[key] = m.group(0)
            return key
        text = _RE["num_separator"].sub(_protect, text)
        text = re.sub(r"(?<!\d)(\d+)\s*/\s*(\d+)(?!\d)", _protect, text)
        return protected, text

    def _protect_compounds_pre(self, text):
        placeholders = {}
        counter = [0]
        for pat, original in _COMPOUND_PATTERNS:
            if pat.search(text):
                ph = f"__CPD{counter[0]}__"
                counter[0] += 1
                placeholders[ph] = original
                text = pat.sub(ph, text)
        return placeholders, text

    def _dedup_tokens(self, text):
        tokens = text.split()
        if len(tokens) < 2:
            return text
        result = [tokens[0]]
        for i in range(1, len(tokens)):
            prev, curr = result[-1], tokens[i]
            prev_lo, curr_lo = prev.lower(), curr.lower()
            if curr_lo == prev_lo:
                if curr_lo in self._intentional_rep:
                    result.append(curr)
                continue
            has_ar_curr = bool(_RE["has_arabic"].search(curr))
            if (not has_ar_curr and len(curr) >= 4
                    and len(curr) < len(prev) and prev_lo.endswith(curr_lo)):
                continue
            if (has_ar_curr and len(curr) >= 5
                    and len(curr) < len(prev) and prev.endswith(curr)):
                m = _RE["arabic_prefix"].match(prev)
                if m and m.group(2) == curr:
                    result.append(curr); continue
                stripped = prev[:len(prev)-len(curr)]
                if stripped in _AR_ALL_PREFIXES and len(stripped) >= 2:
                    result.append(curr); continue
                result.append(curr); continue
            result.append(curr)
        return " ".join(result)

    def normalize(self, text):
        if not isinstance(text, str) or not text.strip():
            return ""
        try:
            protected_nums, text = self._protect_numbers(text)
            protected_cpd, text = self._protect_compounds_pre(text)

            text = self._step_emojis(text)
            text = self._step_unicode_arabic(text)
            text = self._step_extra_ar(text)
            text = self._step_ar_latin_compounds(text)
            text = _RE["wifi_version"].sub(lambda m: m.group(0).lower(), text)

            for pat, repl in self._mixed_pats:
                text = pat.sub(repl, text)
            text = self._step_french(text)
            text = self._step_abbrev(text)
            text = self._step_units(text)
            text = self._step_split_mixed_tokens(text)
            text = self._step_arabizi(text)
            text = self._step_cleanup(text)
            text = self._dedup_tokens(text)
            if self.remove_sw:
                text = self._step_stopwords(text)

            for ph, original in protected_cpd.items():
                text = text.replace(ph, original)
            for key, val in protected_nums.items():
                text = text.replace(key, val)

        except Exception:
            return ""
        return text.strip()

    def _step_emojis(self, text):
        for emoji, word in self.emojis.items():
            if emoji in text:
                text = text.replace(emoji, f" {word} ")
        return text

    def _step_extra_ar(self, text):
        for pat, repl in _EXTRA_AR_PATTERNS:
            text = pat.sub(repl, text)
        return text

    def _step_ar_latin_compounds(self, text):
        for pat, repl in _MIXED_AR_LATIN_COMPOUNDS:
            text = pat.sub(repl, text)
        return text

    def _step_units(self, text):
        def _repl(m):
            num, unit = m.group(1), m.group(2).lower()
            KEEP_ATTACHED = {"ms", "h", "s"}
            if unit in KEEP_ATTACHED:
                return f"{num}{unit}"
            if len(unit) == 1 and unit not in self.units_map:
                return m.group(0)
            norm = self.units_map.get(unit)
            if norm is None:
                return m.group(0)
            return f"{num} {norm}"
        text = _RE["unit_space"].sub(_repl, text)
        text = _RE["unit_nospace"].sub(_repl, text)
        return text

    def _step_abbrev(self, text):
        latin_dom = self._is_latin_dominant(text)
        tokens, result = text.split(), []
        i = 0
        while i < len(tokens):
            tok = tokens[i]
            m = _RE["trail_punct"].match(tok)
            core, trail = (m.group(1), m.group(2)) if m else (tok, "")
            lo_core = core.lower()

            if core.startswith("__CPD") and core.endswith("__"):
                result.append(tok); i += 1; continue
            if core in self._negations:
                result.append(tok); i += 1; continue
            if (not _RE["has_arabic"].search(core)
                    and not _RE["digits_only"].match(core)
                    and lo_core in self.arabizi_words):
                result.append(tok); i += 1; continue
            if lo_core in self._tech_tokens:
                result.append(tok); i += 1; continue

            mn = _RE["num_arabic"].match(core)
            if mn:
                num, unit = mn.groups()
                repl = self._lookup(unit, self.telecom) or unit
                if " " in repl:
                    result += [num] + repl.split()[:-1] + [repl.split()[-1] + trail]
                else:
                    result += [num, repl + trail]
                i += 1; continue

            resolved = False
            for dct in (self.telecom, self.abbreviations):
                if (dct is self.abbreviations
                        and lo_core in _ABBREV_AMBIGUOUS_SKIP
                        and latin_dom):
                    continue
                repl = self._lookup(core, dct)
                if repl is not None:
                    if (latin_dom and dct is self.telecom
                            and not _RE["has_arabic"].search(core)
                            and len(core) <= 4
                            and lo_core not in _ABBREV_KEYS_TO_PROTECT):
                        break
                    if " " in repl:
                        parts = repl.split()
                        result += parts[:-1] + [parts[-1] + trail]
                    else:
                        result.append(repl + trail)
                    resolved = True
                    break

            if not resolved:
                if self._net_re and self._net_re.fullmatch(core):
                    result.append(self._net_form + trail)
                else:
                    result.append(tok)
            i += 1
        return " ".join(result)

    def _step_french(self, text):
        text = self._contraction_re.sub(
            lambda m: self._contractions.get(m.group(0).lower(), m.group(0)), text)
        for pat, repl in self._fr_pats:
            text = pat.sub(repl, text)
        return text

    def _step_split_mixed_tokens(self, text):
        text = _RE["ar_then_lat"].sub(r"\1 \2", text)
        text = _RE["lat_then_ar"].sub(r"\1 \2", text)
        return text

    def _step_arabizi(self, text):
        """
        Version MODIFIÉE : convertit les mots religieux (nchallah, wlh, svp, etc.)
        même avec preserve_latin_darja=True. Le reste de la Darja latine est préservé.
        """
        latin_dom = self._is_latin_dominant(text)
        result = []
        for tok in text.split():
            lo = tok.lower()

            # Protections existantes
            if tok.startswith("__CPD") and tok.endswith("__"):
                result.append(tok); continue
            if lo in self._tech_tokens:
                result.append(tok); continue
            if _RE["has_arabic"].search(tok):
                result.append(tok); continue
            if _RE["digits_only"].match(tok):
                result.append(tok); continue
            if _RE["num_arabic"].match(tok):
                result.append(tok); continue

            if _RE["pure_latin"].match(tok):
                _AMBIGUOUS_SHORT = {"ki", "el", "da", "li", "w", "dz"}
                
                # ✅ PRIORITÉ ABSOLUE : Mots religieux à convertir TOUJOURS
                # Ces mots seront convertis même si preserve_latin_darja = True
                if lo in _RELIGIOUS_WORDS_TO_CONVERT:
                    # Chercher d'abord dans arabizi_words
                    w = self.arabizi_words.get(lo)
                    if w:
                        result.append(w)
                        continue
                    # Chercher dans arabizi_upper
                    found = False
                    for k, v in self._arabizi_upper_sorted:
                        if tok.upper() == k:
                            result.append(v)
                            found = True
                            break
                    if not found:
                        result.append(tok)
                    continue
                
                # Si preserve_latin_darja = True, garder les autres mots latins
                if self.preserve_latin_darja:
                    result.append(tok)
                    continue

                # Logique normale de conversion (quand preserve_latin_darja = False)
                w = self.arabizi_words.get(lo)
                if w:
                    if latin_dom and len(lo) <= 2 and lo in _AMBIGUOUS_SHORT:
                        result.append(tok)
                    else:
                        result.append(w)
                    continue

                for k, v in self._arabizi_upper_sorted:
                    if tok.upper() == k:
                        result.append(v); break
                else:
                    if self._is_proper_noun_token(tok):
                        result.append(tok); continue
                    if self.disable_arabizi or latin_dom:
                        result.append(tok); continue
                    result.append(self._arabizi_convert(tok))
                continue

            if _RE["arabizi_hyb"].match(tok):
                # ✅ PRIORITÉ ABSOLUE : Mots religieux hybrides
                if lo in _RELIGIOUS_WORDS_TO_CONVERT:
                    w = self.arabizi_words.get(lo)
                    if w:
                        result.append(w); continue
                    for k, v in self._arabizi_upper_sorted:
                        if tok.upper() == k:
                            result.append(v); break
                    else:
                        result.append(tok)
                    continue
                
                if self.preserve_latin_darja:
                    result.append(tok)
                    continue

                w = self.arabizi_words.get(lo)
                if w:
                    result.append(w); continue
                for k, v in self._arabizi_upper_sorted:
                    if tok.upper() == k:
                        result.append(v); break
                else:
                    if self.disable_arabizi or latin_dom:
                        result.append(tok)
                    else:
                        result.append(self._arabizi_convert(tok))
                continue

            result.append(tok)
        return " ".join(result)

    def _arabizi_convert(self, token):
        for k, v in self._arabizi_upper_sorted:
            if token.upper() == k:
                return v
        result = token.lower()
        for extra, ar in [("ee", "ي"), ("ii", "ي"), ("oo", "و"), ("pp", "ب")]:
            result = result.replace(extra, ar)
        for seq, ar in self._arabizi_seq:
            result = result.replace(seq, ar)
        result = re.sub(r"[a-z]", "", result)
        return result

    def _step_unicode_arabic(self, text):
        text = _RE["diacritics"].sub("", text)
        text = _RE["tatweel"].sub("", text)
        for variant, canonical in self.unicode_map.items():
            text = text.replace(variant, canonical)
        return text

    def _step_cleanup(self, text):
        text = _RE["arab_digit"].sub(r"\1 \2", text)
        text = _RE["digit_arab"].sub(r"\1 \2", text)
        def _fuse_spaced(m):
            return m.group(0).replace(" ", "")
        text = _RE["spaced_digits"].sub(_fuse_spaced, text)
        text = _RE["arabic_digits_spaced"].sub(_fuse_spaced, text)
        text = _RE["rep_chars"].sub(lambda m: m.group(1) * 2, text)
        text = _RE["rep_punct"].sub(lambda m: m.group(1), text)
        text = _RE["whitespace"].sub(" ", text).strip()
        return text

    def _step_stopwords(self, text):
        if not self._stopwords:
            return text
        placeholders = {}
        for i, compound in enumerate(_PROTECTED_COMPOUNDS):
            pattern = re.compile(re.escape(compound), re.IGNORECASE)
            if pattern.search(text):
                ph = f"__SWC{i}__"
                placeholders[ph] = compound
                text = pattern.sub(ph, text)
        keep = self._negations | self._dialect_keep
        result = []
        for w in text.split():
            lo = w.lower()
            if w.startswith("__") and w.endswith("__"):
                result.append(w); continue
            if (w.isdigit() or w in keep or lo in keep
                    or lo not in self._stopwords):
                result.append(w)
            elif len(w) == 1 and _RE["has_arabic"].match(w):
                result.append(w)
        text = " ".join(result)
        for ph, original in placeholders.items():
            text = text.replace(ph, original)
        return text


# ============================================================
# FONCTIONS SPARK DISTRIBUÉES
# ============================================================

def normaliser_partition(partition):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError

    # Variable pour collecter les commentaires hors scope
    hors_scope_list = []

    try:
        # Initialiser DEUX normaliseurs : un pour chaque mode
        normalizer_arabert = TextNormalizer(
            mode="arabert",
            remove_stopwords=False,
            disable_arabizi=DISABLE_ARABIZI,
            preserve_latin_darja=PRESERVE_LATIN_DARJA,
        )
        normalizer_full = TextNormalizer(
            mode="full",
            remove_stopwords=True,
            disable_arabizi=DISABLE_ARABIZI,
            preserve_latin_darja=PRESERVE_LATIN_DARJA,
        )
        client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_DEST]
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed", "hors_scope": []}
        return

    batch = []
    docs_traites = 0
    docs_hors = 0

    for row in partition:
        texte_original = row.get("Commentaire_Client", "") or ""

        if TextNormalizer._is_hors_scope(texte_original):
            docs_hors += 1
            # Sauvegarder le commentaire hors scope avec ses métadonnées
            hors_scope_list.append({
                "_id": row.get("_id"),
                "Commentaire_Client": texte_original,
                "date": row.get("date"),
                "source": row.get("source"),
                "moderateur": row.get("moderateur"),
                "commentaire_moderateur": row.get("commentaire_moderateur", ""),
                "statut": row.get("statut"),
                "motif": "hors_scope_keywords"
            })
            continue

        # Générer les 2 versions de normalisation
        normalized_arabert = normalizer_arabert.normalize(texte_original)
        normalized_full = normalizer_full.normalize(texte_original)

        # Construction du document final avec TOUTES les colonnes originales
        doc = {
            "_id": row.get("_id"),
            "Commentaire_Client": texte_original,  # On garde l'original NON modifié
            "normalized_arabert": normalized_arabert,
            "normalized_full": normalized_full,
        }
        
        # Ajouter TOUTES les autres colonnes existantes
        for key, value in row.items():
            if key not in ["_id", "Commentaire_Client"]:
                doc[key] = value

        batch.append(InsertOne(doc))
        docs_traites += 1

        if len(batch) >= BATCH_SIZE:
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
    
    # Retourner les stats ET les commentaires hors scope
    yield {
        "docs_traites": docs_traites, 
        "docs_hors_scope": docs_hors, 
        "statut": "ok",
        "hors_scope_comments": hors_scope_list
    }


def lire_partition_depuis_mongo(partition_info):
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient

    for item in partition_info:
        client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_SOURCE]
        # Récupérer TOUTES les colonnes
        curseur = collection.find({}, {}).skip(item["skip"]).limit(item["limit"])
        for doc in curseur:
            doc["_id"] = str(doc["_id"])
            yield doc
        client.close()


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================
temps_debut_global = time.time()

print("=" * 70)
print("🔤 NORMALISATION MULTI-NŒUDS v3.9.5")
print("   Génère 2 colonnes :")
print("     - normalized_arabert   (mode arabert)")
print("     - normalized_full      (mode full avec stopwords)")
print("   ✅ Conserve TOUTES les colonnes originales")
print("   ✅ Sauvegarde les commentaires hors scope dans un fichier JSON")
print("   ✅ Mots religieux CONVERTIS (nchallah, wlh, svp, etc.)")
print("   ✅ Darja latine PRÉSERVÉE (khouya, sahbi, etc.)")
print(f"   FIX-47 : preserve_latin_darja = {PRESERVE_LATIN_DARJA}")
print(f"   Source : {COLLECTION_SOURCE}")
print("=" * 70)

# 1. CONNEXION MONGODB DRIVER
print("\n📂 Connexion MongoDB (Driver)...")
try:
    client_driver = MongoClient(MONGO_URI_DRIVER)
    db_driver = client_driver[DB_NAME]
    coll_source = db_driver[COLLECTION_SOURCE]
    total_docs = coll_source.count_documents({})
    print(f"✅ MongoDB connecté — {total_docs} documents dans la source")
except Exception as e:
    print(f"❌ Erreur MongoDB : {e}")
    exit(1)

# 2. CONNEXION SPARK
print("\n⚡ Connexion au cluster Spark...")
temps_debut_spark = time.time()
spark = SparkSession.builder \
    .appName("Normalisation_MultiNode_v395_2cols") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
temps_fin_spark = time.time()
print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")

# 3. LECTURE DISTRIBUÉE
print("\n📥 LECTURE DISTRIBUÉE...")
temps_debut_chargement = time.time()
docs_par_worker = math.ceil(total_docs / NB_WORKERS)
plages = [
    {"skip": i * docs_par_worker,
     "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
    for i in range(NB_WORKERS)
]
for idx, p in enumerate(plages):
    print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']}")

rdd_data = spark.sparkContext \
    .parallelize(plages, NB_WORKERS) \
    .mapPartitions(lire_partition_depuis_mongo)

df_spark = spark.read.json(rdd_data.map(
    lambda d: json.dumps(
        {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
         for k, v in d.items()}
    )
))
total_lignes = df_spark.count()
temps_fin_chargement = time.time()
print(f"✅ {total_lignes} documents chargés en {temps_fin_chargement - temps_debut_chargement:.2f}s")

# 4. VIDER DESTINATION
coll_dest = db_driver[COLLECTION_DEST]
coll_dest.delete_many({})
print("\n🧹 Collection destination vidée")

# 5. NORMALISATION DISTRIBUÉE
print("\n💾 NORMALISATION DISTRIBUÉE (2 modes simultanés)...")
temps_debut_traitement = time.time()

rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(normaliser_partition)

stats_ecriture = rdd_stats.collect()
total_inseres = sum(s.get("docs_traites", 0) for s in stats_ecriture if s.get("statut") == "ok")
total_hors = sum(s.get("docs_hors_scope", 0) for s in stats_ecriture if s.get("statut") == "ok")
erreurs = [s for s in stats_ecriture if "_erreur" in s]

# Collecter tous les commentaires hors scope de tous les workers
all_hors_scope = []
for s in stats_ecriture:
    if s.get("statut") == "ok" and s.get("hors_scope_comments"):
        all_hors_scope.extend(s.get("hors_scope_comments"))

# Sauvegarder les commentaires hors scope dans un fichier JSON
if all_hors_scope:
    os.makedirs(os.path.dirname(HORS_SCOPE_PATH), exist_ok=True)
    with open(HORS_SCOPE_PATH, "w", encoding="utf-8") as f:
        json.dump(all_hors_scope, f, ensure_ascii=False, indent=2)
    print(f"\n📝 {len(all_hors_scope)} commentaires hors scope sauvegardés dans : {HORS_SCOPE_PATH}")
else:
    print("\n📝 Aucun commentaire hors scope trouvé")

temps_fin_traitement = time.time()
print(f"✅ Normalisation terminée en {temps_fin_traitement - temps_debut_traitement:.2f}s")
print(f"📦 {total_inseres} documents normalisés")
print(f"🚫 {total_hors} documents hors scope filtrés")
if erreurs:
    for e in erreurs:
        print(f"   ⚠️  {e.get('_erreur')}")

spark.stop()

# 6. VÉRIFICATION FINALE
print("\n🔎 VÉRIFICATION FINALE...")
total_en_dest = coll_dest.count_documents({})
succes = total_en_dest == total_inseres

print(f"   • Documents source         : {total_lignes}")
print(f"   • Documents normalisés     : {total_inseres}")
print(f"   • Documents hors scope     : {total_hors}")
print(f"   {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")

# 7. RAPPORT
temps_fin_global = time.time()
temps_total = temps_fin_global - temps_debut_global

lignes_rapport = []
lignes_rapport.append("=" * 70)
lignes_rapport.append("🔤 NORMALISATION MULTI-NŒUDS v3.9.5")
lignes_rapport.append(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
lignes_rapport.append(f"   Mode : Spark 4.1.1 | {NB_WORKERS} Workers | MongoDB direct")
lignes_rapport.append("   Colonnes générées : normalized_arabert, normalized_full")
lignes_rapport.append("   ✅ Conservation de TOUTES les colonnes originales")
lignes_rapport.append("   ✅ Mots religieux CONVERTIS (nchallah, wlh, svp, etc.)")
lignes_rapport.append("   ✅ Darja latine PRÉSERVÉE (khouya, sahbi, etc.)")
lignes_rapport.append(f"   ✅ Hors scope sauvegardé dans : {HORS_SCOPE_PATH}")
lignes_rapport.append("=" * 70)
lignes_rapport.append(f"\n📊 RÉSULTATS :")
lignes_rapport.append(f"   ┌────────────────────────────────────────────┐")
lignes_rapport.append(f"   │ Documents source          : {total_lignes:<15} │")
lignes_rapport.append(f"   │ Documents normalisés      : {total_inseres:<15} │")
lignes_rapport.append(f"   │ Documents hors scope      : {total_hors:<15} │")
lignes_rapport.append(f"   │ Taux de succès            : {total_inseres/total_lignes*100:<14.2f}% │")
lignes_rapport.append(f"   └────────────────────────────────────────────┘")
lignes_rapport.append(f"\n⏱️  TEMPS :")
lignes_rapport.append(f"   • Connexion Spark  : {temps_fin_spark - temps_debut_spark:.2f}s")
lignes_rapport.append(f"   • Chargement       : {temps_fin_chargement - temps_debut_chargement:.2f}s")
lignes_rapport.append(f"   • Normalisation    : {temps_fin_traitement - temps_debut_traitement:.2f}s")
lignes_rapport.append(f"   • TOTAL            : {temps_total:.2f}s ({total_lignes/temps_total:.0f} doc/s)")
lignes_rapport.append(f"\n📁 STOCKAGE :")
lignes_rapport.append(f"   • Source      : {DB_NAME}.{COLLECTION_SOURCE}")
lignes_rapport.append(f"   • Destination : {DB_NAME}.{COLLECTION_DEST}")
lignes_rapport.append(f"   • Hors scope  : {HORS_SCOPE_PATH}")
lignes_rapport.append(f"   • Statut      : {'✅ SUCCÈS' if succes else '⚠️ VÉRIFIER'}")
lignes_rapport.append("=" * 70)

rapport_texte = "\n".join(lignes_rapport)
os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
    f.write(rapport_texte)
print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

print("\n" + "=" * 70)
print("📊 RÉSUMÉ FINAL")
print("=" * 70)
print(f"   📥 Documents source        : {total_lignes}")
print(f"   📤 Documents normalisés    : {total_inseres}")
print(f"   🚫 Hors scope filtrés      : {total_hors}")
print(f"   💾 Fichier hors scope      : {HORS_SCOPE_PATH}")
print(f"   ⏱️  Temps total             : {temps_total:.2f}s")
print(f"   🚀 Vitesse                 : {total_lignes/temps_total:.0f} docs/s")
print(f"   📁 Collection dest.        : {DB_NAME}.{COLLECTION_DEST}")
print("=" * 70)
print(f"   Statut : {'✅ SUCCÈS TOTAL !' if succes else '⚠️  Vérifier manuellement'}")
print("=" * 70)
print("🎉 NORMALISATION TERMINÉE EN MODE MULTI-NŒUDS !")
print("   ✅ 2 colonnes ajoutées : normalized_arabert, normalized_full")
print("   ✅ Commentaire_Client conservé (non modifié)")
print("   ✅ Toutes les colonnes originales conservées")
print("   ✅ Mots religieux convertis : nchallah → إن شاء الله, wlh → والله, svp → s'il vous plaît")
print("   ✅ Darja latine préservée : khouya, sahbi, etc. restent en latin")
print(f"   ✅ Hors scope sauvegardé dans : {HORS_SCOPE_PATH}")

client_driver.close()
print("🔌 Connexions fermées proprement")