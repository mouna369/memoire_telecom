"""
text_normalizer_v3.9.5.py
=======================
Normaliseur unifié pour corpus télécom DZ (26K commentaires).
Charge DEUX fichiers :
- master_dict.json       : dictionnaires de traduction/normalisation
- linguistic_rules.json  : règles linguistiques (négations, stopwords, prefixes…)
Compatible : AraBERT  |  CAMeL-BERT  |  MarBERT  |  DziriBERT
Modes :
"arabert"  → normalisation légère, sans stopwords  (Transformers)
"full"     → normalisation complète + stopwords     (ML classique / stats)

═══════════════════════════════════════════════════════════════
Changelog v3.9.5 — NOUVEAU : Option pour préserver Darja-latine
FIX-47 : Ajout paramètre `preserve_latin_darja` pour préserver Darja-latine
- Quand preserve_latin_darja=True : TOUS les mots latin-Darja sont gardés
  en latin (même s'ils sont dans arabizi_words)
- Les mots du dictionnaire arabizi_words NE sont PAS convertis
- Tous les autres traitements sont conservés (Unicode, French, units...)
- Utile pour analyse qualitative où on veut garder la forme originale
═══════════════════════════════════════════════════════════════
Changelog v3.9.4 — correctifs issus de la vérification qualitative (50 cas) :
FIX-45 : Arabizi latin-dominant non convertis (N°11 vérification) —
bdlna / khalso / khalsoo / draham / tbgho / rbnii / haw / chwala
absents de arabizi_words → conservés en latin même quand le mot
était connu de locuteurs DZ. Ajoutés dans _EXTRA_ARABIZI_WORDS.
Rappel de la logique : arabizi_words est consulté EN PREMIER dans
_step_arabizi, AVANT le test latin_dom — donc ces mots sont
désormais convertis quelle que soit la dominance de la langue.
Mots volontairement exclus (trop ambigus) : applica, m, t.
FIX-46 : الgaming / الgame → coupure artificielle —
_step_split_mixed_tokens découpait "الgaming" en "ال gaming",
puis gaming → الألعاب, produisant "ال الألعاب" (article dupliqué +
espace parasite). Correction : nouvelle étape _step_ar_latin_compounds
exécutée AVANT _step_split_mixed_tokens, via _MIXED_AR_LATIN_COMPOUNDS :
الgaming  → الألعاب
الgame    → اللعبة
الstream  → البث المباشر
الlag     → التأخير
الping    → ping
الwifi    → واي فاي
الweb     → الانترنت
Idem pour variantes majuscules (re.IGNORECASE).
Tous les correctifs antérieurs conservés (FIX-1 à FIX-44 / v3.9.3).
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import re, json, logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

MASTER_DICT_PATH = Path(__file__).parent.parent / "dictionaries" / "master_dict.json"
RULES_DICT_PATH  = Path(__file__).parent.parent / "dictionaries" / "linguistic_rules.json"

# ── Patterns regex compilés une fois globalement ─────────────────────────────
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
    # FIX-42 : regex pour wifi suivi d'un chiffre de version
    "wifi_version":  re.compile(r"\bwifi\d+\b", re.IGNORECASE),
}

_AR_VERB_PREFIXES: Set[str] = {
    "ت", "ي", "ن", "أ", "ا",
    "تت", "يت", "نت", "ست",
}

_AR_CONJ_PREFIXES: Set[str] = {
    "و", "ف", "ب", "ك", "ل",
    "ال", "لل", "بال", "كال", "فال", "وال",
}

_AR_ALL_PREFIXES: Set[str] = _AR_VERB_PREFIXES | _AR_CONJ_PREFIXES

# ── Clés techniques à protéger (ne jamais passer dans arabizi_convert) ────────
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
    # FIX-38 : ms = milliseconde
    "ms",
    # FIX-43 : XG-SPON variante
    "xg-spon", "xg-s-pon",
}

# ── Clés courtes/ambiguës à ne pas traiter dans _step_abbrev ──────────────────
_ABBREV_AMBIGUOUS_SKIP: Set[str] = {
    "re", "co", "da", "an", "ko", "mo", "go", "gb", "mb", "kb",
    "bla", "li", "ki", "w", "at", "db", "tt", "pr", "pk", "pq", "nn",
    "cc", "cv", "fb", "yt", "ok",
}

# ── Composés à protéger dès le début du pipeline (tous modes) ─────────────────
_PROTECTED_COMPOUNDS: List[str] = [
    "my idoom", "my ooredoo", "my djezzy", "my mobilis",
    "no transaction", "no transction",
    "fibre optique",
    "core network",
    "traffic shaping",
    "prime video",
    "prime vidéo",
    "google tv",
    "tv box",
    "twin box",
    "mi box",
]

# ── Mots arabizi supplémentaires (priorité absolue sur latin_dom) ─────────────
# IMPORTANT : ces mots sont cherchés EN PREMIER dans _step_arabizi,
# AVANT le test latin_dom. Ils sont donc convertis même dans un texte
# latin-dominant, contrairement aux mots non répertoriés ici.
_EXTRA_ARABIZI_WORDS: Dict[str, str] = {
    # ── Religiosity / serments ───────────────────────────────────────────────
    "nachalh":    "إن شاء الله", "nchalh":     "إن شاء الله",
    "inchalh":    "إن شاء الله", "inshalah":   "إن شاء الله",
    "inshalh":    "إن شاء الله", "inshaallah": "إن شاء الله",
    "inchalah":   "إن شاء الله", "inshaalah":  "إن شاء الله",
    "inchallah":  "إن شاء الله", "inshallah":  "إن شاء الله",
    "nchallah":   "إن شاء الله", "nshallah":   "إن شاء الله",
    "wlh":    "والله", "wlhi":    "والله", "wellah":  "والله",
    "wella":  "والله", "wallah":  "والله", "wallahi": "والله",
    "wallhy": "والله",
    "flexi":  "فليكسي", "flexili": "فليكسي",
    "nflexi": "فليكسي", "yflexi":  "فليكسي",
    # ── FIX-39 : "my" seul → préserver ──────────────────────────────────────
    "my": "my",
    # ── FIX-40 : promotion préservé ──────────────────────────────────────────
    "promotion":  "promotion",
    "promo":      "promo",
    # ── FIX-44 : arabizi hybride/pur fréquents ───────────────────────────────
    "n9drou":   "نقدرو",
    "nkhlsou":  "نخلصو",
    "nl9a":     "نلقا",
    "chhar":    "شهر",
    "fibr":     "fibre optique",
    "psq":      "parce que",
    "ndkhol":   "ندخل",
    "nkhles":   "نخلص",
    "khir":     "خير",
    "khorda":   "خردة",
    "ytl3":     "يطلع",
    "boost":    "boost",
    "fel":      "في ال",
    "balak":    "بالاك",
    # ── FIX-45 : arabizi manquants texte latin-dominant (N°11 vérif) ─────────
    "bdlna":    "بدّلنا",   # on a changé
    "khalso":   "خلصو",    # ils ont payé / terminé
    "khalsoo":  "خلصو",    # variante avec répétition
    "draham":   "دراهم",   # argent / dirhams
    "tbgho":    "تبغو",    # vous voulez
    "rbnii":    "ربّي",    # Mon Dieu
    "haw":      "هاو",     # interjection : voilà / tiens
    "chwala":   "شوالة",   # beaucoup / une tonne de
    # ── Autres arabizi fréquents ─────────────────────────────────────────────
    "khoya":    "خويا",
    "khouya":   "خويا",
    "dyal":     "ديال",
    "dial":     "ديال",
    "bhal":     "بحال",
    "mazel":    "مزال",
    "sahbi":    "صاحبي",
    "3andek":   "عندك",
    "3andi":    "عندي",
    "hadchi":   "هذا الشيء",
    "wakha":    "واخا",
    "rabi":     "ربي",
    "yehdi":    "يهدي",
    "yehdikoum":"يهديكم",
    "mn":       "من",
    "fi":       "في",
    "hdra":     "هدرة",
    "khdma":    "خدمة",
}

_EXTRA_ARABIZI_UPPER: Dict[str, str] = {
    "NACHALH":   "إن شاء الله", "NCHALH":    "إن شاء الله",
    "INCHALH":   "إن شاء الله", "INSHALAH":  "إن شاء الله",
    "INSHALLAH": "إن شاء الله", "INCHALAH":  "إن شاء الله",
    "INCHALLAH": "إن شاء الله",
    "WLH": "والله", "WELLAH": "والله", "WALLAH": "والله", "WALLAHI": "والله",
    # FIX-45 : variantes majuscules
    "BDLNA":   "بدّلنا",
    "KHALSO":  "خلصو",
    "KHALSOO": "خلصو",
    "DRAHAM":  "دراهم",
    "TBGHO":   "تبغو",
    "RBNII":   "ربّي",
    "HAW":     "هاو",
    "CHWALA":  "شوالة",
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

# ── FIX-46 : composés arabe+latin à résoudre AVANT _step_split_mixed_tokens ───
# Problème : "الgaming" → _step_split_mixed_tokens → "ال gaming"
#            puis gaming → الألعاب → résultat : "ال الألعاب" (article dupliqué)
# Solution : intercepter le composé entier avant la coupure.
_MIXED_AR_LATIN_COMPOUNDS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\bالgaming\b",  re.IGNORECASE), "الألعاب"),
    (re.compile(r"\bالgame\b",    re.IGNORECASE), "اللعبة"),
    (re.compile(r"\bالstream\b",  re.IGNORECASE), "البث المباشر"),
    (re.compile(r"\bالlag\b",     re.IGNORECASE), "التأخير"),
    (re.compile(r"\bالping\b",    re.IGNORECASE), "ping"),
    (re.compile(r"\bالwifi\b",    re.IGNORECASE), "واي فاي"),
    (re.compile(r"\bالweb\b",     re.IGNORECASE), "الانترنت"),
]

HORS_SCOPE_KEYWORDS: List[str] = [
    "embauche", "entretien d'embauche", "recrutement", "offre d'emploi",
    "sarl maxim", "hashtag#",
    "code du travail", "loi 90-11", "comité de participation",
    "عدل3", "عدل 3", "حق_الطعون", "مراجعة_الملفات",
    "المقصيون_من_عدل", "الشفافية_في_عدل",
]

# ─────────────────────────────────────────────────────────────────────────────
# Fonctions de construction (hors classe)
# ─────────────────────────────────────────────────────────────────────────────
def _load_rules(rules_path: Path) -> dict:
    if not rules_path.exists():
        logger.warning(f"rules_dict.json non trouvé : {rules_path} — règles par défaut utilisées")
        return {}
    with open(rules_path, encoding="utf-8") as f:
        return json.load(f)

def _build_negations(rules: dict) -> Set[str]:
    neg = rules.get("negations", {})
    result = set(neg.get("arabe_standard", []))
    result.update(neg.get("dialecte_algerien", []))
    result.update({"ما", "لا", "لم", "لن", "ليس", "غير", "مش", "ميش",
                   "مكانش", "ماكش", "ماهيش", "ماهوش"})
    return result

def _build_dialect_keep(rules: dict) -> Set[str]:
    dk = rules.get("dialect_keep", {})
    result: Set[str] = set()
    for v in dk.values():
        if isinstance(v, list):
            result.update(v)
    result.update({
        "راه", "راهي", "راني", "راك", "راهم", "واش", "كيفاش", "وين",
        "علاش", "قديش", "مزال", "كيما", "باه", "ديما",
        "ya", "pas", "de", "en", "avec", "depuis", "pour",
        "par", "sur", "sous", "dans", "entre", "sans", "vers",
    })
    return result

def _build_intentional_repeats(rules: dict) -> Set[str]:
    ir = rules.get("intentional_repeats", {})
    result: Set[str] = set()
    for v in ir.values():
        if isinstance(v, list):
            result.update(v)
    result.update({
        "كيف", "شوي", "برا", "هاك", "يا", "آه", "لا", "واو",
        "بزاف", "مزال", "كل", "نعم", "جدا",
        "très", "trop", "bien", "non", "oui", "si", "jamais", "encore",
    })
    return result

def _build_tech_tokens(rules: dict) -> Set[str]:
    tt = rules.get("tech_tokens", {})
    result: Set[str] = set()
    for v in tt.values():
        if isinstance(v, list):
            result.update(t.lower() for t in v)
    result.discard("cnx")
    result.update(_ABBREV_KEYS_TO_PROTECT)
    return result

def _build_arabic_prefixes(rules: dict) -> List[str]:
    ap = rules.get("arabic_prefixes", {})
    prefixes = ap.get("prefixes", [])
    if not prefixes:
        prefixes = ["وال", "فال", "بال", "كال", "لل", "ال", "و", "ف", "ب", "ك", "ل"]
    return sorted(prefixes, key=len, reverse=True)

def _build_contractions(rules: dict) -> Dict[str, str]:
    ct = rules.get("contractions", {})
    result = dict(ct.get("francais", {}))
    result.update({
        "j'ai":  "je ai",   "j\u2019ai":  "je ai",
        "c'est": "ce est",  "c\u2019est": "ce est",
        "n'est": "ne est",  "n\u2019est": "ne est",
        "n'a":   "ne a",    "n\u2019a":   "ne a",
        "qu'il": "que il",  "qu\u2019il": "que il",
        "qu'on": "que on",  "qu\u2019on": "que on",
    })
    return result

def _is_regex_key(k: str) -> bool:
    return bool(_RE["regex_key"].search(k))

def _build_plain_dict(raw: dict) -> Dict[str, str]:
    return {k: v for k, v in raw.items() if not _is_regex_key(k)}

def _build_protected_words(rules: dict) -> Set[str]:
    pw = rules.get("protected_words", {})
    result: Set[str] = set()
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

def _build_stopwords_from_rules(rules: dict, negations: Set[str], dialect_keep: Set[str]) -> Set[str]:
    sw_section = rules.get("stopwords", {})
    sw: Set[str] = set()
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

def _build_contraction_re(contractions: Dict[str, str]) -> re.Pattern:
    apostrophe_keys = [k for k in contractions if "'" in k or "\u2019" in k]
    if not apostrophe_keys:
        return re.compile(r"(?!)")
    escaped = sorted([re.escape(k) for k in apostrophe_keys], key=len, reverse=True)
    return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)

# ── Regex de protection des composés (pré-pipeline) ───────────────────────────
_COMPOUND_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(re.escape(c), re.IGNORECASE), c)
    for c in sorted(_PROTECTED_COMPOUNDS, key=len, reverse=True)
]

# ─────────────────────────────────────────────────────────────────────────────
class TextNormalizer:
    # ─────────────────────────────────────────────────────────────────────────
    def __init__(
        self,
        mode: str = "arabert",
        dict_path: Path = MASTER_DICT_PATH,
        rules_path: Path = RULES_DICT_PATH,
        remove_stopwords: bool = False,
        disable_arabizi: bool = False,
        preserve_latin_darja: bool = False,  # ✅ FIX-47 : NOUVEAU PARAMÈTRE v3.9.5
    ):
        assert mode in ("arabert", "full"), "mode doit être 'arabert' ou 'full'"
        self.mode      = mode
        self.remove_sw = remove_stopwords or (mode == "full")
        self.disable_arabizi = disable_arabizi
        self.preserve_latin_darja = preserve_latin_darja  # ✅ FIX-47
        
        with open(dict_path, encoding="utf-8") as f:
            d = json.load(f)
        
        rules = _load_rules(rules_path)
        
        self._negations          = _build_negations(rules)
        self._dialect_keep       = _build_dialect_keep(rules)
        self._intentional_rep    = _build_intentional_repeats(rules)
        self._tech_tokens        = _build_tech_tokens(rules)
        self._arabic_prefixes    = _build_arabic_prefixes(rules)
        self._contractions       = _build_contractions(rules)
        self._contraction_re     = _build_contraction_re(self._contractions)
        
        self.unicode_map: Dict[str, str] = d["unicode_arabic"]
        self.unicode_map.setdefault("\u0629", "\u0647")
        self.unicode_map.setdefault("\u0624", "\u0648")
        
        self.digrams: Dict[str, str] = d["arabizi_digrams"]
        self.monograms: Dict[str, str] = {
            k: v for k, v in d["arabizi_monograms"].items()
            if not (len(k) == 1 and k.isalpha())
        }
        
        raw_arabizi = d["arabizi_words"]
        self.arabizi_words: Dict[str, str] = {**raw_arabizi, **_EXTRA_ARABIZI_WORDS}
        self.arabizi_words = {k: v for k, v in self.arabizi_words.items()
                              if not k.startswith("_")}
        
        self.arabizi_upper: Dict[str, str] = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}
        self.emojis:        Dict[str, str] = d["emojis"]
        self.abbreviations: Dict[str, str] = d["abbreviations"]
        self.telecom: Dict[str, str] = _build_plain_dict(d["telecom_tech"])
        self.units_map: Dict[str, str] = d["units"]
        
        nv = d["network_variants"]
        self._net_form: str       = nv["normalized_form"]
        self._net_all:  List[str] = nv["latin"] + nv["arabic"]
        
        mixed_clean = {k: v for k, v in d["mixed_ar_fr_regex"].items()
                       if not k.startswith("_")}
        self._mixed_pats: List[Tuple[re.Pattern, str]] = self._compile_dict(mixed_clean)
        self._fr_pats:    List[Tuple[re.Pattern, str]] = self._compile_dict(
            d["french_corrections_regex"], flags=re.IGNORECASE | re.UNICODE
        )
        
        escaped_net = [re.escape(v) for v in self._net_all if v]
        self._net_re = re.compile(
            rf'\b({"|".join(escaped_net)})\b', re.IGNORECASE
        ) if escaped_net else None
        
        combined = {**self.digrams, **self.monograms}
        self._arabizi_seq: List[Tuple[str, str]] = sorted(
            combined.items(), key=lambda x: len(x[0]), reverse=True
        )
        self._arabizi_upper_sorted: List[Tuple[str, str]] = sorted(
            self.arabizi_upper.items(), key=lambda x: len(x[0]), reverse=True
        )
        
        _all_vals: List[str] = []
        for v in list(self.telecom.values()) + list(self.abbreviations.values()):
            if v:
                _all_vals.extend(v.split())
        
        self._protected: Set[str] = _build_protected_words(rules)
        self._protected.update(t.lower() for t in _all_vals)
        self._protected.update(self._tech_tokens)
        
        self._stopwords: Set[str] = (
            _build_stopwords_from_rules(rules, self._negations, self._dialect_keep)
            if self.remove_sw else set()
        )
        
        total = sum([
            len(self.unicode_map), len(self.digrams), len(self.monograms),
            len(self.arabizi_words), len(self.arabizi_upper),
            len(self.emojis), len(self.abbreviations), len(self.telecom),
            len(self.units_map), len(self._mixed_pats), len(self._fr_pats),
            len(self._net_all),
        ])
        
        rules_total = sum([
            len(self._negations), len(self._dialect_keep),
            len(self._intentional_rep), len(self._tech_tokens),
            len(self._contractions), len(self._protected),
        ])
        
        logger.info(
            f"TextNormalizer [{mode}] v3.9.5 — {total} entrées dict | "
            f"{rules_total} entrées rules | {dict_path.name} + {rules_path.name} | "
            f"disable_arabizi={self.disable_arabizi} | preserve_latin_darja={self.preserve_latin_darja}"
        )

    # ── Helpers ───────────────────────────────────────────────────────────────
    @staticmethod
    def _compile_dict(d: dict, flags: int = re.UNICODE) -> List[Tuple[re.Pattern, str]]:
        result = []
        for pat, repl in d.items():
            try:
                result.append((re.compile(pat, flags), repl))
            except re.error as e:
                logger.warning(f"Pattern ignoré {pat!r}: {e}")
        return result

    def _split_prefix(self, word: str) -> Tuple[str, str]:
        for p in self._arabic_prefixes:
            if word.startswith(p) and len(word) > len(p) + 1:
                return p, word[len(p):]
        return "", word

    def _lookup(self, word: str, dct: Dict[str, str]) -> Optional[str]:
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

    def _is_latin_dominant(self, text: str) -> bool:
        lat = sum(1 for c in text if "a" <= c.lower() <= "z")
        ar  = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
        tot = lat + ar
        if not tot:
            return False
        threshold = 0.70 if len(text) < 30 else 0.75
        return (lat / tot) > threshold

    @staticmethod
    def _is_proper_noun_token(tok: str) -> bool:
        """FIX-41 : heuristique nom propre (Capitalized, pas acronyme, >=3 chars)."""
        if len(tok) < 3:
            return False
        if not _RE["pure_latin"].match(tok):
            return False
        return tok[0].isupper() and not tok.isupper()

    @staticmethod
    def _is_hors_scope(text: str) -> bool:
        if not text:
            return False
        lo = text.lower()
        return any(kw.lower() in lo for kw in HORS_SCOPE_KEYWORDS)

    def _protect_numbers(self, text: str) -> Tuple[Dict[str, str], str]:
        protected: Dict[str, str] = {}
        counter = [0]
        def _protect(m: re.Match) -> str:
            key = f"__NP{counter[0]}__"
            counter[0] += 1
            protected[key] = m.group(0)
            return key
        text = _RE["num_separator"].sub(_protect, text)
        text = re.sub(r"(?<!\d)(\d+)\s*/\s*(\d+)(?!\d)", _protect, text)
        return protected, text

    def _protect_compounds_pre(self, text: str) -> Tuple[Dict[str, str], str]:
        """FIX-39 : protège les composés avant tout le pipeline."""
        placeholders: Dict[str, str] = {}
        counter = [0]
        for pat, original in _COMPOUND_PATTERNS:
            if pat.search(text):
                ph = f"__CPD{counter[0]}__"
                counter[0] += 1
                placeholders[ph] = original
                text = pat.sub(ph, text)
        return placeholders, text

    def _dedup_tokens(self, text: str) -> str:
        tokens = text.split()
        if len(tokens) < 2:
            return text
        result = [tokens[0]]
        for i in range(1, len(tokens)):
            prev    = result[-1]
            curr    = tokens[i]
            prev_lo = prev.lower()
            curr_lo = curr.lower()
            if curr_lo == prev_lo:
                if curr_lo in self._intentional_rep:
                    result.append(curr)
                    continue
                has_ar_curr = bool(_RE["has_arabic"].search(curr))
                if (not has_ar_curr
                        and len(curr) >= 4
                        and len(curr) < len(prev)
                        and prev_lo.endswith(curr_lo)):
                    continue
                if (has_ar_curr
                        and len(curr) >= 5
                        and len(curr) < len(prev)
                        and prev.endswith(curr)):
                    m = _RE["arabic_prefix"].match(prev)
                    if m and m.group(2) == curr:
                        result.append(curr)
                        continue
                    stripped = prev[: len(prev) - len(curr)]
                    if stripped in _AR_ALL_PREFIXES and len(stripped) >= 2:
                        result.append(curr)
                        continue
                result.append(curr)
                continue
            result.append(curr)
        return " ".join(result)

    # ── Pipeline ──────────────────────────────────────────────────────────────
    def normalize(self, text: str) -> str:
        if not isinstance(text, str) or not text.strip():
            return ""
        try:
            protected_nums, text = self._protect_numbers(text)
            protected_cpd, text  = self._protect_compounds_pre(text)
            text = self._step_emojis(text)
            text = self._step_unicode_arabic(text)
            text = self._step_extra_ar(text)
            # FIX-46 : résoudre les composés ar+latin AVANT la coupure
            text = self._step_ar_latin_compounds(text)
            # FIX-42 : protéger wifi\d+ avant mixed_pats
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
        except Exception as exc:
            logger.error(f"normalize() erreur: {exc} | texte: {text[:80]!r}")
        return text.strip()

    # ── Étapes individuelles ──────────────────────────────────────────────────
    def _step_emojis(self, text: str) -> str:
        for emoji, word in self.emojis.items():
            if emoji in text:
                text = text.replace(emoji, f" {word} ")
        return text

    def _step_extra_ar(self, text: str) -> str:
        for pat, repl in _EXTRA_AR_PATTERNS:
            text = pat.sub(repl, text)
        return text

    def _step_ar_latin_compounds(self, text: str) -> str:
        """
        FIX-46 : normalise les composés arabe+latin collés (ex: الgaming)
        AVANT que _step_split_mixed_tokens ne les découpe artificiellement.
        Ordre dans le pipeline : après _step_extra_ar, avant _mixed_pats.
        """
        for pat, repl in _MIXED_AR_LATIN_COMPOUNDS:
            text = pat.sub(repl, text)
        return text

    def _step_units(self, text: str) -> str:
        def _repl(m: re.Match) -> str:
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

    def _step_abbrev(self, text: str) -> str:
        latin_dom = self._is_latin_dominant(text)
        tokens, result = text.split(), []
        i = 0
        while i < len(tokens):
            tok  = tokens[i]
            m    = _RE["trail_punct"].match(tok)
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
                    if (latin_dom
                            and dct is self.telecom
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

    def _step_french(self, text: str) -> str:
        text = self._contraction_re.sub(
            lambda m: self._contractions.get(m.group(0).lower(), m.group(0)), text
        )
        for pat, repl in self._fr_pats:
            text = pat.sub(repl, text)
        return text

    def _step_split_mixed_tokens(self, text: str) -> str:
        text = _RE["ar_then_lat"].sub(r"\1 \2", text)
        text = _RE["lat_then_ar"].sub(r"\1 \2", text)
        return text

    # ── ✅ MODIFIÉ v3.9.5 : Gestion preserve_latin_darja ───────────────────────
    def _step_arabizi(self, text: str) -> str:
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
                
                # ✅ FIX-47 : preserve_latin_darja → SAUTE TOUS les lookups Arabizi
                if self.preserve_latin_darja:
                    result.append(tok)
                    continue
                
                # Ancienne logique (seulement si preserve_latin_darja=False)
                # Recherche dans arabizi_words EN PREMIER — AVANT le test latin_dom.
                # Garantit la conversion même en texte latin-dominant.
                w = self.arabizi_words.get(lo)
                if w:
                    if latin_dom and len(lo) <= 2 and lo in _AMBIGUOUS_SHORT:
                        result.append(tok)
                    else:
                        result.append(w)
                    continue
                
                for k, v in self._arabizi_upper_sorted:
                    if tok.upper() == k:
                        result.append(v)
                        break
                else:
                    if self._is_proper_noun_token(tok):  # FIX-41
                        result.append(tok); continue
                    if self.disable_arabizi or latin_dom:  # protection texte latin
                        result.append(tok); continue
                    result.append(self._arabizi_convert(tok))
                continue
            
            if _RE["arabizi_hyb"].match(tok):
                # ✅ FIX-47 : preserve_latin_darja → garde token original
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

    def _arabizi_convert(self, token: str) -> str:
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

    def _step_unicode_arabic(self, text: str) -> str:
        text = _RE["diacritics"].sub("", text)
        text = _RE["tatweel"].sub("", text)
        for variant, canonical in self.unicode_map.items():
            text = text.replace(variant, canonical)
        return text

    def _step_cleanup(self, text: str) -> str:
        text = _RE["arab_digit"].sub(r"\1 \2", text)
        text = _RE["digit_arab"].sub(r"\1 \2", text)
        def _fuse_spaced(m: re.Match) -> str:
            return m.group(0).replace(" ", "")
        text = _RE["spaced_digits"].sub(_fuse_spaced, text)
        text = _RE["arabic_digits_spaced"].sub(_fuse_spaced, text)
        text = _RE["rep_chars"].sub(lambda m: m.group(1) * 2, text)
        text = _RE["rep_punct"].sub(lambda m: m.group(1), text)
        text = _RE["whitespace"].sub(" ", text).strip()
        return text

    def _step_stopwords(self, text: str) -> str:
        if not self._stopwords:
            return text
        placeholders: Dict[str, str] = {}
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
            if (w.isdigit() or w in keep or lo in keep or lo not in self._stopwords):
                result.append(w)
            elif len(w) == 1 and _RE["has_arabic"].match(w):
                result.append(w)
        text = " ".join(result)
        for ph, original in placeholders.items():
            text = text.replace(ph, original)
        return text

    # ── DataFrame ─────────────────────────────────────────────────────────────
    def normalize_df(self, df, col="comment", out_col=None,
                     drop_empty=True, filter_hidden=True):
        out_col = out_col or f"{col}_normalized"
        df = df.copy()
        if filter_hidden and "Commentaire Modérateur" in df.columns:
            df = df[df["Commentaire Modérateur"].str.strip().str.lower() != "masquer"]
        df = df.dropna(subset=[col])
        df[out_col] = df[col].apply(self.normalize)
        if drop_empty:
            df = df[df[out_col].str.len() > 0]
        logger.info(f"{len(df)} lignes conservées après normalisation")
        return df

    # ── Spark UDFs ────────────────────────────────────────────────────────────
    def spark_udf(self):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        norm = self
        @udf(returnType=StringType())
        def _udf(text):
            return norm.normalize(text) if text else ""
        return _udf

    def spark_hors_scope_udf(self):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import BooleanType
        @udf(returnType=BooleanType())
        def _udf(text):
            return TextNormalizer._is_hors_scope(text) if text else False
        return _udf

# ── Fonctions de commodité ────────────────────────────────────────────────────
_default_normalizer: Optional[TextNormalizer] = None
_default_normalizer_preserve: Optional[TextNormalizer] = None  # ✅ v3.9.5

def normalize(text, mode="arabert", dict_path=MASTER_DICT_PATH,
              rules_path=RULES_DICT_PATH, preserve_latin_darja=False):  # ✅ v3.9.5
    global _default_normalizer, _default_normalizer_preserve
    
    if preserve_latin_darja:
        if _default_normalizer_preserve is None or _default_normalizer_preserve.mode != mode:
            _default_normalizer_preserve = TextNormalizer(
                mode=mode, dict_path=dict_path, rules_path=rules_path,
                preserve_latin_darja=True
            )
        return _default_normalizer_preserve.normalize(text)
    else:
        if _default_normalizer is None or _default_normalizer.mode != mode:
            _default_normalizer = TextNormalizer(
                mode=mode, dict_path=dict_path, rules_path=rules_path
            )
        return _default_normalizer.normalize(text)

def normalize_df(df, col="comment", mode="arabert",
                 dict_path=MASTER_DICT_PATH, rules_path=RULES_DICT_PATH,
                 preserve_latin_darja=False, **kwargs):  # ✅ v3.9.5
    n = TextNormalizer(
        mode=mode, dict_path=dict_path, rules_path=rules_path,
        preserve_latin_darja=preserve_latin_darja
    )
    return n.normalize_df(df, col=col, **kwargs)

# ── Tests de régression v3.9.5 ────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    
    dict_path  = Path(sys.argv[1]) if len(sys.argv) > 1 else MASTER_DICT_PATH
    rules_path = Path(sys.argv[2]) if len(sys.argv) > 2 else RULES_DICT_PATH
    
    for p in [dict_path, rules_path]:
        if not p.exists():
            print(f"⚠️  Fichier non trouvé : {p}")
            sys.exit(1)
    
    print("\n" + "=" * 70)
    print("  TextNormalizer v3.9.5 — TESTS RÉGRESSION + FIX-47")
    print("=" * 70)
    
    N = TextNormalizer(mode="arabert", dict_path=dict_path, rules_path=rules_path)
    N_preserve = TextNormalizer(
        mode="arabert", dict_path=dict_path, rules_path=rules_path,
        preserve_latin_darja=True
    )
    
    # Tests avec Arabizi NORMAL (comportement par défaut)
    cases_normal = [
        ("slm converti", "slm khouya", "سلام", None),
        ("khalso converti", "khalso walo", "خلصو", None),
        ("ghir converti", "ghir hedik", "غير", None),
        ("3la converti", "3la khaterha", "على", None),
    ]
    
    # Tests avec preserve_latin_darja=True (FIX-47)
    cases_preserve = [
        ("slm gardé latin", "slm khouya", None, "سلام"),    # slm gardé en latin
        ("khalso gardé latin", "khalso walo", None, "خلصو"), # khalso gardé
        ("ghir gardé latin", "ghir hedik", None, "غير"),     # ghir gardé
        ("3la gardé latin", "3la khaterha", None, "على"),    # 3la gardé
        ("3yina gardé", "3yina walah", None, "عينا"),        # 3yina gardé
        ("kayen gardé", "kayen ness", None, "كاين"),         # kayen gardé
    ]
    
    print("\n── Tests mode arabert (Arabizi NORMAL) ───────────────────────────")
    all_ok = True
    for label, inp, must_have, must_not in cases_normal:
        out = N.normalize(inp)
        ok1 = (must_have in out) if must_have else True
        ok2 = (must_not not in out) if must_not else True
        ok = ok1 and ok2
        if not ok:
            all_ok = False
        print(f"  {'✓' if ok else '✗ FAIL'}  [{label}]")
        if not ok:
            print(f"         IN : {inp}")
            print(f"         OUT: {out}")
    
    print("\n── Tests mode arabert (preserve_latin_darja=True - FIX-47) ───────")
    for label, inp, must_have, must_not in cases_preserve:
        out = N_preserve.normalize(inp)
        ok1 = (must_have in out) if must_have else True
        ok2 = (must_not not in out) if must_not else True
        ok = ok1 and ok2
        if not ok:
            all_ok = False
        print(f"  {'✓' if ok else '✗ FAIL'}  [{label}]")
        if not ok:
            print(f"         IN : {inp}")
            print(f"         OUT: {out}")
    
    # Tests de régression v3.9.4 (conservés)
    cases_regression = [
        ("FIX-46a الgaming→الألعاب", "ماهيش مراعية اصحاب عرض الgaming", "الألعاب", "ال الألعاب"),
        ("FIX-42 wifi5 préservé", "يستعمل مودم wifi5 لا يلائم السرعة", "wifi5", None),
        ("FIX-35a my idoom", "تطبيق my idoom يحتاج إلى خاصية", "my idoom", None),
    ]
    
    print("\n── Tests régression v3.9.4 (conservés) ───────────────────────────")
    for label, inp, must_have, must_not in cases_regression:
        out = N_preserve.normalize(inp)
        ok1 = (must_have in out) if must_have else True
        ok2 = (must_not not in out) if must_not else True
        ok = ok1 and ok2
        if not ok:
            all_ok = False
        print(f"  {'✓' if ok else '✗ FAIL'}  [{label}]")
        if not ok:
            print(f"         IN : {inp}")
            print(f"         OUT: {out}")
    
    print("\n" + "=" * 70)
    print("  ✅ v3.9.5 OK — tous les tests passent" if all_ok else "  ❌ Régressions détectées")
    print("=" * 70)