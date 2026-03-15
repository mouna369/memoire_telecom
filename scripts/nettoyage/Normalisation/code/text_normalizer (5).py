"""
text_normalizer_v3.8.2.py
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
Changelog v3.8 :

  FIX-1  CHIFFRE_DISPARU (615 cas)
         Protection num_separator (15:00, 20:48, 90-11, 3-7) déplacée
         EN TÊTE de pipeline via _protect_numbers() — avant tout traitement.
         Restauration finale après _dedup_tokens.

  FIX-2  TRONCATURE CHIFFRES (8000→800)
         rep_chars exclut désormais les chiffres :
         (.)\1{2,}  →  (?<!\d)(.)\1{2,}(?!\d)
         Empêche 000→00 qui tronquait les montants (8000 DA etc.).

  FIX-3  DOUBLON_EXACT جدا جدا (585 cas)
         جدا ajouté au fallback minimal de _build_intentional_repeats.
         (déjà dans rules JSON mais le fallback était incomplet)

  FIX-4  DOUBLON_SUFFIXE يتعلمباي (11 cas)
         _dedup_tokens : la suppression du suffixe arabe requiert maintenant
         que stripped soit dans _AR_ALL_PREFIXES ET len(stripped) >= 2.
         Évite la fusion de mots séparés par une ponctuation collée.

  FIX-5  RÉSIDU_ARABIZI slm/mkch entourés d'arabe (78 cas)
         _step_arabizi : lookup arabizi_words effectué EN PREMIER,
         avant le test latin_dom. Un mot connu est toujours converti
         quel que soit le contexte dominant du texte.

  FIX-6  TEXTE LATIN DOMINANT — tokens hybrides convertis à tort
         (y9olo→ق, maghlou9a→غق, wnel9awha→ق)
         _step_arabizi : pour les textes latin-dominant, les tokens
         arabizi_hyb sont d'abord cherchés dans arabizi_words ;
         si non trouvés, conservés tels quels (au lieu de _arabizi_convert).

  FIX-7  ئ→ي SUPPRIMÉ du unicode_map (causait سيئة→سييه)
         Le setdefault ئ→ي retiré de __init__.
         ة→ه conservé (voulu pour AraBERT).

  FIX-8  بينج→ping ajouté (variante de بينغ)
         Dans master_dict + telecom_tech (géré côté dictionnaire).

  FIX-9  بونيص/بونص→bonus (géré côté dictionnaire).

  FIX-10 الكونيكسيو / الكنكسيون→الانترنت (géré côté dictionnaire).

  FIX-11 نت arabe seul→الانترنت (géré côté dictionnaire).

  FIX-12 انتارنات/انتارنت→الانترنت (géré côté dictionnaire).

  FIX-13 Dialecte algérien latin enrichi dans arabizi_words
         (kifech, homa, y9olo, maghlou9a, nastana, ghal9in, thir, kho…)

  STABLE : tous les fixes v3.7 conservés (FIX-A→E, pipeline order,
           rules_dict.json, contractions, protected_words…)
  STABLE : master_dict.json v3.6 (nouveau)

  BUG-C  wifi 6/7 : slash avec espaces maintenant protégé
  BUG-E  ta3/tae tokens courts conservés en texte latin-dominant

  FIX-19 connexion variantes (كوناكسيون/كونكسيو/كوناكسيو)
         ADSL (لادياسال), fibre (فيبروتيك/فايبر/فيبر)
         modem (مودام/المودام), Wi-Fi (ويفي/يفي)
         débit (ديبي), ميقا→Mo, جيقا→Go, google tv, الفكس

  FIX-20 arabizi_words — majozch, masma3nach, wana, rahom, 3lina,
         3awdoulna, s7ab, nkhdmou, tdkhol, tl3b, dayrin, kdabin,
         barkaw, lo9doum, sebtembre, g33 et autres (189 total)

  FIX-21 telecom_tech — nokia/tenda/zte/huawei, mi box, airon,
         ethernet, gigabit, starlink, google tv, wi-fi, débit

  FIX-22 french_corrections — sebtembre→septembre

  FIX-23 protected_words — bonus, marques(nokia/tenda/google/starlink)
  FIX-24 stopwords anglais — retrait we/get/want/good/bad/very/all/so
  FIX-25 dialect_keep — واشمن/شحال/négations composées arabes
  FIX-26 intentional_repeats.francais — mm

  FIX-14 gaming/télécom arabes (master_dict v3.7)
         ڤايمينڤ→gaming, سيرفر→serveur, فورت نايت→Fortnite,
         لارام→RAM, لا لين→ligne, تيفي بوكس→TV Box,
         بروموسيو→promotion, موديم→مودم, ولله→والله,
         نتفلكس→Netflix, امزون→Amazon, prime vidéo protégé.

  FIX-15 telecom_tech enrichi (master_dict v3.7)
         twinbox, tv box, fortnite, netflix, prime video,
         4k/8k/hd/fhd/uhd, ram/rom/cpu/gpu, android/ios, serveur.

  FIX-16 جد ajouté à intentional_repeats (rules v1.1)
         جد جد جد conservé (= "très très très" dialectal).

  FIX-17 protected_words.streaming (rules v1.1)
         prime, netflix, amazon, fortnite, twinbox, ram,
         heures (évite heures→ساعات dans phrases françaises).

  FIX-18 tech_tokens.gaming_materiel (rules v1.1)
         4k, hd, ram, android, ios, serveur, fortnite, netflix.

  FIX-27 normalize_df() — paramètre filter_hors_scope supprimé.
         Le filtre hors-scope est géré en amont par la Cellule 9
         du pipeline Spark via _is_hors_scope() appliqué sur pandas.
         Évite toute double application sur le même corpus.

  FIX-28 TOKENS COLLÉS arabe+latin (slm/wlh non convertis — 21/25 cas)
         Vérification qualitative : 'الجزائرSlm' et 'الجزائرwlh' non
         convertis car _RE["has_arabic"] matchait le token entier.
         Nouvelle étape _step_split_mixed_tokens() insérée AVANT
         _step_arabizi dans normalize() :
           ([\u0600-\u06FF])([a-zA-Z]) → \1 \2
           ([a-zA-Z])([\u0600-\u06FF]) → \1 \2
         Ainsi 'الجزائرSlm' → 'الجزائر Slm' → 'الجزائر سلام'.

  FIX-29 بونيص/بونص avec préfixes arabes (10/17 cas échouaient)
         Vérification qualitative : 'والبونيص', 'لبونص', 'البونيص'
         non convertis car les patterns \b ne capturaient pas les
         formes avec préfixes collés.
         Correction dans mixed_ar_fr_regex du master_dict.json :
         ajout de 4 variantes préfixées (وبونيص / بالبونيص / etc.).
         NB : correction côté dictionnaire uniquement.

  FIX-30 BUG-E trop large — slm/wlh/bzf non convertis (18/25 cas)
         Vérification qualitative : tokens arabizi de 3 chars
         ('slm', 'wlh', 'bzf', 'kho'…) non convertis en texte
         latin-dominant car BUG-E protégeait tout mot len <= 3.
         Correction dans _step_arabizi :
           Avant : len(lo) <= 3 → conserver
           Après : len(lo) <= 2 ET lo in _AMBIGUOUS_SHORT → conserver
         Les mots ambigus explicites {'ki','el','da','li','w','dz'}
         restent protégés. Tous les autres mots connus sont convertis.

  FIX-31 'mais' supprimé en mode full (D7 stopwords agressifs)
         Vérification qualitative : 'mais' perdu sur N°50
         ('on m a branche la fibre mais on m a pas libere la ligne').
         'mais' est un connecteur logique d'opposition essentiel.
         Correction dans linguistic_rules.json :
         'mais' retiré de stopwords.francais,
         ajouté dans dialect_keep.francais_garde.
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
    # FIX-2 : exclure les chiffres de rep_chars (empêche 8000→800)
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
    # FIX-1 : protection séquences numériques avec séparateur
    "num_separator": re.compile(r"(?<!\d)(\d+)([-:/])(\d+)(?!\d)"),
    # FIX-28 : séparation tokens collés arabe+latin (الجزائرSlm → الجزائر Slm)
    "ar_then_lat":   re.compile(r"([\u0600-\u06FF])([a-zA-Z])"),
    "lat_then_ar":   re.compile(r"([a-zA-Z])([\u0600-\u06FF])"),
}

# ── Préfixes verbaux et conjonctifs arabes ────────────────────────────────────
_AR_VERB_PREFIXES: Set[str] = {
    "ت", "ي", "ن", "أ", "ا",
    "تت", "يت", "نت", "ست",
}
_AR_CONJ_PREFIXES: Set[str] = {
    "و", "ف", "ب", "ك", "ل",
    "ال", "لل", "بال", "كال", "فال", "وال",
}
_AR_ALL_PREFIXES: Set[str] = _AR_VERB_PREFIXES | _AR_CONJ_PREFIXES

# ── Variantes إن شاء الله et flexi (latines) ─────────────────────────────────
_EXTRA_ARABIZI_WORDS: Dict[str, str] = {
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
}

_EXTRA_ARABIZI_UPPER: Dict[str, str] = {
    "NACHALH":   "إن شاء الله", "NCHALH":    "إن شاء الله",
    "INCHALH":   "إن شاء الله", "INSHALAH":  "إن شاء الله",
    "INSHALLAH": "إن شاء الله", "INCHALAH":  "إن شاء الله",
    "INCHALLAH": "إن شاء الله",
    "WLH": "والله", "WELLAH": "والله", "WALLAH": "والله", "WALLAHI": "والله",
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

# ── Mots-clés hors-scope télécom ─────────────────────────────────────────────
HORS_SCOPE_KEYWORDS: List[str] = [
    "embauche", "entretien d'embauche", "recrutement", "offre d'emploi",
    "sarl maxim", "hashtag#",
    "code du travail", "loi 90-11", "comité de participation",
    "عدل3", "عدل 3", "حق_الطعون", "مراجعة_الملفات",
    "المقصيون_من_عدل", "الشفافية_في_عدل",
]


# ── Chargement rules_dict.json ────────────────────────────────────────────────

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
    # FIX-3 : جدا ajouté au fallback minimal
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
            result.update(v)
    result.discard("cnx")  # FIX-C v3.6 : cnx doit passer par abbreviations
    result.update({
        "adsl", "vdsl", "ftth", "fttb", "xgs", "xgs-pon", "pon",
        "dsl", "lte", "volte", "dns", "ont",
        "wifi", "4g", "3g", "2g", "5g",
        "mbps", "kbps", "gbps", "mbs",       # mbs ajouté FIX-5
        "idoom", "djezzy", "mobilis", "ooredoo",
        "fibre", "fiber", "febre",
        "ping",                               # ping protégé
    })
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


def _build_protected_words(rules: dict) -> Set[str]:
    pw = rules.get("protected_words", {})
    result: Set[str] = set()
    for v in pw.values():
        if isinstance(v, list):
            result.update(t.lower() for t in v)
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
        "il", "ils", "de", "me", "le", "la", "les", "du",
        "une", "un", "qui", "que", "dans", "sur", "pour", "par",
        "avec", "sans", "sont", "est", "etait", "avait", "peut",
        "font", "avez", "avons",
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


# ─────────────────────────────────────────────────────────────────────────────
class TextNormalizer:
# ─────────────────────────────────────────────────────────────────────────────

    def __init__(
        self,
        mode: str = "arabert",
        dict_path: Path = MASTER_DICT_PATH,
        rules_path: Path = RULES_DICT_PATH,
        remove_stopwords: bool = False,
    ):
        assert mode in ("arabert", "full"), "mode doit être 'arabert' ou 'full'"
        self.mode      = mode
        self.remove_sw = remove_stopwords or (mode == "full")

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

        # ── unicode_arabic ────────────────────────────────────────────────
        self.unicode_map: Dict[str, str] = d["unicode_arabic"]
        self.unicode_map.setdefault("\u0629", "\u0647")   # ة → ه  (AraBERT)
        # FIX-7 : ئ→ي SUPPRIMÉ — causait سيئة→سييه
        # NE PAS ajouter : self.unicode_map.setdefault("\u0626", "\u064A")
        self.unicode_map.setdefault("\u0624", "\u0648")   # ؤ → و

        self.digrams: Dict[str, str] = d["arabizi_digrams"]
        self.monograms: Dict[str, str] = {
            k: v for k, v in d["arabizi_monograms"].items()
            if not (len(k) == 1 and k.isalpha())
        }

        self.arabizi_words: Dict[str, str] = {**d["arabizi_words"], **_EXTRA_ARABIZI_WORDS}
        # Nettoyer les clés _comment_* du JSON
        self.arabizi_words = {k: v for k, v in self.arabizi_words.items()
                              if not k.startswith("_")}
        self.arabizi_upper: Dict[str, str] = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}
        self.emojis:        Dict[str, str] = d["emojis"]
        self.abbreviations: Dict[str, str] = d["abbreviations"]
        self.telecom:       Dict[str, str] = d["telecom_tech"]
        self.units_map:     Dict[str, str] = d["units"]

        nv = d["network_variants"]
        self._net_form: str       = nv["normalized_form"]
        self._net_all:  List[str] = nv["latin"] + nv["arabic"]

        # Filtrer les clés _comment_* dans mixed_ar_fr_regex
        mixed_clean = {k: v for k, v in d["mixed_ar_fr_regex"].items()
                       if not k.startswith("_")}
        self._mixed_pats: List[Tuple[re.Pattern, str]] = self._compile_dict(mixed_clean)
        self._fr_pats:    List[Tuple[re.Pattern, str]] = self._compile_dict(
            d["french_corrections_regex"], flags=re.IGNORECASE | re.UNICODE
        )

        escaped = [re.escape(v) for v in self._net_all if v]
        self._net_re = re.compile(
            rf'\b({"|".join(escaped)})\b', re.IGNORECASE
        ) if escaped else None

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
        self._protected.update(
            t.lower() for t in (
                _all_vals
                + list(self.telecom.keys())
                + list(self.abbreviations.keys())
                + list(self._tech_tokens)
            )
        )

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
            f"TextNormalizer [{mode}] v3.8.2 — {total} entrées dict | "
            f"{rules_total} entrées rules | {dict_path.name} + {rules_path.name}"
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
        return (lat / tot) > (0.55 if len(text) < 20 else 0.60) if tot else False

    @staticmethod
    def _is_hors_scope(text: str) -> bool:
        if not text:
            return False
        lo = text.lower()
        return any(kw.lower() in lo for kw in HORS_SCOPE_KEYWORDS)

    # ── FIX-1 : protection numérique en tête de pipeline ─────────────────────
    def _protect_numbers(self, text: str) -> Tuple[Dict[str, str], str]:
        """
        Protège les séquences numériques avec séparateur (15:00, 20:48, 90-11)
        AVANT tout traitement. Retourne (dict_protections, texte_modifié).
        La restauration se fait après _dedup_tokens dans normalize().
        """
        protected: Dict[str, str] = {}
        counter = [0]

        def _protect(m: re.Match) -> str:
            key = f"__NP{counter[0]}__"
            counter[0] += 1
            protected[key] = m.group(0)
            return key

        text = _RE["num_separator"].sub(_protect, text)
        # FIX-BUG-C : slash avec espaces optionnels entre chiffres (ex: wifi 6 / 7)
        text = re.sub(r"(?<!\d)(\d+)\s*/\s*(\d+)(?!\d)", _protect, text)
        return protected, text

    # ── FIX-4 : _dedup_tokens avec guard suffixe ──────────────────────────────
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

            # Doublon exact
            if curr_lo == prev_lo:
                if curr_lo in self._intentional_rep:
                    result.append(curr)
                continue

            has_ar_curr = bool(_RE["has_arabic"].search(curr))

            # Suffixe latin
            if (not has_ar_curr
                    and len(curr) >= 4
                    and len(curr) < len(prev)
                    and prev_lo.endswith(curr_lo)):
                continue

            # FIX-4 : suffixe arabe — guard len(stripped) >= 2
            if (has_ar_curr
                    and len(curr) >= 5
                    and len(curr) < len(prev)
                    and prev.endswith(curr)):

                m = _RE["arabic_prefix"].match(prev)
                if m and m.group(2) == curr:
                    result.append(curr)
                    continue

                stripped = prev[: len(prev) - len(curr)]
                # FIX-4 : stripped doit être un préfixe connu ET >= 2 chars
                if stripped in _AR_ALL_PREFIXES and len(stripped) >= 2:
                    result.append(curr)
                    continue

                # Sinon : ni suppression ni ajout du curr seul — conserver prev
                # Le curr est un vrai mot distinct, pas un suffixe parasite
                result.append(curr)
                continue

            result.append(curr)

        return " ".join(result)

    # ── Pipeline v3.8 ─────────────────────────────────────────────────────────
    # ORDRE :
    #   0.  _protect_numbers        (FIX-1 — AVANT TOUT)
    #   1.  emojis
    #   2.  unicode_arabic          (FIX-7 : sans ئ→ي)
    #   3.  extra_ar_patterns
    #   4.  mixed_pats
    #   5.  french (contractions)
    #   6.  abbrev + telecom
    #   7.  units
    #   7b. split_mixed_tokens      (FIX-28 : الجزائرSlm → الجزائر Slm)
    #   8.  arabizi                 (FIX-5, FIX-6, FIX-30 : BUG-E corrigé)
    #   9.  cleanup                 (FIX-2 : rep_chars sans chiffres)
    #  10.  dedup                   (FIX-3, FIX-4)
    #  11.  stopwords
    #  12.  restauration nums       (FIX-1)

    def normalize(self, text: str) -> str:
        if not isinstance(text, str) or not text.strip():
            return ""
        try:
            # FIX-1 : protection immédiate des séquences numériques
            protected_nums, text = self._protect_numbers(text)

            text = self._step_emojis(text)
            text = self._step_unicode_arabic(text)
            text = self._step_extra_ar(text)
            for pat, repl in self._mixed_pats:
                text = pat.sub(repl, text)
            text = self._step_french(text)
            text = self._step_abbrev(text)
            text = self._step_units(text)
            text = self._step_split_mixed_tokens(text)   # FIX-28 : avant arabizi
            text = self._step_arabizi(text)
            text = self._step_cleanup(text)
            text = self._dedup_tokens(text)
            if self.remove_sw:
                text = self._step_stopwords(text)

            # FIX-1 : restauration finale
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

            if core in self._negations:
                result.append(tok)
                i += 1
                continue

            if core.lower() in self._tech_tokens:
                result.append(tok)
                i += 1
                continue

            mn = _RE["num_arabic"].match(core)
            if mn:
                num, unit = mn.groups()
                repl = self._lookup(unit, self.telecom) or unit
                if " " in repl:
                    result += [num] + repl.split()[:-1] + [repl.split()[-1] + trail]
                else:
                    result += [num, repl + trail]
                i += 1
                continue

            resolved = False
            for dct in (self.telecom, self.abbreviations):
                repl = self._lookup(core, dct)
                if repl is not None:
                    if (latin_dom
                            and dct is self.telecom
                            and not _RE["has_arabic"].search(core)
                            and len(core) <= 4):
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

    # ── FIX-28 : séparation tokens collés arabe+latin ─────────────────────────
    def _step_split_mixed_tokens(self, text: str) -> str:
        """
        Insère un espace entre caractères arabes et latins collés.
        Résout le problème des tokens comme 'الجزائرSlm' ou 'CNXميتة'
        qui ne passaient pas par arabizi_words car _RE['has_arabic'] matchait.

        Exemples :
          'الجزائرSlm'  → 'الجزائر Slm'  → ensuite Slm → سلام
          'الجزائرwlh'  → 'الجزائر wlh'  → ensuite wlh → والله
          'CNXميتة'     → 'CNX ميتة'

        Protection : les tokens __NP0__ (protections numériques) sont ignorés.
        """
        text = _RE["ar_then_lat"].sub(r"\1 \2", text)
        text = _RE["lat_then_ar"].sub(r"\1 \2", text)
        return text

    def _step_arabizi(self, text: str) -> str:
        latin_dom = self._is_latin_dominant(text)
        result = []
        for tok in text.split():
            lo = tok.lower()

            # Protection tech_tokens + protected_words
            if lo in self._tech_tokens or lo in self._protected:
                result.append(tok)
                continue
            if _RE["has_arabic"].search(tok):
                result.append(tok)
                continue
            if _RE["digits_only"].match(tok):
                result.append(tok)
                continue
            if _RE["num_arabic"].match(tok):
                result.append(tok)
                continue

            if _RE["pure_latin"].match(tok):
                # FIX-5  : arabizi_words EN PREMIER, avant latin_dom
                # FIX-30 : BUG-E trop large — slm/wlh/bzf (len=3) bloqués à tort
                #          en texte latin-dominant. Nouvelle règle : seuls les tokens
                #          de len <= 2 ET explicitement ambigus (ki/el/da/li/w/dz)
                #          sont protégés. Tous les autres mots connus sont convertis.
                _AMBIGUOUS_SHORT = {"ki", "el", "da", "li", "w", "dz"}
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
                    if latin_dom:
                        result.append(tok)
                        continue
                    result.append(self._arabizi_convert(tok))
                continue

            # FIX-6 : tokens hybrides (arabizi_hyb) dans texte latin-dominant
            if _RE["arabizi_hyb"].match(tok):
                # Chercher d'abord dans arabizi_words
                w = self.arabizi_words.get(lo)
                if w:
                    result.append(w)
                    continue
                for k, v in self._arabizi_upper_sorted:
                    if tok.upper() == k:
                        result.append(v)
                        break
                else:
                    if latin_dom:
                        # Texte latin dominant : conserver le token tel quel
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
        # NB : num_separator déjà protégé en amont — pas de re-protection ici
        text = _RE["arab_digit"].sub(r"\1 \2", text)
        text = _RE["digit_arab"].sub(r"\1 \2", text)

        def _fuse_spaced(m: re.Match) -> str:
            return m.group(0).replace(" ", "")

        text = _RE["spaced_digits"].sub(_fuse_spaced, text)
        text = _RE["arabic_digits_spaced"].sub(_fuse_spaced, text)
        # FIX-2 : rep_chars exclut les chiffres
        text = _RE["rep_chars"].sub(lambda m: m.group(1) * 2, text)
        text = _RE["rep_punct"].sub(lambda m: m.group(1), text)
        text = _RE["whitespace"].sub(" ", text).strip()
        return text

    def _step_stopwords(self, text: str) -> str:
        if not self._stopwords:
            return text
        keep = self._negations | self._dialect_keep
        result = []
        for w in text.split():
            lo = w.lower()
            if (w.isdigit()
                    or w in keep
                    or lo in keep
                    or lo not in self._stopwords):
                result.append(w)
            elif len(w) == 1 and _RE["has_arabic"].match(w):
                result.append(w)
        return " ".join(result)

    # ── DataFrame ─────────────────────────────────────────────────────────────
    # FIX-27 : filter_hors_scope supprimé — géré en amont par la Cellule 9
    #          du pipeline Spark via _is_hors_scope(). Évite double application.

    def normalize_df(
        self,
        df,
        col: str = "comment",
        out_col: Optional[str] = None,
        drop_empty: bool = True,
        filter_hidden: bool = True,
    ):
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


def normalize(
    text: str,
    mode: str = "arabert",
    dict_path: Path = MASTER_DICT_PATH,
    rules_path: Path = RULES_DICT_PATH,
) -> str:
    global _default_normalizer
    if _default_normalizer is None or _default_normalizer.mode != mode:
        _default_normalizer = TextNormalizer(mode=mode, dict_path=dict_path, rules_path=rules_path)
    return _default_normalizer.normalize(text)


def normalize_df(
    df,
    col: str = "comment",
    mode: str = "arabert",
    dict_path: Path = MASTER_DICT_PATH,
    rules_path: Path = RULES_DICT_PATH,
    **kwargs,
):
    n = TextNormalizer(mode=mode, dict_path=dict_path, rules_path=rules_path)
    return n.normalize_df(df, col=col, **kwargs)


# ── Tests de régression v3.8 ──────────────────────────────────────────────────

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
    print("  TextNormalizer v3.8.2 — TESTS RÉGRESSION")
    print("=" * 70)

    N = TextNormalizer(mode="arabert", dict_path=dict_path, rules_path=rules_path)

    cases = [
        ("VERIF-14 18-00 conservé", "المشكل تع وقت الذروة 18-00 نتامك", "18-00", "18 00"),
        ("VERIF-36 10/10 conservé", "شكيت 4 شكاوي في الهاتف 10/10",    "10/10", "10 10"),
        ("VERIF-D7 لكن conservé",   "زدت 200دج و لكن التدفق لا زال",   "لكن",   None),
        ("VERIF-D7 فقط conservé",   "نسحقو انترنت فقط ديجا وغاليا",    "فقط",   None),
        ("VERIF-D7 خمس conservé",   "أبلغ الوكالة أكثر من خمس مرات",   "خمس",   None),
        ("VERIF-Q5 baridimo→ar",    "على طريق baridimo وين نشتكي",     "بريدي موب", "baridimo"),
        # ── FIX-1 : protection chiffres séparateurs ───────────────────────
        ("FIX-1a 15:00 conservé",       "الساعة 15:00 زوالا",                   "15:00",          "1500"),
        ("FIX-1b 20:48 conservé",       "20:48 Fibre 1gb/s Telechargelent",     "20:48",          "2048"),
        ("FIX-1c 10:30 conservé",       "ghal9in a bou ismaël 10:30 rien",      "10:30",          "1030"),
        ("FIX-1d 90-11 conservé",       "Loi 90-11 du travail",                 "90-11",          "9011"),
        ("FIX-1e 3-7 conservé",         "entre 3-7 jours",                      "3-7",            "37"),
        ("FIX-1f 1000 intact",          "ما هي الفائدة من 1000 جيغا",           "1000",           None),
        # ── FIX-2 : 8000 intact ───────────────────────────────────────────
        ("FIX-2 8000 intact",           "نستنا حتى يكون السعر ب 8000دج",        "8000",           None),
        # ── FIX-3 : جدا جدا conservé ──────────────────────────────────────
        ("FIX-3 جدا جدا conservé",      "انترنت 0 خدمة رديئة جدا جدا",         "جدا جدا",         None),
        # ── FIX-4 : DOUBLON_SUFFIXE ───────────────────────────────────────
        ("FIX-4 باي باي conservé",      "5 جي و الكل يتعلم...باي باي اشيري",   "باي باي",         None),
        # ── FIX-5 : arabizi_words avant latin_dom ─────────────────────────
        ("FIX-5a slm converti",         "Slm khoti khasni التعبئة",             "سلام",           None),
        ("FIX-5b mkch converti",        "مكاش واش mkch راهو",                   "ماكش",           None),
        # ── FIX-6 : latin_dom protège hybrides ────────────────────────────
        ("FIX-6a y9olo conservé/conv",  "kifech homa y9olo maftouha wlh",       "يقولو",          "ق"),
        ("FIX-6b maghlou9a conservé",   "f alge mghlo9aa hbb",                  "مغلوقة",         "غق"),
        ("FIX-6c ghal9in converti",     "algérie télécom ghal9in",              "غالقين",         None),
        # ── FIX-7 : ئ conservé ────────────────────────────────────────────
        ("FIX-7 سيئة intact",           "خدمة سيئة جدا",                        "سيئه",           "سييه"),
        # ── FIX-8 : بينج → ping ───────────────────────────────────────────
        ("FIX-8 بينج → ping",           "ريقلو بينج تاع فورت نايت",             "ping",           "بينج"),
        # ── FIX-9 : بونيص → bonus ─────────────────────────────────────────
        ("FIX-9 بونيص → bonus",         "انا قلت كاش بونيص راح ديروه",         "bonus",          "بونيص"),
        # ── FIX-10 : variantes connexion arabes ───────────────────────────
        ("FIX-10a الكونيكسيو",          "ثقة في الكونيكسيو ديالكم",             "الانترنت",       "الكونيكسيو"),
        ("FIX-10b الكنكسيون",           "ريقلولنا الكنكسيون برك",               "الانترنت",       "الكنكسيون"),
        # ── FIX-11 : نت arabe → الانترنت ─────────────────────────────────
        ("FIX-11 النت → الانترنت",      "أكثر من أسبوع بلا نت",                "الانترنت",       None),
        # ── FIX-12 : انتارنات ─────────────────────────────────────────────
        ("FIX-12 انتارنات",             "درت فيبر مكاش انتارنات ندمت",          "الانترنت",       "انتارنات"),
        # ── FIX-13 : dialecte latin enrichi ───────────────────────────────
        ("FIX-13a kifech",              "kifech homa y9olo",                     "كيفاش",          None),
        ("FIX-13b nastana",             "nastana dirona fibre kraht",            "نستنا",          None),
        ("FIX-13c wlh",                 "wlh thir kho",                         "والله",           None),
        # ── Régressions v3.7 conservées ───────────────────────────────────
        ("REG cnx → connexion",         "Cnx ميتة",                             "connexion",      "cnx"),
        ("REG CNX majuscule",           "CNX ميتة",                             "connexion",      "CNX"),
        ("REG xgs conservé",            "مودام xgs pon",                        "xgs",            None),
        ("REG febre → fibre optique",   "دخلونا febre واش",                     "fibre optique",  None),
        ("REG ping protégé",            "ping élevé sur gaming",                "ping",           None),
        ("REG بينغ → ping",             "بينغ راه يطلع في الالعاب",             "ping",           "بينغ"),
        ("REG inshallah",               "inshallah يجي التقني",                 "إن شاء الله",    None),
        ("REG wallah hybride",          "wallah hshuma 3likom",                 "والله",           None),
        ("REG nflexi",                  "واش نقدر nflexi بالفيبر",              "فليكسي",          None),
        ("REG انترنيت",                 "الانترنيت مقطوعة",                     "الانترنت",        "انترنيت"),
        ("REG s'il → si il",            "s'il vous plaît",                      "si il",          None),
        ("REG جدا simple conservé",     "رديئة جدا",                            "جدا",            None),
        ("REG très très intentionnel",  "très très lente la connexion",         "très très",      None),
        ("REG كيف كيف intentionnel",    "كونكسيو ضعيفة كيف كيف",               "كيف كيف",        None),
        ("REG غير conservé",            "مودام غير متوافق",                     "غير",            None),
        ("REG جانفي conservé",          "يوم 1 جانفي 2026",                     "جانفي",          None),
        ("REG 90-11 hors-scope",        "Loi 90-11 du travail",                 None,             None),
        # ── FIX-28 : tokens collés arabe+latin ────────────────────────────────
        ("FIX-28a الجزائرSlm → séparé",  "الجزائرSlm معندناشش انترنت",          "سلام",           "slm"),
        ("FIX-28b الجزائرwlh → séparé",  "اتصالاتwlh راهو ضعيف بزاف",          "والله",           "wlh"),
        ("FIX-28c CNXميتة → séparé",     "CNXميتة هذا الاتصال",                 "connexion",      "CNX"),
        # ── FIX-29 : بونيص/بونص avec préfixes ────────────────────────────────
        ("FIX-29a والبونيص",             "والبونيص ماعادوش كيما قبل",           "bonus",          None),
        ("FIX-29b لبونص",                "قالولي لبونص راح ينقص",               "bonus",          None),
        ("FIX-29c البونيص",              "مالو البونيص تاعي",                   "bonus",          None),
        # ── FIX-30 : BUG-E corrigé — slm/wlh en texte latin dominant ─────────
        ("FIX-30a slm latin_dom",        "slm je suis D'Oran test 00 mega",     "سلام",           "slm"),
        ("FIX-30b wlh latin_dom",        "la honte wlh c'est vraiment nul",     "والله",           "wlh"),
        ("FIX-30c Slm latin_dom",        "Slm j'ai remplis ce formulaire",      "سلام",           "slm"),
        ("FIX-30d bzf latin_dom",        "c'est bzf lent la connexion",         "بزاف",           "bzf"),
        # ── Vérification non-régression BUG-E tokens ambigus ──────────────────
        ("FIX-30e ki conservé fr",       "ki tu fais ça correctement",          "ki",             None),
        ("FIX-30f el conservé es",       "el servicio es muy malo",             "el",             None),
    ]

    all_ok = True
    for label, inp, must_have, must_not in cases:
        out = N.normalize(inp)
        ok1 = (must_have in out) if must_have else True
        ok2 = (must_not not in out) if must_not else True
        ok  = ok1 and ok2
        if not ok:
            all_ok = False
        status = "✓" if ok else "✗ FAIL"
        print(f"  {status}  [{label}]")
        if not ok:
            print(f"         IN : {inp}")
            print(f"         OUT: {out}")
            if not ok1:
                print(f"         MANQUE     : {must_have!r}")
            if not ok2:
                print(f"         INDÉSIRABLE: {must_not!r}")

    print()
    print("── TEST FIX-D hors-scope ─────────────────────────────────────────")
    hs_cases = [
        ("Selon le Code du travail algérien (Loi 90-11)",             True),
        ("Sarl Maxim continue toujours les entretiens d'embauche",    True),
        ("خدمة 4g مقطوعة هنا في الجلفة",                              False),
        ("fibre optique Alger Centre coupée depuis hier",             False),
    ]
    for txt, expected in hs_cases:
        got = TextNormalizer._is_hors_scope(txt)
        ok  = (got == expected)
        if not ok:
            all_ok = False
        print(f"  {'✓' if ok else '✗ FAIL'}  hors_scope={got} (attendu={expected})  {txt[:60]}")

    print()
    print("── TESTS UNITÉS ──────────────────────────────────────────────────")
    for inp, exp in [
        ("20mbps", "20 Mbps"), ("15 Mo", "15 Mo"), ("60ms", "60ms"),
        ("200da", "200 DA"),   ("18h",   "18h"),    ("512kb", "512 Ko"),
        ("2go",   "2 Go"),     ("600mbs","600 Mbps"),
    ]:
        out = N._step_units(inp)
        ok  = "✓" if out == exp else "✗"
        if out != exp:
            all_ok = False
        print(f"  {ok} {inp:12} → {out:15}  (attendu: {exp})")

    print("\n" + "=" * 70)
    print("  ✅ v3.8.2 OK — tous les fixes validés" if all_ok else "  ❌ Régressions détectées — voir ci-dessus")
    print("=" * 70)