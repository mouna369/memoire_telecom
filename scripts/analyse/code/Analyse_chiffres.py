#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
text_normalizer_v3.8.2_multinode.py
====================================
Normaliseur unifié pour corpus télécom DZ (26K commentaires).
Version MULTI-NŒUDS avec Spark pour traitement distribué.
Charge DEUX fichiers :
  - master_dict.json       : dictionnaires de traduction/normalisation
  - linguistic_rules.json  : règles linguistiques (négations, stopwords, prefixes…)

Compatible : AraBERT  |  CAMeL-BERT  |  MarBERT  |  DziriBERT

Modes :
  "arabert"  → normalisation légère, sans stopwords  (Transformers)
  "full"     → normalisation complète + stopwords     (ML classique / stats)

Utilisation avec Spark :
  1. Charger les dictionnaires sur les workers
  2. Appliquer la normalisation en parallèle
  3. Sauvegarder les résultats dans MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, BooleanType
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime
import os
import time
import math
import json
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI_DRIVER  = "mongodb://localhost:27018/"
MONGO_URI_WORKERS = "mongodb://mongodb_pfe:27017/"
DB_NAME           = "telecom_algerie"
COLLECTION_SOURCE = "commentaires_sans_doublons"  # Après suppression des doublons
COLLECTION_DEST   = "commentaires_normalises"
BATCH_SIZE        = 1000
NB_WORKERS        = 3
SPARK_MASTER      = "spark://spark_master_pfe:7077"
RAPPORT_PATH      = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage des textes/Rapports/rapport_normalisation_multinode.txt"

# Chemins des dictionnaires (seront envoyés aux workers)
MASTER_DICT_PATH = Path(__file__).parent.parent / "dictionaries" / "master_dict.json"
RULES_DICT_PATH  = Path(__file__).parent.parent / "dictionaries" / "linguistic_rules.json"

# ============================================================
# CLASSE TEXTNORMALIZER (identique à l'original)
# ============================================================

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
}

# ── Préfixes verbaux et conjonctifs arabes ────────────────────────────────────
_AR_VERB_PREFIXES: Set[str] = {"ت", "ي", "ن", "أ", "ا", "تت", "يت", "نت", "ست"}
_AR_CONJ_PREFIXES: Set[str] = {"و", "ف", "ب", "ك", "ل", "ال", "لل", "بال", "كال", "فال", "وال"}
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


class TextNormalizer:
    """Normaliseur de texte pour dialecte algérien - Version compatible Spark"""
    
    def __init__(self, mode: str = "arabert", dict_path: Path = None, rules_path: Path = None):
        assert mode in ("arabert", "full"), "mode doit être 'arabert' ou 'full'"
        self.mode = mode
        self.remove_sw = (mode == "full")
        
        # Charger les dictionnaires
        if dict_path is None:
            dict_path = MASTER_DICT_PATH
        if rules_path is None:
            rules_path = RULES_DICT_PATH
            
        with open(dict_path, encoding="utf-8") as f:
            d = json.load(f)
            
        rules = self._load_rules(rules_path)
        
        self._negations = self._build_negations(rules)
        self._dialect_keep = self._build_dialect_keep(rules)
        self._intentional_rep = self._build_intentional_repeats(rules)
        self._tech_tokens = self._build_tech_tokens(rules)
        self._arabic_prefixes = self._build_arabic_prefixes(rules)
        self._contractions = self._build_contractions(rules)
        self._contraction_re = self._build_contraction_re(self._contractions)
        
        # ── unicode_arabic ────────────────────────────────────────────────
        self.unicode_map: Dict[str, str] = d["unicode_arabic"]
        self.unicode_map.setdefault("\u0629", "\u0647")   # ة → ه  (AraBERT)
        # FIX-7 : ئ→ي SUPPRIMÉ
        self.unicode_map.setdefault("\u0624", "\u0648")   # ؤ → و
        
        self.digrams: Dict[str, str] = d["arabizi_digrams"]
        self.monograms: Dict[str, str] = {
            k: v for k, v in d["arabizi_monograms"].items()
            if not (len(k) == 1 and k.isalpha())
        }
        
        self.arabizi_words: Dict[str, str] = {**d["arabizi_words"], **_EXTRA_ARABIZI_WORDS}
        self.arabizi_words = {k: v for k, v in self.arabizi_words.items()
                              if not k.startswith("_")}
        self.arabizi_upper: Dict[str, str] = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}
        self.emojis: Dict[str, str] = d["emojis"]
        self.abbreviations: Dict[str, str] = d["abbreviations"]
        self.telecom: Dict[str, str] = d["telecom_tech"]
        self.units_map: Dict[str, str] = d["units"]
        
        nv = d["network_variants"]
        self._net_form: str = nv["normalized_form"]
        self._net_all: List[str] = nv["latin"] + nv["arabic"]
        
        # Filtrer les clés _comment_* dans mixed_ar_fr_regex
        mixed_clean = {k: v for k, v in d["mixed_ar_fr_regex"].items()
                       if not k.startswith("_")}
        self._mixed_pats: List[Tuple[re.Pattern, str]] = self._compile_dict(mixed_clean)
        self._fr_pats: List[Tuple[re.Pattern, str]] = self._compile_dict(
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
        
        self._protected: Set[str] = self._build_protected_words(rules)
        self._protected.update(
            t.lower() for t in (
                _all_vals
                + list(self.telecom.keys())
                + list(self.abbreviations.keys())
                + list(self._tech_tokens)
            )
        )
        
        self._stopwords: Set[str] = (
            self._build_stopwords_from_rules(rules, self._negations, self._dialect_keep)
            if self.remove_sw else set()
        )
        
    # ── Méthodes de chargement des règles ─────────────────────────────────
    
    @staticmethod
    def _load_rules(rules_path: Path) -> dict:
        if not rules_path.exists():
            return {}
        with open(rules_path, encoding="utf-8") as f:
            return json.load(f)
    
    @staticmethod
    def _build_negations(rules: dict) -> Set[str]:
        neg = rules.get("negations", {})
        result = set(neg.get("arabe_standard", []))
        result.update(neg.get("dialecte_algerien", []))
        result.update({"ما", "لا", "لم", "لن", "ليس", "غير", "مش", "ميش",
                       "مكانش", "ماكش", "ماهيش", "ماهوش"})
        return result
    
    @staticmethod
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
    
    @staticmethod
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
    
    @staticmethod
    def _build_tech_tokens(rules: dict) -> Set[str]:
        tt = rules.get("tech_tokens", {})
        result: Set[str] = set()
        for v in tt.values():
            if isinstance(v, list):
                result.update(v)
        result.discard("cnx")
        result.update({
            "adsl", "vdsl", "ftth", "fttb", "xgs", "xgs-pon", "pon",
            "dsl", "lte", "volte", "dns", "ont",
            "wifi", "4g", "3g", "2g", "5g",
            "mbps", "kbps", "gbps", "mbs",
            "idoom", "djezzy", "mobilis", "ooredoo",
            "fibre", "fiber", "febre",
            "ping",
        })
        return result
    
    @staticmethod
    def _build_arabic_prefixes(rules: dict) -> List[str]:
        ap = rules.get("arabic_prefixes", {})
        prefixes = ap.get("prefixes", [])
        if not prefixes:
            prefixes = ["وال", "فال", "بال", "كال", "لل", "ال", "و", "ف", "ب", "ك", "ل"]
        return sorted(prefixes, key=len, reverse=True)
    
    @staticmethod
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
    
    @staticmethod
    def _build_contraction_re(contractions: Dict[str, str]) -> re.Pattern:
        apostrophe_keys = [k for k in contractions if "'" in k or "\u2019" in k]
        if not apostrophe_keys:
            return re.compile(r"(?!)")
        escaped = sorted([re.escape(k) for k in apostrophe_keys], key=len, reverse=True)
        return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)
    
    @staticmethod
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
    
    @staticmethod
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
        return sw - negations - dialect_keep
    
    # ── Helpers ───────────────────────────────────────────────────────────────
    
    @staticmethod
    def _compile_dict(d: dict, flags: int = re.UNICODE) -> List[Tuple[re.Pattern, str]]:
        result = []
        for pat, repl in d.items():
            try:
                result.append((re.compile(pat, flags), repl))
            except re.error:
                pass
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
        ar = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
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
    
    # ── FIX-4 : _dedup_tokens avec guard suffixe ──────────────────────────────
    def _dedup_tokens(self, text: str) -> str:
        tokens = text.split()
        if len(tokens) < 2:
            return text
        
        result = [tokens[0]]
        for i in range(1, len(tokens)):
            prev = result[-1]
            curr = tokens[i]
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
                if stripped in _AR_ALL_PREFIXES and len(stripped) >= 2:
                    result.append(curr)
                    continue
                
                result.append(curr)
                continue
            
            result.append(curr)
        
        return " ".join(result)
    
    # ── Pipeline v3.8 ─────────────────────────────────────────────────────────
    
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
            text = self._step_split_mixed_tokens(text)
            text = self._step_arabizi(text)
            text = self._step_cleanup(text)
            text = self._dedup_tokens(text)
            if self.remove_sw:
                text = self._step_stopwords(text)
            
            # FIX-1 : restauration finale
            for key, val in protected_nums.items():
                text = text.replace(key, val)
            
        except Exception:
            return ""
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
            tok = tokens[i]
            m = _RE["trail_punct"].match(tok)
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
    
    def _step_split_mixed_tokens(self, text: str) -> str:
        text = _RE["ar_then_lat"].sub(r"\1 \2", text)
        text = _RE["lat_then_ar"].sub(r"\1 \2", text)
        return text
    
    def _step_arabizi(self, text: str) -> str:
        latin_dom = self._is_latin_dominant(text)
        result = []
        for tok in text.split():
            lo = tok.lower()
            
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
            
            if _RE["arabizi_hyb"].match(tok):
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


# ============================================================
# FONCTIONS SPARK DISTRIBUÉES
# ============================================================

def normaliser_partition(partition):
    """
    Chaque Worker normalise sa partition en utilisant TextNormalizer
    """
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient, InsertOne
    from pymongo.errors import BulkWriteError
    
    try:
        # Initialiser le normalizer sur le worker
        normalizer = TextNormalizer(mode="arabert")
        
        # Connexion MongoDB
        client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_DEST]
    except Exception as e:
        yield {"_erreur": str(e), "statut": "connexion_failed"}
        return
    
    batch = []
    docs_traites = 0
    
    for row in partition:
        # Normaliser les deux colonnes de texte
        commentaire_normalise = normalizer.normalize(row.get("Commentaire_Client", ""))
        moderateur_normalise = normalizer.normalize(row.get("commentaire_moderateur", ""))
        
        doc = {
            "_id": row.get("_id"),
            "Commentaire_Client": commentaire_normalise,
            "commentaire_moderateur": moderateur_normalise,
            "date": row.get("date"),
            "source": row.get("source"),
            "moderateur": row.get("moderateur"),
            "metadata": row.get("metadata"),
            "statut": row.get("statut"),
            "normalise_par": f"worker_{os.environ.get('HOSTNAME', 'unknown')}",
            "date_normalisation": datetime.now().isoformat()
        }
        
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
    yield {"docs_traites": docs_traites, "statut": "ok"}


def lire_partition_depuis_mongo(partition_info):
    """Chaque Worker lit sa portion depuis MongoDB directement"""
    import sys
    sys.path.insert(0, '/opt/pymongo_libs')
    from pymongo import MongoClient
    
    for item in partition_info:
        try:
            client = MongoClient(MONGO_URI_WORKERS, serverSelectionTimeoutMS=5000)
            db = client[DB_NAME]
            collection = db[COLLECTION_SOURCE]
            
            curseur = collection.find(
                {},
                {"_id": 1, "Commentaire_Client": 1, "commentaire_moderateur": 1,
                 "date": 1, "source": 1, "moderateur": 1, "metadata": 1, "statut": 1}
            ).skip(item["skip"]).limit(item["limit"])
            
            for doc in curseur:
                doc["_id"] = str(doc["_id"])
                yield doc
            
            client.close()
        except Exception as e:
            yield {"_erreur": str(e), "skip": item["skip"], "statut": "lecture_failed"}


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================
temps_debut_global = time.time()

print("=" * 70)
print("🔤 NORMALISATION MULTI-NŒUDS v3.8.2")
print("   Spark 4.1.1 | mapPartitions | Workers → MongoDB direct")
print("   Mode: arabert (pour Transformers)")
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
    .appName("Normalisation_MultiNode") \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
temps_fin_spark = time.time()
print(f"✅ Spark connecté en {temps_fin_spark - temps_debut_spark:.2f}s")
print(f"📍 Master URL: {spark.sparkContext.master}")
print(f"🆔 Application ID: {spark.sparkContext.applicationId}")

# 3. LECTURE DISTRIBUÉE
print("\n📥 LECTURE DISTRIBUÉE — Chaque Worker lit sa portion...")
temps_debut_chargement = time.time()

docs_par_worker = math.ceil(total_docs / NB_WORKERS)
plages = [
    {"skip": i * docs_par_worker,
     "limit": min(docs_par_worker, total_docs - i * docs_par_worker)}
    for i in range(NB_WORKERS)
]

for idx, p in enumerate(plages):
    print(f"   • Worker {idx+1} : skip={p['skip']}, limit={p['limit']} docs")

# Créer RDD avec les plages
rdd_plages = spark.sparkContext.parallelize(plages, NB_WORKERS)

# Lire les données depuis MongoDB via les workers
rdd_data = rdd_plages.mapPartitions(lire_partition_depuis_mongo)

# Filtrer les erreurs de lecture
rdd_data_filtered = rdd_data.filter(lambda x: "_erreur" not in x)

if rdd_data_filtered.isEmpty():
    print("❌ Aucune donnée lue depuis MongoDB !")
    exit(1)

# Convertir en DataFrame
df_spark = spark.read.json(rdd_data_filtered.map(
    lambda d: json.dumps(
        {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
         for k, v in d.items()}
    )
))

total_lignes = df_spark.count()
temps_fin_chargement = time.time()
print(f"✅ {total_lignes} documents chargés en {temps_fin_chargement - temps_debut_chargement:.2f}s")

# 4. VIDER LA COLLECTION DESTINATION
print("\n🧹 PRÉPARATION DE LA COLLECTION DESTINATION...")
try:
    coll_dest = db_driver[COLLECTION_DEST]
    coll_dest.delete_many({})
    print(f"   ✅ Collection {COLLECTION_DEST} vidée")
except Exception as e:
    print(f"   ⚠️  Erreur vidage collection: {e}")

# 5. NORMALISATION DISTRIBUÉE + ÉCRITURE
print("\n💾 NORMALISATION DISTRIBUÉE — Chaque Worker normalise et écrit...")
temps_debut_traitement = time.time()

# Distribuer les dictionnaires aux workers
spark.sparkContext.addPyFile(str(MASTER_DICT_PATH))
spark.sparkContext.addPyFile(str(RULES_DICT_PATH))

rdd_stats = df_spark.rdd \
    .map(lambda row: row.asDict()) \
    .mapPartitions(normaliser_partition)

stats_ecriture = rdd_stats.collect()
total_inseres = sum(s.get("docs_traites", 0) for s in stats_ecriture if s.get("statut") == "ok")
erreurs = [s for s in stats_ecriture if "_erreur" in s]

temps_fin_traitement = time.time()
print(f"✅ Normalisation terminée en {temps_fin_traitement - temps_debut_traitement:.2f}s")
print(f"📦 {total_inseres} documents normalisés et écrits dans MongoDB")

if erreurs:
    for e in erreurs:
        print(f"   ⚠️  {e.get('_erreur')}")

# 6. VÉRIFICATION FINALE
print("\n🔎 VÉRIFICATION FINALE...")
temps_debut_verif = time.time()

try:
    total_en_dest = coll_dest.count_documents({})
    print(f"   • Documents en destination  : {total_en_dest}")
    
    # Vérifier quelques exemples
    print(f"\n📝 Exemples de commentaires normalisés:")
    echantillon = list(coll_dest.aggregate([{"$sample": {"size": 3}}]))
    for i, doc in enumerate(echantillon, 1):
        commentaire = doc.get("Commentaire_Client", "")
        if len(commentaire) > 80:
            commentaire = commentaire[:80] + "..."
        print(f"\n   {i}. {commentaire}")
except Exception as e:
    print(f"   ⚠️  Erreur vérification: {e}")

# 7. RAPPORT FINAL
temps_fin_global = time.time()
temps_total = temps_fin_global - temps_debut_global

rapport = f"""
{"="*70}
RAPPORT — NORMALISATION MULTI-NŒUDS v3.8.2
{"="*70}
Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Mode   : Spark 4.1.1 | {NB_WORKERS} Workers | MongoDB direct
Normalisation : arabert (pour Transformers)

⏱️  TEMPS:
   • Connexion Spark  : {temps_fin_spark - temps_debut_spark:.2f}s
   • Chargement       : {temps_fin_chargement - temps_debut_chargement:.2f}s
   • Normalisation    : {temps_fin_traitement - temps_debut_traitement:.2f}s
   • TOTAL            : {temps_total:.2f}s ({total_lignes/temps_total:.0f} doc/s)

📊 RÉSULTATS:
   • Documents source       : {total_lignes}
   • Documents normalisés   : {total_inseres}
   • Taux de succès         : {total_inseres/total_lignes*100:.2f}%

📁 STOCKAGE:
   • Source      : {DB_NAME}.{COLLECTION_SOURCE}
   • Destination : {DB_NAME}.{COLLECTION_DEST}
   • Statut      : {"✅ SUCCÈS" if total_inseres == total_lignes else "⚠️ PARTIEL"}

🔧 DICTIONNAIRES:
   • master_dict.json       : {MASTER_DICT_PATH.name}
   • linguistic_rules.json  : {RULES_DICT_PATH.name}
"""

# Sauvegarder le rapport
os.makedirs(os.path.dirname(RAPPORT_PATH), exist_ok=True)
with open(RAPPORT_PATH, "w", encoding="utf-8") as f:
    f.write(rapport)
print(f"\n✅ Rapport sauvegardé : {RAPPORT_PATH}")

print("\n" + "=" * 70)
print("📊 RÉSUMÉ FINAL")
print("=" * 70)
print(f"   📥 Documents source     : {total_lignes}")
print(f"   📤 Documents normalisés : {total_inseres}")
print(f"   ⏱️  Temps total          : {temps_total:.2f}s")
print(f"   🚀 Vitesse              : {total_lignes/temps_total:.0f} docs/s")
print(f"   📁 Collection dest.     : {DB_NAME}.{COLLECTION_DEST}")
print("=" * 70)
print("🎉 NORMALISATION TERMINÉE EN MODE MULTI-NŒUDS !")

# Nettoyage
spark.stop()
client_driver.close()
print("🔌 Connexions fermées proprement")