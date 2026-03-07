"""
text_normalizer_v3.py
=====================
Normaliseur unifié pour corpus télécom DZ (26K commentaires).
Charge un seul fichier : master_dict.json

Compatible : AraBERT  |  CAMeL-BERT  |  MarBERT  |  DziriBERT

Modes :
  "arabert"  → normalisation légère, sans stopwords  (Transformers)
  "full"     → normalisation complète + stopwords     (ML classique / stats)

Changelog v3.5 :
  FIX-1  arabizi_monograms — suppression des monogrammes latins simples (a→ا
         b→ب ...) qui convertissaient les mots FR/EN protégés si le check
         _protected échouait. Seuls les chiffres arabizi (3,5,6,7,9) et les
         digrammes (gh, kh, sh, ch, ou, ei, ai) sont conservés.
  FIX-2  unicode_arabic — ajout de ة→ه et ئ→ي absents du master_dict
  FIX-3  _dedup_tokens — ne supprime plus "عرض" après "العرض" (article
         arabe = préfixe, pas un doublon)
  FIX-4  _arabizi_convert — n'applique plus les monogrammes latins sur les
         tokens purement latins protégés
  FIX-5  _step_abbrev — wifi/data/net cherché dans telecom_tech en priorité
         pour éviter la double entrée abbreviations↔telecom_tech
  FIX-6  master_dict regex groupe capturant — vérifié et correct en Python
  STABLE : tous les fixes v3.4 conservés (pipeline order, inshallah, flexi...)
"""

from __future__ import annotations
import re, json, logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

MASTER_DICT_PATH = Path(__file__).parent.parent / "dictionaries" / "master_dict.json"

# ── Mots répétés intentionnellement (emphase) — ne pas dédupliquer ───────────
_INTENTIONAL_REPEATS = {
    "كيف", "شوي", "برا", "هاك", "يا", "آه", "لا", "واو", "هو", "هي",
    "très", "trop", "bien", "non", "oui", "si",
}

# ── Patterns regex compilés une fois globalement ─────────────────────────────
_RE = {
    "diacritics":   re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06ED]"),
    "tatweel":      re.compile(r"\u0640+"),
    "rep_chars":    re.compile(r"(.)\1{2,}"),
    "rep_punct":    re.compile(r"([!?.،:;])\1+"),
    "whitespace":   re.compile(r"\s+"),
    "digits_only":  re.compile(r"^\d+$"),
    "pure_latin":   re.compile(r"^[a-zA-Z'\u2019\-]+$"),
    "arabizi_hyb":  re.compile(r"^(?=.*[a-zA-Z])(?=.*(?<=[a-zA-Z])[35679]|[35679](?=[a-zA-Z])).+$"),
    "has_arabic":   re.compile(r"[\u0600-\u06FF]"),
    "trail_punct":  re.compile(r"^(.*[^!.،,;:؟?])((?:[!.،,;:؟?])+)$"),
    "num_arabic":   re.compile(r"^(\d+)([\u0600-\u06FF\u0750-\u077F].*)$"),
    "unit_nospace": re.compile(r"(?<!\w)(\d+)([a-zA-Z/]+(?:ps|/s)?)(?=[\u0600-\u06FF\s,،.!?؟$]|$)", re.IGNORECASE),
    "unit_space":   re.compile(r"\b(\d+)\s+([a-zA-Z/]+(?:ps|/s)?)\b", re.IGNORECASE),
    "contraction":  re.compile(r"\b(j['\u2019]ai|c['\u2019]est|n['\u2019]est|n['\u2019]a|qu['\u2019]il|qu['\u2019]on)\b", re.IGNORECASE),
    "arab_digit":   re.compile(r'([\u0600-\u06FF])(\d)'),
    "digit_arab":   re.compile(r'(\d)([\u0600-\u06FF])'),
    "spaced_digits": re.compile(r'(?<![:\-\d])(\d)(?: (\d)){1,6}(?![:\-\d])'),
    "arabic_digits_spaced": re.compile(r'(?<![٠-٩])([٠-٩])(?: ([٠-٩])){1,6}(?![٠-٩])'),
    # FIX-3 : détecte les préfixes arabes (ال, و, ف, ب, ك, ل + combinaisons)
    "arabic_prefix": re.compile(r'^(وال|فال|بال|كال|لل|ال|و|ف|ب|ك|ل)(.+)$'),
}

_CONTRACTIONS = {
    "j'ai": "je ai", "j\u2019ai": "je ai",
    "c'est": "ce est", "c\u2019est": "ce est",
    "n'est": "ne est", "n\u2019est": "ne est",
    "n'a": "ne a", "n\u2019a": "ne a",
    "qu'il": "que il", "qu\u2019il": "que il",
    "qu'on": "que on", "qu\u2019on": "que on",
}

_ARABIC_PREFIXES = ["وال", "فال", "بال", "كال", "لل", "ال", "و", "ف", "ب", "ك", "ل"]

_NEGATIONS = {"ما", "لا", "لم", "لن", "ليس", "مكانش", "ماكش", "ماهيش", "ماهوش", "غير", "مش", "ميش"}
_DIALECT_KEEP = {
    "راه", "راهي", "راني", "راك", "راهم", "واش", "كيفاش", "وين", "علاش", "قديش",
    "من", "لي", "نحن", "أنا", "أنت", "أنتم", "في", "على", "بعد", "نفس", "كل", "منذ",
    "عند", "بين", "خلال", "بعض", "مزال", "كيما", "علي", "باه", "راني", "ديما",
    "ب", "ك", "ل", "ya", "pas", "de", "en", "c", "ma", "la", "les", "au", "du",
    "avec", "depuis", "pour", "par", "sur", "sous", "dans", "entre", "sans", "vers",
}

_TECH_TOKENS = {
    "xgs", "xgs-pon", "pon", "adsl", "vdsl", "ftth", "fttb", "dns", "ont",
    "dsl", "cnx", "4g", "3g", "2g", "5g", "lte", "volte", "mbps", "kbps", "gbps",
    "fibre", "fiber", "febre", "wifi", "idoom", "djezzy", "mobilis", "ooredoo",
}

# ── Variantes إن شاء الله (latines) — inline car très fréquent dans corpus ───
_EXTRA_ARABIZI_WORDS = {
    "nachalh": "إن شاء الله", "nchalh": "إن شاء الله", "inchalh": "إن شاء الله",
    "inshalah": "إن شاء الله", "inshalh": "إن شاء الله", "inshaallah": "إن شاء الله",
    "inchalah": "إن شاء الله", "inshaalah": "إن شاء الله", "inchallah": "إن شاء الله",
    "inshallah": "إن شاء الله", "nchallah": "إن شاء الله", "nshallah": "إن شاء الله",
    "wlh": "والله", "wlhi": "والله", "wellah": "والله", "wella": "والله",
    "wallah": "والله", "wallahi": "والله", "wallhy": "والله",
    "flexi": "فليكسي", "flexili": "فليكسي", "nflexi": "فليكسي", "yflexi": "فليكسي",
}

_EXTRA_ARABIZI_UPPER = {
    "NACHALH": "إن شاء الله", "NCHALH": "إن شاء الله", "INCHALH": "إن شاء الله",
    "INSHALAH": "إن شاء الله", "INSHALLAH": "إن شاء الله", "INCHALAH": "إن شاء الله",
    "INCHALLAH": "إن شاء الله",
    "WLH": "والله", "WELLAH": "والله", "WALLAH": "والله", "WALLAHI": "والله",
}

# Patterns arabes inline (إن شاء الله, فليكسي — variantes arabes)
_EXTRA_AR_PATTERNS = [
    (re.compile(r'\bانشاء الله\b'),  "إن شاء الله"),
    (re.compile(r'\bنشاالله\b'),     "إن شاء الله"),
    (re.compile(r'\bانشالله\b'),     "إن شاء الله"),
    (re.compile(r'\bنشالله\b'),      "إن شاء الله"),
    (re.compile(r'\bانشاالله\b'),    "إن شاء الله"),
    (re.compile(r'\bاشالله\b'),      "إن شاء الله"),
    (re.compile(r'\bفكيكسيت\b'),     "فليكسي"),
    (re.compile(r'\bفلكسيلي\b'),     "فليكسي"),
    (re.compile(r'\bفليكسيلي\b'),    "فليكسي"),
    (re.compile(r'\bفليكسيت\b'),     "فليكسي"),
    (re.compile(r'\bنفليكسي\b'),     "فليكسي"),
    (re.compile(r'\bيفليكسي\b'),     "فليكسي"),
    (re.compile(r'\bتفليكسي\b'),     "فليكسي"),
]


class TextNormalizer:

    def __init__(
        self,
        mode: str = "arabert",
        dict_path: Path = MASTER_DICT_PATH,
        remove_stopwords: bool = False,
    ):
        assert mode in ("arabert", "full"), "mode doit être 'arabert' ou 'full'"
        self.mode     = mode
        self.remove_sw = remove_stopwords or (mode == "full")

        with open(dict_path, encoding="utf-8") as f:
            d = json.load(f)

        # ── unicode_arabic — FIX-2 : ajouter ة→ه et ئ→ي si absents ──────────
        self.unicode_map: Dict[str, str] = d["unicode_arabic"]
        self.unicode_map.setdefault("\u0629", "\u0647")   # ة → ه  (FIX-2)
        self.unicode_map.setdefault("\u0626", "\u064A")   # ئ → ي  (FIX-2)
        self.unicode_map.setdefault("\u0624", "\u0648")   # ؤ → و  (sécurité)

        # ── arabizi ──────────────────────────────────────────────────────────
        self.digrams:  Dict[str, str] = d["arabizi_digrams"]

        # FIX-1 : ne conserver que les chiffres arabizi dans les monogrammes
        # Les lettres latines simples (a→ا b→ب ...) sont supprimées car elles
        # convertissent n'importe quel mot FR/EN non protégé en charabia arabe.
        _raw_mono = d["arabizi_monograms"]
        self.monograms: Dict[str, str] = {
            k: v for k, v in _raw_mono.items()
            if not (len(k) == 1 and k.isalpha())   # FIX-1 : exclure a,b,c...z
        }
        # → seuls restent : "3"→ع  "5"→خ  "6"→ط  "7"→ح  "9"→ق
        logger.debug(f"arabizi_monograms après FIX-1 : {list(self.monograms.keys())}")

        self.arabizi_words: Dict[str, str] = {**d["arabizi_words"], **_EXTRA_ARABIZI_WORDS}
        self.arabizi_upper: Dict[str, str] = {**d["arabizi_upper"], **_EXTRA_ARABIZI_UPPER}

        self.emojis:     Dict[str, str] = d["emojis"]
        self.abbreviations: Dict[str, str] = d["abbreviations"]
        self.telecom:    Dict[str, str] = d["telecom_tech"]
        self.units_map:  Dict[str, str] = d["units"]

        nv = d["network_variants"]
        self._net_form: str      = nv["normalized_form"]
        self._net_all:  List[str] = nv["latin"] + nv["arabic"]

        self._mixed_pats: List[Tuple[re.Pattern, str]] = self._compile_dict(d["mixed_ar_fr_regex"])
        self._fr_pats:    List[Tuple[re.Pattern, str]] = self._compile_dict(
            d["french_corrections_regex"], flags=re.IGNORECASE | re.UNICODE
        )

        escaped = [re.escape(v) for v in self._net_all if v]
        self._net_re = re.compile(
            rf'\b({"|".join(escaped)})\b', re.IGNORECASE
        ) if escaped else None

        # Arabizi trié longueur décroissante (digrammes avant monogrammes)
        combined = {**self.digrams, **self.monograms}
        self._arabizi_seq: List[Tuple[str, str]] = sorted(
            combined.items(), key=lambda x: len(x[0]), reverse=True
        )
        self._arabizi_upper_sorted: List[Tuple[str, str]] = sorted(
            self.arabizi_upper.items(), key=lambda x: len(x[0]), reverse=True
        )

        # Ensemble des tokens protégés (ne jamais convertir en arabizi)
        _all_vals: List[str] = []
        for v in list(self.telecom.values()) + list(self.abbreviations.values()):
            if v:
                _all_vals.extend(v.split())

        self._protected: Set[str] = {
            t.lower() for t in (
                _all_vals
                + list(self.telecom.keys()) + list(self.abbreviations.keys())
                + list(_TECH_TOKENS)
                + [
                    "adsl", "vdsl", "wifi", "fibre", "fiber", "optique",
                    "idoom", "djezzy", "mobilis", "ooredoo",
                    "internet", "connexion", "problème", "réseau",
                    "service", "mbps", "kbps", "gbps", "mo", "go", "ko", "da",
                    "4g", "5g", "3g", "2g", "lte", "volte",
                    "pas", "on", "ne", "fait", "rien", "tout", "fois", "bien",
                    "moi", "encore", "niveau", "bravo", "message", "solution",
                    "compte", "temps", "même", "comme", "chaque", "alors",
                    "avant", "depuis", "juste", "vraiment", "tres", "lente",
                    "mois", "plusieurs", "bonjour", "merci", "salut", "normal",
                    "speed", "gaming", "game", "live", "ping", "high", "low",
                    "facebook", "whatsapp", "youtube", "instagram",
                    "ont", "il", "ils", "de", "me", "le", "la", "les", "du",
                    "une", "un", "pas", "ne", "qui", "que", "dans", "sur",
                    "pour", "par", "avec", "sans", "sont", "est", "etait",
                    "avait", "peut", "font", "fait", "avez", "avons",
                ]
            )
        }

        self._stopwords: Set[str] = self._build_stopwords() if self.remove_sw else set()

        total = sum([
            len(self.unicode_map), len(self.digrams), len(self.monograms),
            len(self.arabizi_words), len(self.arabizi_upper),
            len(self.emojis), len(self.abbreviations), len(self.telecom),
            len(self.units_map), len(self._mixed_pats), len(self._fr_pats),
            len(self._net_all),
        ])
        logger.info(f"TextNormalizer [{mode}] v3.5 — {total} entrées | {dict_path.name}")

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
        for p in _ARABIC_PREFIXES:
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

    def _dedup_tokens(self, text: str) -> str:
        """
        Supprime les tokens dupliqués consécutifs, sauf :
        - tokens dans _INTENTIONAL_REPEATS (emphase intentionnelle)
        - suffixes d'un token avec préfixe arabe (FIX-3)
          Ex: العرض + عرض → gardés (العرض = article+mot, عرض = mot seul)
        """
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
                if curr_lo in _INTENTIONAL_REPEATS:
                    result.append(curr)
                continue

            # FIX-3 : ne pas supprimer si prev a un préfixe arabe
            # Ex: "العرض" contient "عرض" comme suffixe → ce N'EST PAS un doublon
            has_ar_curr = _RE["has_arabic"].search(curr)
            has_ar_prev = _RE["has_arabic"].search(prev)

            if has_ar_prev and has_ar_curr:
                # Vérifier si prev = préfixe + curr
                m = _RE["arabic_prefix"].match(prev)
                if m and m.group(2) == curr:
                    # prev = ال + عرض, curr = عرض → garder les deux
                    result.append(curr)
                    continue

            # Suppression suffixe latin
            if not has_ar_curr and len(curr) >= 3 and len(curr) < len(prev):
                if prev_lo.endswith(curr_lo):
                    continue

            # Suppression suffixe arabe (sans préfixe — FIX-3 déjà traité)
            if has_ar_curr and len(curr) >= 4 and len(curr) < len(prev):
                if prev.endswith(curr):
                    # Vérifier qu'il n'y a PAS de préfixe arabe avant
                    m = _RE["arabic_prefix"].match(prev)
                    if not m:
                        continue

            result.append(curr)

        return " ".join(result)

    # ── Pipeline v3.5 ─────────────────────────────────────────────────────────
    # ORDRE :
    #   1. emojis
    #   2. unicode_arabic  (avant mixed_pats — FIX v3.4 conservé)
    #   3. extra_ar_patterns (إن شاء الله, فليكسي arabes)
    #   4. mixed_pats (dict)
    #   5. french
    #   6. abbrev + telecom
    #   7. units
    #   8. arabizi
    #   9. cleanup
    #  10. dedup  (FIX-3)
    #  11. stopwords

    def normalize(self, text: str) -> str:
        if not isinstance(text, str) or not text.strip():
            return ""
        try:
            text = self._step_emojis(text)
            text = self._step_unicode_arabic(text)
            text = self._step_extra_ar(text)
            for pat, repl in self._mixed_pats:
                text = pat.sub(repl, text)
            text = self._step_french(text)
            text = self._step_abbrev(text)
            text = self._step_units(text)
            text = self._step_arabizi(text)
            text = self._step_cleanup(text)
            text = self._dedup_tokens(text)
            if self.remove_sw:
                text = self._step_stopwords(text)
        except Exception as exc:
            logger.error(f"normalize() erreur: {exc} | texte: {text[:80]!r}")
        return text.strip()

    # ── Étapes ────────────────────────────────────────────────────────────────

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

    def _step_abbrev(self, text: str) -> str:
        """
        FIX-5 : cherche d'abord dans telecom_tech, puis abbreviations.
        Évite la double résolution sur wifi, data, net, etc.
        """
        latin_dom = self._is_latin_dominant(text)
        tokens, result = text.split(), []
        i = 0
        while i < len(tokens):
            tok = tokens[i]
            m = _RE["trail_punct"].match(tok)
            core, trail = (m.group(1), m.group(2)) if m else (tok, "")

            # Préserver les négations/modaux
            if core in ("غير", "مش", "ميش", "ما", "لا"):
                result.append(tok)
                i += 1
                continue

            # Tokens techniques → conserver tels quels
            if core.lower() in _TECH_TOKENS:
                result.append(tok)
                i += 1
                continue

            # Nombre collé à un token arabe : 1جيڤا, 60ميجا
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

            # FIX-5 : telecom_tech en priorité, puis abbreviations
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
        text = _RE["contraction"].sub(
            lambda m: _CONTRACTIONS.get(m.group(0).lower(), m.group(0)), text
        )
        for pat, repl in self._fr_pats:
            text = pat.sub(repl, text)
        return text

    def _step_arabizi(self, text: str) -> str:
        """
        FIX-4 : _arabizi_convert() n'applique plus les monogrammes latins
        (supprimés en FIX-1). Seuls chiffres arabizi + digrammes actifs.
        """
        latin_dom = self._is_latin_dominant(text)
        result = []
        for tok in text.split():
            lo = tok.lower()

            if _RE["has_arabic"].search(tok):
                result.append(tok)
                continue
            if lo in self._protected or lo in _TECH_TOKENS:
                result.append(tok)
                continue
            if _RE["digits_only"].match(tok):
                result.append(tok)
                continue
            if _RE["num_arabic"].match(tok):
                result.append(tok)
                continue

            if _RE["pure_latin"].match(tok):
                # 1. Lookup mot complet (inshallah, wallah, flexi...)
                w = self.arabizi_words.get(lo)
                if w:
                    result.append(w)
                    continue
                # 2. Lookup majuscules (WALLAH, CNX...)
                for k, v in self._arabizi_upper_sorted:
                    if tok.upper() == k:
                        result.append(v)
                        break
                else:
                    # 3. Si texte latin dominant → conserver
                    if latin_dom:
                        result.append(tok)
                        continue
                    # 4. Sinon → tentative de conversion arabizi
                    result.append(self._arabizi_convert(tok))
                continue

            # Token hybride (lettres + chiffres arabizi)
            if _RE["arabizi_hyb"].match(tok):
                result.append(self._arabizi_convert(tok))
                continue

            result.append(tok)
        return " ".join(result)

    def _arabizi_convert(self, token: str) -> str:
        """
        Convertit un token arabizi.
        FIX-4 : les monogrammes latins (a,b,c...) ayant été retirés de
        self.monograms (FIX-1), _step_cleanup supprimera les lettres latines
        résiduelles — résultat propre sans charabia.
        """
        # 1. Lookup majuscules complet
        for k, v in self._arabizi_upper_sorted:
            if token.upper() == k:
                return v

        result = token.lower()

        # 2. Digrammes spéciaux
        for extra, ar in [("ee", "ي"), ("ii", "ي"), ("oo", "و"), ("pp", "ب")]:
            result = result.replace(extra, ar)

        # 3. Digrammes + chiffres arabizi (monogrammes latins exclus par FIX-1)
        for seq, ar in self._arabizi_seq:
            result = result.replace(seq, ar)

        # 4. Supprimer les lettres latines résiduelles
        result = re.sub(r'[a-z]', '', result)
        return result

    def _step_unicode_arabic(self, text: str) -> str:
        text = _RE["diacritics"].sub("", text)
        text = _RE["tatweel"].sub("", text)
        for variant, canonical in self.unicode_map.items():
            text = text.replace(variant, canonical)
        return text

    def _step_cleanup(self, text: str) -> str:
        text = _RE["arab_digit"].sub(r'\1 \2', text)
        text = _RE["digit_arab"].sub(r'\1 \2', text)

        def _fuse_spaced(m):
            return m.group(0).replace(" ", "")

        text = _RE["spaced_digits"].sub(_fuse_spaced, text)
        text = _RE["arabic_digits_spaced"].sub(_fuse_spaced, text)
        text = _RE["rep_chars"].sub(lambda m: m.group(1) * 2, text)
        text = _RE["rep_punct"].sub(lambda m: m.group(1), text)
        return _RE["whitespace"].sub(" ", text).strip()

    def _step_stopwords(self, text: str) -> str:
        if not self._stopwords:
            return text
        keep = _NEGATIONS | _DIALECT_KEEP
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

    @staticmethod
    def _build_stopwords() -> Set[str]:
        sw = {
            "le", "la", "les", "l", "un", "une", "des", "du", "de", "et", "ou", "mais",
            "donc", "car", "ni", "or", "ce", "cet", "cette", "ces", "mon", "ton", "son",
            "notre", "votre", "leur", "leurs", "ma", "ta", "sa", "mes", "tes", "ses",
            "je", "tu", "il", "elle", "nous", "vous", "ils", "elles", "me", "te", "se",
            "lui", "y", "en", "que", "qui", "quoi", "dont", "est", "sont", "être", "avoir",
            "a", "ai", "avec", "sans", "sur", "sous", "dans", "par", "pour", "très", "plus",
            "moins", "aussi", "bien", "tout", "pas", "ne", "on", "si",
            "i", "my", "we", "our", "you", "your", "he", "she", "it", "its", "they", "their",
            "am", "is", "are", "was", "were", "have", "has", "had", "do", "does", "did",
            "will", "would", "could", "should", "a", "an", "the", "and", "or", "but",
            "if", "in", "on", "at", "by", "for", "with", "to", "from", "not", "no",
            "إلى", "عن", "مع", "كان", "كانت", "هذا", "هذه", "ذلك", "تلك",
            "هو", "هي", "هم", "هن", "ثم", "أو", "إن", "إذا", "لو", "قد",
            "لكن", "بل", "حتى", "ضد", "أن", "التي", "الذي", "الذين",
        }
        try:
            from nltk.corpus import stopwords as _sw
            sw.update(_sw.words("arabic"))
        except Exception:
            pass
        return sw - _NEGATIONS - _DIALECT_KEEP

    # ── DataFrame ─────────────────────────────────────────────────────────────

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

    # ── Spark UDF ─────────────────────────────────────────────────────────────

    def spark_udf(self):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        norm = self

        @udf(returnType=StringType())
        def _udf(text):
            return norm.normalize(text) if text else ""

        return _udf


# ── Fonctions de commodité ─────────────────────────────────────────────────────

_default_normalizer: Optional[TextNormalizer] = None


def normalize(text: str, mode: str = "arabert", dict_path: Path = MASTER_DICT_PATH) -> str:
    global _default_normalizer
    if _default_normalizer is None or _default_normalizer.mode != mode:
        _default_normalizer = TextNormalizer(mode=mode, dict_path=dict_path)
    return _default_normalizer.normalize(text)


def normalize_df(df, col: str = "comment", mode: str = "arabert",
                 dict_path: Path = MASTER_DICT_PATH, **kwargs):
    n = TextNormalizer(mode=mode, dict_path=dict_path)
    return n.normalize_df(df, col=col, **kwargs)


# ── Tests ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    dict_path = Path(sys.argv[1]) if len(sys.argv) > 1 else MASTER_DICT_PATH
    if not dict_path.exists():
        print(f"⚠️  master_dict.json non trouvé : {dict_path}")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("  TextNormalizer v3.5 — TESTS RÉGRESSION")
    print("=" * 70)

    N = TextNormalizer(mode="arabert", dict_path=dict_path)

    cases = [
        # (description, input, doit_contenir, ne_doit_pas_contenir)

        # ── FIX-1 : monogrammes latins ──────────────────────────────────────
        ("FIX-1 fibre conservé",           "fibre optique lente",           "fibre",         "فيبره"),
        ("FIX-1 service conservé",         "service client horrible",       "service",       "سهرفيسه"),
        ("FIX-1 bonjour conservé",         "bonjour problème réseau",       "bonjour",       "بهنجهور"),

        # ── FIX-2 : ة et ئ normalisés ───────────────────────────────────────
        ("FIX-2 خدمة → خدمه",             "خدمة سيئة",                     "خدمه",          "خدمة"),
        ("FIX-2 ئ → ي",                   "يئس العميل",                    "يис",           None),

        # ── FIX-3 : العرض + عرض conservés ──────────────────────────────────
        ("FIX-3 العرض عرض gardés",         "هذا العرض عرض لا يكفي",        "العرض عرض",     None),
        ("FIX-3 تمشي مشي gardés",          "وانترنات مشي تمشي مشي حرام",  "تمشي مشي",      None),

        # ── v3.4 pipeline ───────────────────────────────────────────────────
        ("إن شاء الله latin",              "inshallah يجي التقني",          "إن شاء الله",   None),
        ("إن شاء الله nachalh",            "nachalh يصلح الريزو",           "إن شاء الله",   None),
        ("إن شاء الله انشالله",            "انشالله يصلحوها",               "إن شاء الله",   None),
        ("والله wallah",                   "wallah hshuma 3likom",          "والله",          None),
        ("فليكسي flexi",                   "واش نقدر nflexi بالفيبر",       "فليكسي",         None),
        ("فليكسي فكيكسيت",                 "فكيكسيت من البطاقة الذهبية",    "فليكسي",         None),
        ("انترنيت → الانترنت",             "الانترنيت مقطوعة",              "الانترنت",       "انترنيت"),

        # ── Régressions chiffres ────────────────────────────────────────────
        ("15:00 conservé",                 "الساعة 15:00 زوالا",            "15",             "1500"),
        ("90-11 conservé",                 "Loi 90-11 du travail",          "90",             "9011"),
        ("1000 conservé",                  "ما هي الفائدة من 1000 جيغا",   "1000",           None),

        # ── Négation + tech ─────────────────────────────────────────────────
        ("غير متوافق conservé",            "مودام غير متوافق مع لافيبر",   "غير",            None),
        ("كيف كيف intentionnel",           "كونكسيو ضعيفة كيف كيف",        "كيف كيف",        None),
        ("dsl conservé",                   "حنا كان عند dsl",               "dsl",            None),
        ("cnx → connexion",                "Cnx ميتة",                      "connexion",      None),
    ]

    all_ok = True
    for label, inp, must_have, must_not in cases:
        out = N.normalize(inp)
        ok1 = (must_have in out) if must_have else True
        ok2 = (must_not not in out) if must_not else True
        ok = ok1 and ok2
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

    print("── TESTS UNITÉS ──────────────────────────────────────────────────")
    for inp, exp in [
        ("20mbps", "20 Mbps"), ("15 Mo", "15 Mo"), ("60ms", "60ms"),
        ("200da", "200 DA"), ("18h", "18h"), ("512kb", "512 Ko"), ("2go", "2 Go"),
    ]:
        out = N._step_units(inp)
        ok = "✓" if out == exp else "✗"
        print(f"  {ok} {inp:12} → {out:12}  (attendu: {exp})")

    print("\n" + "=" * 70)
    print("  ✅ v3.5 OK" if all_ok else "  ❌ Régressions — voir ci-dessus")
    print("=" * 70)