"""
Normaliseur complet — corpus Télécom DZ
Utilise :
  - dictionnaire_normalisation.json  (v3.11.3)
  - stopwords_dialect.json           (v1.7)

Pipeline par document :
  1. Suppressions  : prix / téléphone / technique seul / charabia clavier / charabia arabe
  2. Unicode arabe : homogénéisation des variantes (أإآ→ا, ک→ك …)
  3. Emojis        : remplacement par étiquette arabe
  4. Arabizi words : mots entiers connus → arabe/français normalisé
  5. Arabizi upper : idem pour majuscules
  6. Mixed AR-FR regex : patterns compilés depuis le dictionnaire
  7. Abréviations  : bjr→bonjour, cnx→connexion …
  8. Termes télécom: normalisation terminologique
  9. French corrections regex
  10. Corrections générales : majuscules, hashtags, glue chiffre-lettre, espaces
"""

import re
import json
from pathlib import Path

# ════════════════════════════════════════════════════════════
#  Chargement des dictionnaires (passés en dict Python)
# ════════════════════════════════════════════════════════════

def build_normalizer(dict_norm: dict, dict_stop: dict):
    """
    Construit et retourne une fonction clean(text) → (cleaned, reason).
    """

    # ── 1. Suppression patterns ──────────────────────────────────────────────
    PRIX_RE = re.compile(
        r'\b\d[\d\s]*(?:da|dz|dzd|دج|دينار|dinars?|euros?|€)\b'
        r'|\b(?:da|dz|dzd|دج|دينار)\s*\d[\d\s]*\b'
        r'|\b\d+\s*(?:da|dz)\b',
        re.IGNORECASE,
    )
    TEL_RE = re.compile(
        r'\b(?:0|\+213|00213)[\s\-]?[5-7]\d[\s\-]?\d{2}[\s\-]?\d{2}[\s\-]?\d{2}\b'
        r'|\b0[5-7]\d{8}\b'
    )
    # Termes purement techniques (sans phrase autour)
    _tech_raw = list(dict_norm.get("telecom_tech", {}).keys())
    _tech_pattern = '|'.join(re.escape(t) for t in sorted(_tech_raw, key=len, reverse=True))
    TECH_ONLY_RE = re.compile(rf'\b(?:{_tech_pattern})\b', re.IGNORECASE)

    CHARABIA_LAT = re.compile(r'\b[b-df-hj-np-tv-z]{6,}\b', re.IGNORECASE)
    CHARABIA_AR  = re.compile(r'[\u0621-\u064A]{1,2}([\u0621-\u064A])\1{3,}')

    # ── 2. Unicode arabe ─────────────────────────────────────────────────────
    UNICODE_MAP = dict_norm.get("unicode_arabic", {})
    UNICODE_TABLE = str.maketrans(UNICODE_MAP)

    # ── 3. Emojis ────────────────────────────────────────────────────────────
    EMOJIS = dict_norm.get("emojis", {})
    # Tri par longueur décroissante pour les multi-char emojis
    EMOJI_LIST = sorted(EMOJIS.items(), key=lambda x: len(x[0]), reverse=True)

    def replace_emojis(text):
        for emoji, label in EMOJI_LIST:
            text = text.replace(emoji, f" {label} ")
        return text

    # ── 4. Arabizi words (lowercase exact) ──────────────────────────────────
    ARABIZI = dict_norm.get("arabizi_words", {})
    # Tri longueur décroissante pour éviter substitutions partielles
    ARABIZI_SORTED = sorted(ARABIZI.items(), key=lambda x: len(x[0]), reverse=True)
    ARABIZI_PATTERNS = [
        (re.compile(rf'\b{re.escape(k)}\b', re.IGNORECASE), v)
        for k, v in ARABIZI_SORTED
    ]

    # ── 5. Arabizi upper ─────────────────────────────────────────────────────
    ARABIZI_UP = dict_norm.get("arabizi_upper", {})
    ARABIZI_UP_PATTERNS = [
        (re.compile(rf'\b{re.escape(k)}\b'), v)
        for k, v in sorted(ARABIZI_UP.items(), key=lambda x: len(x[0]), reverse=True)
    ]

    # ── 6. Mixed AR-FR regex ─────────────────────────────────────────────────
    MIXED_RAW = dict_norm.get("mixed_ar_fr_regex", {})
    MIXED_PATTERNS = []
    for pattern_str, repl in MIXED_RAW.items():
        try:
            MIXED_PATTERNS.append((re.compile(pattern_str, re.IGNORECASE | re.UNICODE), repl))
        except re.error:
            pass  # pattern invalide ignoré

    # ── 7. Abréviations ──────────────────────────────────────────────────────
    ABBREVS = dict_norm.get("abbreviations", {})
    ABBREV_PATTERNS = [
        (re.compile(rf'\b{re.escape(k)}\b', re.IGNORECASE), v)
        for k, v in sorted(ABBREVS.items(), key=lambda x: len(x[0]), reverse=True)
    ]

    # ── 8. Termes télécom ────────────────────────────────────────────────────
    TELECOM = dict_norm.get("telecom_tech", {})
    TELECOM_PATTERNS = [
        (re.compile(rf'\b{re.escape(k)}\b', re.IGNORECASE), v)
        for k, v in sorted(TELECOM.items(), key=lambda x: len(x[0]), reverse=True)
    ]

    # ── 9. French corrections regex ──────────────────────────────────────────
    FR_CORR = dict_norm.get("french_corrections_regex", {})
    FR_PATTERNS = []
    for p, r in FR_CORR.items():
        try:
            FR_PATTERNS.append((re.compile(p, re.IGNORECASE), r))
        except re.error:
            pass

    # ── 10. Corrections générales ────────────────────────────────────────────
    HASHTAG_RE  = re.compile(r'#\w+')
    EXTRA_SP_RE = re.compile(r'\s{2,}')
    GLUE1       = re.compile(r'([a-zA-Z\u0621-\u064A])(\d)', re.UNICODE)
    GLUE2       = re.compile(r'(\d)([a-zA-Z\u0621-\u064A])', re.UNICODE)

    # ── Network variants ─────────────────────────────────────────────────────
    nv = dict_norm.get("network_variants", {})
    nv_normalized = nv.get("normalized_form", "شبكة")
    nv_latin  = nv.get("latin", [])
    nv_arabic = nv.get("arabic", [])
    NV_PATTERN = re.compile(
        r'\b(?:' + '|'.join(re.escape(w) for w in nv_latin) + r')\b'
        + (r'|(?:' + '|'.join(re.escape(w) for w in nv_arabic) + r')' if nv_arabic else ''),
        re.IGNORECASE,
    )

    # ════════════════════════════════════════════════════════
    #  Fonction principale
    # ════════════════════════════════════════════════════════
    def clean(text: str):
        """
        Retourne (texte_nettoyé | None, raison_suppression | None)
        """
        if not isinstance(text, str) or not text.strip():
            return None, "vide"

        t = text.strip()

        # ── Suppressions ──────────────────────────────────────────────────
        # Prix
        prix_stripped = PRIX_RE.sub("", t).strip()
        if PRIX_RE.search(t) and len(prix_stripped) < len(t) * 0.30:
            return None, "prix_detecte"

        # Téléphone
        tel_stripped = TEL_RE.sub("", t).strip()
        if TEL_RE.search(t) and len(tel_stripped) < len(t) * 0.30:
            return None, "telephone_detecte"

        # Technique seul (uniquement termes techniques + chiffres, pas de vraie phrase)
        tech_stripped = re.sub(r'[\d\s\W]+', '', TECH_ONLY_RE.sub("", t))
        if tech_stripped == "" and len(t.split()) <= 5:
            return None, "technique_seul"

        # Charabia latin
        if CHARABIA_LAT.search(t):
            cl = CHARABIA_LAT.sub("", t).strip()
            if len(cl) < len(t) * 0.40:
                return None, "charabia_clavier"

        # Charabia arabe
        if CHARABIA_AR.search(t):
            ca = CHARABIA_AR.sub("", t).strip()
            if len(ca) < len(t) * 0.40:
                return None, "charabia_arabe"

        # ── Corrections ───────────────────────────────────────────────────

        # 2. Unicode arabe
        t = t.translate(UNICODE_TABLE)

        # 3. Emojis
        t = replace_emojis(t)

        # 4. Arabizi upper (avant lowercase)
        for pat, repl in ARABIZI_UP_PATTERNS:
            t = pat.sub(repl, t)

        # 5. Arabizi words (insensible à la casse)
        for pat, repl in ARABIZI_PATTERNS:
            t = pat.sub(repl, t)

        # 6. Mixed AR-FR regex
        for pat, repl in MIXED_PATTERNS:
            t = pat.sub(repl, t)

        # 7. Network variants
        t = NV_PATTERN.sub(nv_normalized, t)

        # 8. Abréviations
        for pat, repl in ABBREV_PATTERNS:
            t = pat.sub(repl, t)

        # 9. Termes télécom
        for pat, repl in TELECOM_PATTERNS:
            t = pat.sub(repl, t)

        # 10. French corrections
        for pat, repl in FR_PATTERNS:
            t = pat.sub(repl, t)

        # 11. Corrections générales
        t = t.lower()
        t = HASHTAG_RE.sub("", t)
        t = GLUE1.sub(r'\1 \2', t)
        t = GLUE2.sub(r'\1 \2', t)
        t = EXTRA_SP_RE.sub(" ", t).strip()

        return t, None

    return clean
