"""
validate_existing_normalization.py
====================================
Lit un fichier JSON contenant déjà :
  - client_comment     → texte original
  - normalized_comment → texte déjà normalisé

Compare les deux et détecte les anomalies de normalisation.
Pas besoin de relancer le normaliseur.

Usage :
    python validate_existing_normalization.py corpus.json
    python validate_existing_normalization.py corpus.json --out rapport.csv
    python validate_existing_normalization.py corpus.json --sample 500

Changelog v3 :
  BUG-4 fix  check_duplicates — faux positifs DOUBLON_EXACT sur les mots
             répétés intentionnellement pour emphase (كيف كيف, très très…).
             Ajout de _INTENTIONAL_REPEATS : si les deux tokens identiques
             font partie de cette liste, la paire est ignorée.

  BUG-5 fix  check_duplicates — faux positifs DOUBLON_SUFFIXE sur les mots
             courts en latin (optique/que, shout/out).
             Seuil relevé de len(b) > 2 à len(b) >= 4 pour le latin,
             aligné sur le seuil du normaliseur v3.7.

  BUG-6 fix  _PROTECTED_TERMS — termes manquants généraient des faux
             positifs RÉSIDU_ARABIZI : vpn, bch, dbm, voip, rj45, w6,
             idm, djz, mob ajoutés.

  STABLE : BUG-1/2/3 (v2) conservés.
"""

import json, re, sys, argparse, logging
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# ── Constantes ────────────────────────────────────────────────────────────────

_ARABIC_NEG_PREFIXES = {"غير", "لا", "ما", "لم", "لن", "ليس", "بدون", "دون"}

# BUG-4 fix (v3) : mots répétés intentionnellement pour emphase
# Ne jamais signaler ces paires comme DOUBLON_EXACT
_INTENTIONAL_REPEATS = {
    # Arabe dialectal
    "كيف", "شوي", "برا", "هاك", "يا", "آه", "لا", "واو", "هو", "هي",
    "بزاف", "مزال", "كل", "نعم", "أه", "وش", "هاه", "صح", "زيد", "هكا",
    "شوية", "يعني", "هذا", "ذاك", "آآآه",
    # Français
    "très", "trop", "bien", "non", "oui", "si", "encore",
    "jamais", "toujours", "vraiment", "absolument",
    "vite", "mal", "fort", "vrai", "faux",
}

# BUG-3/6 fix : termes techniques à ne JAMAIS signaler comme résidu arabizi
_PROTECTED_TERMS = {
    # Débit / unités
    "ping", "mbps", "kbps", "gbps", "mo", "go", "ko", "dbm",  # BUG-6 : dbm
    # Protocoles / normes réseau
    "adsl", "vdsl", "dsl",
    "ftth", "fttb",
    "xgs", "xgs-pon",
    "lte", "volte",
    "dns", "ont", "pon",
    "4g", "3g", "2g", "5g",
    "wi-fi", "wifi",
    "vpn",                                    # BUG-6 : vpn ajouté
    "voip",                                   # BUG-6 : voip ajouté
    "rj45",                                   # BUG-6 : rj45 ajouté
    # Terminaux / équipements
    "febre", "fiber", "fibre", "optique",
    "routing", "routage",
    "w6",                                     # BUG-6 : w6 ajouté
    # Opérateurs DZ
    "idoom", "djezzy", "mobilis", "ooredoo",
    "idm", "djz", "mob",                      # BUG-6 : abréviations opérateurs
    # Services / applis
    "connexion", "cnx",
    "internet",
    "facebook", "whatsapp", "youtube", "instagram",
    "tiktok", "twitter", "telegram", "linkedin",
    # Arabizi dialectal reconnu
    "bch",                                    # BUG-6 : bch (باش) ajouté
    # Divers télécom
    "box", "esim", "sim",
}

# Patterns hors-scope (commentaires non télécom)
_OUT_OF_SCOPE = {
    "legal":       re.compile(r"code du travail|comité de participation|loi 90.11|règlement intérieur", re.I),
    "recrutement": re.compile(r"entretiens d.embauche|envoyez votre cv|technico.commercial|recrutement@", re.I),
    "hors_sujet":  re.compile(r"#عدل3|حق_الطعون|الحق_في_السكن", re.UNICODE),
}

# BUG-1 : regex pour détecter les séquences numériques avec séparateur
_RE_NUM_SEP     = re.compile(r'(?<!\d)(\d+)([-:/])(\d+)(?!\d)')
_RE_NUMBERS     = re.compile(r'\b(\d{2,})\b')
_RE_NEG_PHRASE  = re.compile(r'\b(غير|بدون|دون)\s+(\S+)', re.UNICODE)
_RE_ARABIC_RESIDUE = re.compile(r'\b[bcdfghjklmnpqrstvwxyz]{3,}\b', re.I)
_RE_HAS_ARABIC  = re.compile(r'[\u0600-\u06FF]')

# BUG-2 fix : préfixes arabes qui forment des mots avec leur base
_ARABIC_KNOWN_PREFIXES = {
    "ال", "لل",
    "و", "ف", "ب", "ك", "ل",
    "وال", "فال", "بال", "كال",
    "ت", "ي", "ن", "أ", "ا",
    "تت", "يت", "نت", "ست",
}


# ── Checks individuels ────────────────────────────────────────────────────────

def check_numbers(original: str, normalized: str) -> list:
    """
    Détecte les chiffres tronqués ou disparus.

    BUG-1 fix : les séquences avec séparateur (15:00, 90-11, 20:48) sont
    traitées comme une unité → une seule anomalie émise.
    """
    issues = []

    sep_spans = set()
    for m in _RE_NUM_SEP.finditer(original):
        sep_spans.add(m.start())
        seq       = m.group(0)
        found_seq = seq in normalized
        fused     = m.group(1) + m.group(3)
        found_fused = bool(re.search(rf'\b{re.escape(fused)}\b', normalized))

        if not found_seq and not found_fused:
            issues.append(("CHIFFRE_DISPARU",
                            f"séquence '{seq}' absente du résultat"))
        elif not found_seq and found_fused:
            issues.append(("CHIFFRE_DISPARU",
                            f"'{seq}' fusionné en '{fused}' (séparateur perdu)"))

    for m in _RE_NUMBERS.finditer(original):
        if any(m.start() == s for s in sep_spans):
            continue
        in_sep = False
        for sm in _RE_NUM_SEP.finditer(original):
            if sm.start() <= m.start() and m.end() <= sm.end():
                in_sep = True
                break
        if in_sep:
            continue

        num = m.group(1)
        if re.search(rf'\b{re.escape(num)}\b', normalized):
            continue
        nums_in_norm = _RE_NUMBERS.findall(normalized)
        truncated = [n for n in nums_in_norm if num.startswith(n) and n != num]
        if truncated:
            issues.append(("CHIFFRE_TRONQUÉ",
                            f"{num} → {truncated[0]}  (tronqué de {len(num)-len(truncated[0])} chiffres)"))
        else:
            issues.append(("CHIFFRE_DISPARU",
                            f"'{num}' absent du résultat"))
    return issues


def check_negations(original: str, normalized: str) -> list:
    """Négations arabes supprimées qui inversent le sens."""
    issues = []
    for m in _RE_NEG_PHRASE.finditer(original):
        neg       = m.group(1)
        following = m.group(2)
        full      = m.group(0)
        if full not in normalized and following in normalized:
            issues.append(("NÉGATION_SUPPRIMÉE",
                            f"'{full}' → '{following}' ('{neg}' perdu, sens inversé)"))
    return issues


def check_duplicates(normalized: str) -> list:
    """
    Détecte les tokens consécutifs dupliqués.

    BUG-2 fix (v2) : ignore les paires arabe préfixe + racine.
    BUG-4 fix (v3) : ignore les doublons intentionnels (_INTENTIONAL_REPEATS).
    BUG-5 fix (v3) : seuil latin relevé à >= 4 (évite optique/que, shout/out).
    """
    issues = []
    tokens = normalized.split()
    for i in range(1, len(tokens)):
        a  = tokens[i - 1]
        b  = tokens[i]
        au = a.upper()
        bu = b.upper()

        # ── Doublon exact ──────────────────────────────────────────────────
        if au == bu:
            # BUG-4 : emphase intentionnelle → ignorer
            if b.lower() in _INTENTIONAL_REPEATS:
                continue
            issues.append(("DOUBLON_EXACT",
                            f"'{b}' apparaît deux fois de suite"))
            continue

        # ── Doublon suffixe ────────────────────────────────────────────────
        has_arabic_b = bool(_RE_HAS_ARABIC.search(b))

        # BUG-5 : seuil latin >= 4 (aligné sur normaliseur v3.7)
        min_len = 4 if not has_arabic_b else 3

        if au.endswith(bu) and len(b) >= min_len and au != bu:
            # BUG-2 : préfixe arabe connu + racine → ignorer
            stripped = a[: len(a) - len(b)]
            if stripped in _ARABIC_KNOWN_PREFIXES:
                continue
            issues.append(("DOUBLON_SUFFIXE",
                            f"'{a}' suivi de son suffixe '{b}'"))

    return issues


def check_arabizi_residue(normalized: str) -> list:
    """
    Détecte les résidus de consonnes latines entourés d'arabe.

    BUG-3 fix (v2) : _PROTECTED_TERMS complété (xgs, dsl, cnx, febre…).
    BUG-6 fix (v3) : ajout vpn, bch, dbm, voip, rj45, w6, opérateurs abrégés.
    """
    issues = []
    for m in _RE_ARABIC_RESIDUE.finditer(normalized):
        tok = m.group(0).lower()
        if tok in _PROTECTED_TERMS:
            continue
        ctx_start = max(0, m.start() - 15)
        ctx       = normalized[ctx_start: m.end() + 15]
        if _RE_HAS_ARABIC.search(ctx):
            issues.append(("RÉSIDU_ARABIZI",
                            f"'{m.group(0)}' semble non converti (entouré d'arabe)"))
    return issues


def check_out_of_scope(original: str) -> list:
    """Commentaires hors-scope télécom."""
    issues = []
    for tag, pat in _OUT_OF_SCOPE.items():
        if pat.search(original):
            issues.append(("HORS_SCOPE", f"catégorie: {tag}"))
    return issues


def check_empty_result(original: str, normalized: str) -> list:
    """Normalisé vide alors que l'original ne l'est pas."""
    if original.strip() and not normalized.strip():
        return [("RÉSULTAT_VIDE", "normalized_comment vide")]
    return []


# ── Chargement JSON ───────────────────────────────────────────────────────────

def load_json(path: Path) -> list:
    raw = path.read_text(encoding="utf-8").strip()
    if not raw.startswith("["):
        raw = "[" + raw.rstrip(",") + "]"
    data = json.loads(raw)
    if isinstance(data, list):
        return data
    for k in ("data", "comments", "corpus", "results"):
        if k in data and isinstance(data[k], list):
            return data[k]
    raise ValueError("Structure JSON non reconnue")


# ── Validation principale ─────────────────────────────────────────────────────

def validate(corpus: list, sample_size: int = None) -> list:
    if sample_size:
        import random
        corpus = random.sample(corpus, min(sample_size, len(corpus)))

    rows  = []
    stats = defaultdict(int)
    stats["total"] = len(corpus)

    for idx, item in enumerate(corpus):
        original   = (item.get("client_comment")    or "").strip()
        normalized = (item.get("normalized_comment") or "").strip()

        if not original:
            stats["vides_ignorés"] += 1
            continue

        all_issues = (
            check_out_of_scope(original)
            + check_empty_result(original, normalized)
            + check_numbers(original, normalized)
            + check_negations(original, normalized)
            + check_duplicates(normalized)
            + check_arabizi_residue(normalized)
        )

        for issue_type, detail in all_issues:
            stats[issue_type] += 1
            rows.append({
                "idx":        idx,
                "type":       issue_type,
                "detail":     detail,
                "original":   original[:300],
                "normalized": normalized[:300],
            })

        if not all_issues:
            stats["OK"] += 1

    # ── Résumé console ────────────────────────────────────────────────────
    total = stats["total"]
    print("\n" + "═" * 60)
    print(f"  RAPPORT DE VALIDATION — {total} commentaires")
    print("═" * 60)
    ok_count = stats.get("OK", 0)
    print(f"  ✓ Sans anomalie    : {ok_count:>6}  ({ok_count/total*100:.1f}%)")
    print()
    for k in sorted(stats):
        if k in ("total", "OK", "vides_ignorés"):
            continue
        v = stats[k]
        print(f"  ✗ {k:<30} : {v:>5}  ({v/total*100:.1f}%)")
    if stats["vides_ignorés"]:
        print(f"\n  (ignorés car vides : {stats['vides_ignorés']})")
    print("═" * 60 + "\n")

    return rows


# ── Export CSV ────────────────────────────────────────────────────────────────

def save_csv(rows: list, path: Path):
    import csv
    if not rows:
        print("✓ Aucune anomalie — pas de fichier CSV généré.")
        return
    fieldnames = ["idx", "type", "detail", "original", "normalized"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"Rapport CSV → {path}  ({len(rows)} lignes)")


# ── Affichage exemples ────────────────────────────────────────────────────────

def print_examples(rows: list, n_per_type: int = 3):
    from itertools import groupby
    sorted_rows = sorted(rows, key=lambda r: r["type"])
    for issue_type, group in groupby(sorted_rows, key=lambda r: r["type"]):
        items = list(group)
        print(f"\n── {issue_type}  ({len(items)} cas) ──────────────────────")
        for row in items[:n_per_type]:
            print(f"  [{row['idx']}] {row['detail']}")
            print(f"   original  : {row['original'][:90]}")
            print(f"   normalized: {row['normalized'][:90]}")


# ── Tests unitaires internes ──────────────────────────────────────────────────

def _run_self_tests():
    errors = []

    # BUG-1 : 15:00 → 1500 = 1 seule anomalie
    issues1 = check_numbers("الساعة 15:00 زوالا", "الساعه 1500 زوالا")
    if len(issues1) != 1:
        errors.append(f"BUG-1 : attendu 1, obtenu {len(issues1)} : {issues1}")
    elif "15:00" not in issues1[0][1]:
        errors.append(f"BUG-1 : message inattendu : {issues1[0][1]}")

    # BUG-1 : 15:00 intact = 0 anomalie
    if check_numbers("الساعة 15:00 زوالا", "الساعه 15:00 زوالا"):
        errors.append("BUG-1 : 15:00 intact ne doit pas générer d'anomalie")

    # BUG-1 : 90-11 → 9011 = 1 anomalie
    if len(check_numbers("Loi 90-11 du travail", "loi 9011 du travail")) != 1:
        errors.append("BUG-1 90-11 : attendu 1 anomalie")

    # BUG-2 : تمشي + مشي → pas de DOUBLON_SUFFIXE
    if [i for i in check_duplicates("وانترنات مشي تمشي مشي حرام") if i[0] == "DOUBLON_SUFFIXE"]:
        errors.append("BUG-2 تمشي/مشي : faux positif")

    # BUG-2 : وكيف + كيف → pas de DOUBLON_SUFFIXE
    if [i for i in check_duplicates("وكيف كيف ضعيفة") if i[0] == "DOUBLON_SUFFIXE"]:
        errors.append("BUG-2 وكيف/كيف : faux positif")

    # BUG-2 : العرض + عرض → pas de DOUBLON_SUFFIXE
    if [i for i in check_duplicates("هذا العرض عرض لا يكفي") if i[0] == "DOUBLON_SUFFIXE"]:
        errors.append("BUG-2 العرض/عرض : faux positif")

    # BUG-2 : vrai doublon suffixe toujours détecté
    if not [i for i in check_duplicates("connexion nnexion rapide") if i[0] == "DOUBLON_SUFFIXE"]:
        errors.append("BUG-2 : vrai doublon suffixe non détecté")

    # BUG-3 : xgs → pas de RÉSIDU_ARABIZI
    if [i for i in check_arabizi_residue("عندي مودام xgs pon") if "xgs" in i[1]]:
        errors.append("BUG-3 xgs : faux positif")

    # BUG-3 : dsl → pas de RÉSIDU_ARABIZI
    if [i for i in check_arabizi_residue("حنا كان عند dsl") if "dsl" in i[1]]:
        errors.append("BUG-3 dsl : faux positif")

    # BUG-3 : cnx → pas de RÉSIDU_ARABIZI
    if [i for i in check_arabizi_residue("cnx ميتة") if "cnx" in i[1]]:
        errors.append("BUG-3 cnx : faux positif")

    # BUG-4 : très très → pas de DOUBLON_EXACT
    if [i for i in check_duplicates("très très lente") if i[0] == "DOUBLON_EXACT"]:
        errors.append("BUG-4 très très : faux positif DOUBLON_EXACT")

    # BUG-4 : كيف كيف → pas de DOUBLON_EXACT
    if [i for i in check_duplicates("كونكسيو ضعيفة كيف كيف") if i[0] == "DOUBLON_EXACT"]:
        errors.append("BUG-4 كيف كيف : faux positif DOUBLON_EXACT")

    # BUG-4 : vrai doublon exact toujours détecté
    if not [i for i in check_duplicates("connexion connexion lente") if i[0] == "DOUBLON_EXACT"]:
        errors.append("BUG-4 : vrai doublon exact non détecté")

    # BUG-5 : optique/que → pas de DOUBLON_SUFFIXE
    if [i for i in check_duplicates("fibre optique que vous promettez") if i[0] == "DOUBLON_SUFFIXE"]:
        errors.append("BUG-5 optique/que : faux positif DOUBLON_SUFFIXE")

    # BUG-5 : shout/out → pas de DOUBLON_SUFFIXE
    if [i for i in check_duplicates("a big shout out to the team") if i[0] == "DOUBLON_SUFFIXE"]:
        errors.append("BUG-5 shout/out : faux positif DOUBLON_SUFFIXE")

    # BUG-6 : vpn → pas de RÉSIDU_ARABIZI
    if [i for i in check_arabizi_residue("ندير vpn باش نتفرج") if "vpn" in i[1]]:
        errors.append("BUG-6 vpn : faux positif")

    # BUG-6 : dbm → pas de RÉSIDU_ARABIZI
    if [i for i in check_arabizi_residue("الإشارة مليحة 19 dbm") if "dbm" in i[1]]:
        errors.append("BUG-6 dbm : faux positif")

    # BUG-6 : bch → pas de RÉSIDU_ARABIZI
    if [i for i in check_arabizi_residue("الوكالة bch ndirha") if "bch" in i[1]]:
        errors.append("BUG-6 bch : faux positif")

    print("\n" + "=" * 60)
    print("  TESTS INTERNES validate_existing_normalization v3")
    print("=" * 60)
    if errors:
        for e in errors:
            print(f"  ✗ {e}")
        print(f"\n  ❌ {len(errors)} test(s) échoué(s)")
    else:
        print("  ✓ BUG-1 : check_numbers — séquences séparateur OK")
        print("  ✓ BUG-2 : check_duplicates — préfixes arabes OK")
        print("  ✓ BUG-3 : _PROTECTED_TERMS — termes tech OK")
        print("  ✓ BUG-4 : _INTENTIONAL_REPEATS — emphase OK")
        print("  ✓ BUG-5 : seuil suffixe latin >= 4 OK")
        print("  ✓ BUG-6 : vpn / bch / dbm protégés OK")
        print("\n  ✅ Tous les tests passent")
    print("=" * 60 + "\n")
    return len(errors) == 0


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Valide un corpus JSON déjà normalisé")
    ap.add_argument("corpus",     type=Path, nargs="?",
                    help="Fichier JSON avec client_comment + normalized_comment")
    ap.add_argument("--out",      type=Path, default=Path("rapport_anomalies.csv"),
                    help="Fichier CSV de sortie")
    ap.add_argument("--sample",   type=int,  default=None,
                    help="Nb de commentaires à tester (défaut: tout)")
    ap.add_argument("--examples", type=int,  default=3,
                    help="Nb d'exemples par type à afficher (défaut: 3)")
    ap.add_argument("--no-csv",   action="store_true",
                    help="Ne pas générer le CSV")
    ap.add_argument("--test",     action="store_true",
                    help="Lancer les tests internes uniquement")
    args = ap.parse_args()

    if args.test or args.corpus is None:
        ok = _run_self_tests()
        sys.exit(0 if ok else 1)

    if not args.corpus.exists():
        print(f"Erreur : fichier introuvable → {args.corpus}")
        sys.exit(1)

    print(f"Chargement : {args.corpus}")
    corpus = load_json(args.corpus)
    print(f"  {len(corpus)} entrées chargées")

    rows = validate(corpus, sample_size=args.sample)

    if rows:
        print_examples(rows, n_per_type=args.examples)
        if not args.no_csv:
            save_csv(rows, args.out)