#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline complet de nettoyage avancé de commentaires_normalises
--------------------------------------------------------------
1. Correction des emojis (Spark multi-nœuds)
2. Nettoyage avancé (suppression + normalisation)
3. Regroupement (déduplication) → dataset_unifie_sans_doublons
"""

import subprocess
import sys
import os
import re
import time
from pymongo import MongoClient, UpdateOne
import pandas as pd
from collections import Counter

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
SOURCE_COLL = "commentaires_normalises"
TEMP_COLL = "commentaires_normalises_etape1"
FINAL_COLL = "dataset_unifie_final"

# Chemin du script Spark (à adapter)
SPARK_SCRIPT = "/home/mouna/projet_telecom/scripts/nettoyage/Nettoyage_des_textes/code/corriger_structure_emojis.py"  # à modifier

# ============================================================
# 1. EXÉCUTION DU SCRIPT SPARK (correction des emojis)
# ============================================================
def step1_correction_emojis():
    print("\n" + "="*70)
    print("ÉTAPE 1 : Correction des emojis (Spark multi-nœuds)")
    print("="*70)
    # Le script Spark doit être configuré pour lire SOURCE_COLL et écrire dans TEMP_COLL
    # On peut modifier dynamiquement les constantes dans le script avant de l'exécuter
    # Pour simplifier, on suppose que le script est déjà paramétré avec les bonnes collections
    # Sinon, on peut le faire avec sed ou en passant des arguments.
    # Ici, on lance le script tel quel.
    cmd = ["python3", SPARK_SCRIPT]
    print(f"Lancement : {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("❌ Étape Spark échouée.")
        sys.exit(1)
    print("✅ Étape 1 terminée.")

# ============================================================
# 2. NETTOYAGE AVANCÉ (code clean_telecom_FINAL_v3.py adapté)
# ============================================================
# On reprend les fonctions et patterns du code 1, mais en les appliquant à TEMP_COLL
# Pour éviter la duplication, on les définit ici.

# --- PATTERNS DE SUPPRESSION (extraits du code 1) ---
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
TECH_WORDS = re.compile(
    r'\b(4g|5g|3g|sbox|inbox|lte|apn|sms|mms|ussd|forfait|recharge|solde)\b',
    re.IGNORECASE,
)
CHARABIA_LAT = re.compile(r'\b[b-df-hj-np-tv-z]{6,}\b', re.IGNORECASE)
CHARABIA_AR  = re.compile(r'[\u0621-\u064A]{1,2}([\u0621-\u064A])\1{3,}')
SUBS         = re.compile(r'\b\w{3,}\b')

BRUIT_LAT = re.compile(
    r'^[\s]*(?:h{3,}|(?:[a-z]{1,3})?([a-z])\1{3,}|lol+|mdr+|xd+|haha+|hihi+|héhé+)[\s!.]*$',
    re.IGNORECASE,
)
URL_ONLY = re.compile(
    r'^[\s]*(?:https?[\s:/\\]+\S*|www\.\S+)[\s]*$',
    re.IGNORECASE,
)
VIDE_CONTENT = re.compile(
    r'^[\s]*(?:'
    r'كيفاش|كيف|وقتاش|شحال|واش|علاش|فين|قداش|'
    r'ok|oui|non|no|yes|yep|nope|'
    r'تم|يخي|ايه|إيه|آه|واه|لا|ها|هه|'
    r'خي|يا خي|آ خي|بلا|هاك|هاكا|هيا|يلا|'
    r'[اى]{3,}|ات+ال+ات+|[أإآا]{4,}'
    r')[\s?!.،؟…]*$',
    re.IGNORECASE | re.UNICODE,
)
TECH_UNIT_ONLY = re.compile(
    r'^[\s]*\d*[\s]*(mbps|mb/s|kbps|gb/s|mbits?|mb|gb|go|mo|ko|ghz|mhz|'
    r'adsl|vdsl|4g|5g|3g|2g|lte|ftth|fttb|gpon|ping|dns)[\s]*$',
    re.IGNORECASE,
)
INTERJECTION_VIDE = re.compile(
    r'^[\s]*(?:'
    r"let'?s\s*goo*|yall?a+|yala+|cool+|super+|bravo+|bien+|ni|lies?|"
    r"intéressé[e]?|interested|photo(?:\s+\w{1,10})?|ok\s+cool|cool\s+ok"
    r')[\s?!.،؟…]*$',
    re.IGNORECASE | re.UNICODE,
)
PRIX_MOT_SEUL = re.compile(
    r'^[\s]*(?:'
    r'(?:le\s+)?prix|سعر|ثمن|تمن'
    r')[\s?!.،؟…]*$',
    re.IGNORECASE | re.UNICODE,
)
CHIFFRE_MOT_VAGUE = re.compile(
    r'^[\s]*(?:'
    r'\d[\d\s]*(?:ooo+|millions?|billions?|three|two|one|zero|com\b.*)'
    r'|millions?|billions?'
    r')[\s?!.،؟…]*$',
    re.IGNORECASE | re.UNICODE,
)
VILLE_SEULE = re.compile(
    r'^[\s]*(?:'
    r'sidi\s+bel\s+abb[eè]s|oran|alger(?:s)?|constantine|annaba|'
    r'tizi\s+ouzou|bejaia|béjaïa|blida|setif|sétif|batna|biskra|'
    r'tlemcen|chlef|médéa|mostaganem|mascara|djelfa|m.?sila|'
    r'skikda|jijel|guelma|souk\s+ahras|tiaret|ghardaia|bechar'
    r')[\s?!.,،؟…]*$',
    re.IGNORECASE | re.UNICODE,
)
CHARABIA_DIACRITIC = re.compile(
    r'^[\s\w]{0,5}[żźśøüðþæœ][\s\w]{0,10}$',
    re.IGNORECASE | re.UNICODE,
)
INTERJECTION_AR_VIDE = re.compile(
    r'^[\s]*(?:'
    r'[يه]{2,}[اوى]+[اه]*|هم+|هه+خ+|ام+|اهه+|'
    r'هذا\s*(?:4g|5g|3g|lte|adsl|wifi)|'
    r'واي\s*فاي\s*\d+|سيدي\s+\S+(?:\s+\S+)?'
    r')[\s?!.،؟…]*$',
    re.IGNORECASE | re.UNICODE,
)
CHIFFRE_INTERJECTION = re.compile(
    r'^[\s]*\d[\d\s]*\s*(?:'
    r'ام+|اهه+|هم+|همم+|اممم+|ping\s+adsl|adsl\s+ping'
    r')[\s?!.،؟…]*$',
    re.IGNORECASE | re.UNICODE,
)
TECH_ARTICLE_SEUL = re.compile(
    r'^[\s]*(?:la\s+|le\s+|les\s+|une?\s+|du\s+|de\s+)?'
    r'(?:fibre|fiber|adsl|vdsl|4g|5g|3g|lte|ftth|fttb|wifi|connexion|réseau|network)'
    r'(?:\s+\d+)?[\s?!.،؟…]*$',
    re.IGNORECASE | re.UNICODE,
)
PING_TECH_SEUL = re.compile(
    r'^[\s]*(?:ping\s*\d*\s*(?:adsl|vdsl|4g|5g|lte|ms)?'
    r'|\d+\s*(?:ping|ms)\s*(?:adsl|vdsl|4g|5g)?)[\s?!.]*$',
    re.IGNORECASE,
)
SEQUENCE_AR_VIDE = re.compile(
    r'^[\s]*[يا]{3,}[\s]*[يا]{0,5}[\s?!.،؟…]*$',
    re.UNICODE,
)

def _is_low_vowel_noise(text: str) -> bool:
    t = text.strip().lower()
    if not t or len(t) > 12 or not t.isalpha():
        return False
    vowels = sum(1 for c in t if c in 'aeiouéèêëàâùûü')
    return (vowels / len(t)) < 0.15

def _is_suppressed(t: str):
    if BRUIT_LAT.match(t):
        return "bruit_rire_clavier"
    if URL_ONLY.match(t):
        return "url_seule"
    if VIDE_CONTENT.match(t):
        return "contenu_vide"
    if TECH_UNIT_ONLY.match(t):
        return "technique_seul"
    if PRIX_MOT_SEUL.match(t):
        return "prix_detecte"
    if INTERJECTION_VIDE.match(t):
        return "contenu_vide"
    if CHIFFRE_MOT_VAGUE.match(t):
        return "contenu_vide"
    if VILLE_SEULE.match(t):
        return "contenu_vide"
    if INTERJECTION_AR_VIDE.match(t):
        return "contenu_vide"
    if CHIFFRE_INTERJECTION.match(t):
        return "contenu_vide"
    if CHARABIA_DIACRITIC.match(t):
        return "charabia_clavier"
    if TECH_ARTICLE_SEUL.match(t):
        return "technique_seul"
    if PING_TECH_SEUL.match(t):
        return "technique_seul"
    if SEQUENCE_AR_VIDE.match(t):
        return "contenu_vide"
    if _is_low_vowel_noise(t):
        return "charabia_clavier"
    if PRIX_RE.search(t):
        rest = PRIX_RE.sub("", t).strip()
        if len(SUBS.findall(rest)) <= 1:
            return "prix_detecte"
    if TEL_RE.search(t):
        rest = TEL_RE.sub("", t).strip()
        if len(SUBS.findall(rest)) <= 2:
            return "telephone_detecte"
    tech_rest = re.sub(r'\d+', '', TECH_WORDS.sub("", t)).strip()
    if len(SUBS.findall(tech_rest)) == 0 and len(t.split()) <= 6:
        return "technique_seul"
    lat = CHARABIA_LAT.findall(t)
    if lat and sum(len(m) for m in lat) > len(t) * 0.35:
        return "charabia_clavier"
    if CHARABIA_AR.search(t):
        ca = CHARABIA_AR.sub("", t).strip()
        if len(ca) < len(t) * 0.45:
            return "charabia_arabe"
    return None

# --- NORMALISATION (extrait du code 1) ---
from dict_data import DICT_NORM  # Assurez-vous que ce fichier existe

_UNICODE_TABLE = str.maketrans(DICT_NORM["unicode_arabic"])
_EMOJIS = sorted(DICT_NORM["emojis"].items(), key=lambda x: len(x[0]), reverse=True)
_ARABIZI_UP = [(re.compile(rf'\b{re.escape(k)}\b'), v)
               for k, v in sorted(DICT_NORM["arabizi_upper"].items(), key=lambda x: len(x[0]), reverse=True)]
_ARABIZI = [(re.compile(rf'\b{re.escape(k)}\b', re.IGNORECASE), v)
            for k, v in sorted(DICT_NORM["arabizi_words"].items(), key=lambda x: len(x[0]), reverse=True)]
_MIXED = []
for _p, _r in DICT_NORM["mixed_ar_fr_regex"].items():
    try:
        _MIXED.append((re.compile(_p, re.IGNORECASE | re.UNICODE), _r))
    except re.error:
        pass
_nv = DICT_NORM["network_variants"]
_NV = re.compile(
    r'\b(?:' + '|'.join(re.escape(w) for w in _nv["latin"]) + r')\b'
    + r'|(?:' + '|'.join(re.escape(w) for w in _nv["arabic"]) + r')',
    re.IGNORECASE,
)
_NV_REPL = _nv["normalized_form"]
_ABBREV = [(re.compile(rf'\b{re.escape(k)}\b', re.IGNORECASE), v)
           for k, v in sorted(DICT_NORM["abbreviations"].items(), key=lambda x: len(x[0]), reverse=True)]
_TELECOM = [(re.compile(rf'\b{re.escape(k)}\b', re.IGNORECASE), v)
            for k, v in sorted(DICT_NORM["telecom_tech"].items(), key=lambda x: len(x[0]), reverse=True)]
_FR = []
for _p, _r in DICT_NORM["french_corrections_regex"].items():
    try:
        _FR.append((re.compile(_p, re.IGNORECASE), _r))
    except re.error:
        pass
_HASHTAG = re.compile(r'#\w+')
_EXTRA   = re.compile(r'\s{2,}')
_GLUE1   = re.compile(r'([a-zA-Z\u0621-\u064A])(\d)', re.UNICODE)
_GLUE2   = re.compile(r'(\d)([a-zA-Z\u0621-\u064A])', re.UNICODE)

CORRECTIONS_AR = [
    (re.compile(r'\bمعاكم\b', re.UNICODE), 'معكم'),
    (re.compile(r'\bاالتزام\b', re.UNICODE), 'التزام'),
    (re.compile(r'\bبيس\b', re.UNICODE), 'شريحة'),
    (re.compile(r'\bالبونوس\b', re.UNICODE), 'بونوس'),
    (re.compile(r'[اأإآ]{2,}', re.UNICODE), 'ا'),
    (re.compile(r'\bيخي\b', re.UNICODE), ''),
    (re.compile(r'-{2,}'), ' '),
    (re.compile(r'[—–]{2,}'), ' '),
]

def clean_advanced(text: str):
    if not isinstance(text, str) or not text.strip():
        return None, "vide"
    t = text.strip()
    reason = _is_suppressed(t)
    if reason:
        return None, reason
    t = t.translate(_UNICODE_TABLE)
    for pat, repl in CORRECTIONS_AR:
        t = pat.sub(repl, t)
    for em, label in _EMOJIS:
        t = t.replace(em, f" {label} ")
    for pat, r in _ARABIZI_UP:
        t = pat.sub(r, t)
    for pat, r in _ARABIZI:
        t = pat.sub(r, t)
    for pat, r in _MIXED:
        t = pat.sub(r, t)
    t = _NV.sub(_NV_REPL, t)
    for pat, r in _ABBREV:
        t = pat.sub(r, t)
    for pat, r in _TELECOM:
        t = pat.sub(r, t)
    for pat, r in _FR:
        t = pat.sub(r, t)
    t = t.lower()
    t = _HASHTAG.sub("", t)
    t = _GLUE1.sub(r'\1 \2', t)
    t = _GLUE2.sub(r'\1 \2', t)
    t = _EXTRA.sub(" ", t).strip()
    if not t or len(t.strip()) < 2:
        return None, "vide_apres_nettoyage"
    return t, None

def step2_advanced_cleaning():
    print("\n" + "="*70)
    print("ÉTAPE 2 : Nettoyage avancé (suppression + normalisation)")
    print("="*70)
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[TEMP_COLL]
    total = coll.count_documents({})
    print(f"📊 {total} documents à traiter dans {TEMP_COLL}")
    bulk_ops = []
    updated = 0
    suppressed = 0
    BATCH = 500
    for doc in coll.find({}, {"_id": 1, "normalized_arabert": 1, "normalized_full": 1}):
        ca, ra = clean_advanced(doc.get("normalized_arabert", ""))
        cf, rf = clean_advanced(doc.get("normalized_full", ""))
        update = {}
        if ca is not None:
            update["normalized_arabert"] = ca
        else:
            update["normalized_arabert_supprime"] = ra
        if cf is not None:
            update["normalized_full"] = cf
        else:
            update["normalized_full_supprime"] = rf
        update["nettoyage_v3_applique"] = True
        if ca is not None or cf is not None:
            updated += 1
        else:
            suppressed += 1
        bulk_ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": update}))
        if len(bulk_ops) >= BATCH:
            coll.bulk_write(bulk_ops, ordered=False)
            bulk_ops = []
    if bulk_ops:
        coll.bulk_write(bulk_ops, ordered=False)
    client.close()
    print(f"✅ Nettoyage avancé terminé : {updated} conservés, {suppressed} supprimés (marqués)")

# ============================================================
# 3. REGROUPEMENT (déduplication)
# ============================================================
def step3_grouping():
    print("\n" + "="*70)
    print("ÉTAPE 3 : Regroupement (déduplication) → dataset_unifie_sans_doublons")
    print("="*70)
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[TEMP_COLL]
    # Chargement en DataFrame
    cursor = coll.find({})
    df = pd.DataFrame(list(cursor))
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)
    if 'normalized_arabert' not in df.columns:
        print("❌ Colonne 'normalized_arabert' absente.")
        sys.exit(1)
    colonne_texte = 'normalized_arabert'
    # Convertir numériques
    for col in ['confidence', 'score']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    groupes = df.groupby(colonne_texte)
    resultats = []
    group_id = 1
    def get_first_value(values):
        for v in values:
            if v and str(v) != 'nan' and str(v) != '[]':
                return v
        return None
    for texte, groupe in groupes:
        first_id = groupe.iloc[0]['_id'] if '_id' in groupe.columns else None
        dates = groupe['date'].tolist() if 'date' in df.columns else []
        sources = groupe['source'].unique().tolist() if 'source' in df.columns else []
        moderateurs = groupe['moderateur'].unique().tolist() if 'moderateur' in df.columns else []
        labels = groupe['label'].tolist() if 'label' in df.columns else []
        commentaire_moderateur = get_first_value(groupe['commentaire_moderateur'].tolist()) if 'commentaire_moderateur' in df.columns else None
        statut = get_first_value(groupe['statut'].tolist()) if 'statut' in df.columns else None
        commentaire_client = get_first_value(groupe['Commentaire_Client'].tolist()) if 'Commentaire_Client' in df.columns else None
        normalized_full = get_first_value(groupe['normalized_full'].tolist()) if 'normalized_full' in df.columns else None
        emojis_originaux = get_first_value(groupe['emojis_originaux'].tolist()) if 'emojis_originaux' in df.columns else None
        emojis_sentiment = get_first_value(groupe['emojis_sentiment'].tolist()) if 'emojis_sentiment' in df.columns else None
        score = get_first_value(groupe['score'].tolist()) if 'score' in df.columns else None
        confidence = get_first_value(groupe['confidence'].tolist()) if 'confidence' in df.columns else None
        reason = get_first_value(groupe['reason'].tolist()) if 'reason' in df.columns else None
        anote = get_first_value(groupe['annoté'].tolist()) if 'annoté' in df.columns else None
        if labels:
            cnt = Counter(labels)
            label_maj = cnt.most_common(1)[0][0]
            nb_maj = cnt.most_common(1)[0][1]
            if nb_maj / len(labels) >= 0.6:
                label_final = label_maj
                conflit = False
            else:
                label_final = "CONFLIT_A_REVOIR"
                conflit = True
        else:
            label_final = None
            conflit = False
        doc = {
            '_id': first_id,
            'Group_ID': f"groupe_{group_id:04d}",
            'nb_occurrences': len(groupe),
            'commentaire_moderateur': commentaire_moderateur,
            'statut': statut,
            'Commentaire_Client': commentaire_client,
            'normalized_arabert': texte,
            'normalized_full': normalized_full,
            'emojis_originaux': emojis_originaux,
            'emojis_sentiment': emojis_sentiment,
            'score': round(score, 2) if score and not pd.isna(score) else None,
            'confidence': round(confidence, 2) if confidence and not pd.isna(confidence) else None,
            'reason': reason,
            'annoté': anote,
            'label_final': label_final,
            'conflit': conflit,
            'dates': dates,
            'sources': sources,
            'moderateurs': moderateurs,
            'labels_originaux': labels
        }
        resultats.append(doc)
        group_id += 1
    df_final = pd.DataFrame(resultats)
    # Écrire dans MongoDB
    if FINAL_COLL in db.list_collection_names():
        db[FINAL_COLL].drop()
        print(f"   Ancienne collection '{FINAL_COLL}' supprimée.")
    documents = df_final.to_dict('records')
    for doc in documents:
        for k, v in doc.items():
            if isinstance(v, list):
                doc[k] = ','.join(str(x) for x in v)
    db[FINAL_COLL].insert_many(documents)
    print(f"✅ Collection '{FINAL_COLL}' créée avec {len(documents)} documents.")
    client.close()

# ============================================================
# MAIN : EXÉCUTION DES 3 ÉTAPES
# ============================================================
if __name__ == "__main__":
    print("🚀 PIPELINE COMPLET DE NETTOYAGE AVANCÉ")
    print(f"Source : {SOURCE_COLL} → {TEMP_COLL} → {FINAL_COLL}")
    # Étape 1 : correction des emojis (Spark)
    rep = input("Lancer l'étape 1 (correction emojis) ? (oui/non) : ")
    if rep.lower() == 'oui':
        step1_correction_emojis()
    else:
        print("Étape 1 ignorée. Assurez-vous que la collection temporaire existe.")
    # Étape 2 : nettoyage avancé
    rep = input("Lancer l'étape 2 (nettoyage avancé) ? (oui/non) : ")
    if rep.lower() == 'oui':
        step2_advanced_cleaning()
    # Étape 3 : regroupement
    rep = input("Lancer l'étape 3 (regroupement final) ? (oui/non) : ")
    if rep.lower() == 'oui':
        step3_grouping()
    print("🏁 Pipeline terminé.")