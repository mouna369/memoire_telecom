#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
darija_mapper.py
Normalisation et traduction du dialecte algérien (Darija) vers le français.
Gère : Darija latinisé (chiffres arabes), arabe dialectal, français.
"""

import re
import json
import emoji
from langdetect import detect, LangDetectException
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

# Chargement du dictionnaire
with open(BASE_DIR / "data" / "synonymes_darija.json", encoding="utf-8") as f:
    _dico = json.load(f)

DARIJA_MAP   = _dico["darija_to_french"]
ARABIC_MAP   = _dico["arabic_to_french"]

# Normalisation des chiffres arabes en phonèmes latins
PHONEME_MAP = {
    "2": "a",    # ء / أ
    "3": "ain",  # ع
    "7": "h",    # ح
    "9": "q",    # ق
}


def detecter_langue(texte: str) -> str:
    """Détecte la langue : 'fr', 'ar', 'darija_latin' ou 'inconnu'."""
    texte_clean = re.sub(r'[^\w\s]', '', texte).strip()
    if not texte_clean:
        return "inconnu"
    # Si contient des chiffres arabes phonétiques (3, 7, 9...)
    if re.search(r'[2379]', texte_clean):
        return "darija_latin"
    try:
        lang = detect(texte_clean)
        if lang in ("ar", "fa"):
            return "ar"
        elif lang == "fr":
            return "fr"
        return "darija_latin"
    except LangDetectException:
        return "darija_latin"


def normaliser_chiffres_phonemes(texte: str) -> str:
    """Remplace les chiffres phonétiques arabes : 3 → ain, 7 → h, etc."""
    for chiffre, phoneme in PHONEME_MAP.items():
        texte = texte.replace(chiffre, phoneme)
    return texte


def traduire_darija_latin(texte: str) -> str:
    """Traduit les mots darija latinisés en français."""
    mots = texte.split()
    traduits = []
    i = 0
    while i < len(mots):
        # Essayer bigramme d'abord
        if i + 1 < len(mots):
            bigramme = mots[i] + " " + mots[i+1]
            if bigramme in DARIJA_MAP:
                traduits.append(DARIJA_MAP[bigramme])
                i += 2
                continue
        # Puis unigramme
        mot = mots[i]
        traduits.append(DARIJA_MAP.get(mot, mot))
        i += 1
    return ' '.join(traduits)


def traduire_arabe(texte: str) -> str:
    """Traduit les expressions arabes dialectales en français."""
    for ar, fr in ARABIC_MAP.items():
        texte = texte.replace(ar, fr)
    return texte


def nettoyer_texte(texte: str) -> str:
    """Nettoyage général : URLs, mentions, emojis, ponctuation excessive."""
    texte = emoji.replace_emoji(texte, replace=' ')
    texte = re.sub(r'http\S+|www\.\S+', '', texte)
    texte = re.sub(r'@\w+', '', texte)
    texte = re.sub(r'#\w+', '', texte)
    texte = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', texte)
    texte = re.sub(r'\s+', ' ', texte).strip()
    return texte.lower()


def normaliser(texte: str) -> str:
    """
    Pipeline complet de normalisation :
    1. Nettoyage
    2. Détection de langue
    3. Traduction selon la langue détectée
    4. Normalisation finale
    """
    if not texte or not texte.strip():
        return ""

    texte = nettoyer_texte(texte)
    langue = detecter_langue(texte)

    if langue == "ar":
        texte = traduire_arabe(texte)
    elif langue == "darija_latin":
        texte = normaliser_chiffres_phonemes(texte)
        texte = traduire_darija_latin(texte)
    # Si fr : pas de traduction nécessaire

    texte = re.sub(r'\s+', ' ', texte).strip()
    return texte


if __name__ == "__main__":
    exemples = [
        "wach kayn connexion 4G ?",
        "ma kaynach internet",
        "واش راك صحبي",
        "salam bghit ncharg mon forfait",
        "3ndi problème réseau bezzaf",
        "choukran barak allah fik",
    ]
    print("=== Test du normaliser ===")
    for ex in exemples:
        print(f"  Entrée  : {ex}")
        print(f"  Sortie  : {normaliser(ex)}")
        print()
