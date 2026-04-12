#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tests/test_pipeline.py
Tests unitaires pour le pipeline ChatBot
Lancer : python -m pytest tests/ -v
"""

import sys
import os
import json
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from preprocessing.darija_mapper import (
    normaliser,
    detecter_langue,
    traduire_darija,
    supprimer_urls,
    supprimer_emojis,
    extraire_emojis,
)


# ============================================================
# TESTS PREPROCESSING
# ============================================================
class TestPreprocessing:

    def test_supprimer_urls(self):
        texte = "regarde ce lien https://example.com pour plus d'info"
        result = supprimer_urls(texte)
        assert "https" not in result
        assert "example.com" not in result

    def test_supprimer_emojis(self):
        texte = "salam 😡😡 wach kayn internet ?"
        result = supprimer_emojis(texte)
        assert "😡" not in result
        assert "salam" in result

    def test_extraire_emojis(self):
        texte = "wach rak 😊 labas 🎉"
        emojis = extraire_emojis(texte)
        assert "😊" in emojis
        assert "🎉" in emojis

    def test_traduire_darija_salam(self):
        result = traduire_darija("salam")
        assert "bonjour" in result

    def test_traduire_darija_bezzaf(self):
        result = traduire_darija("bezzaf")
        assert "beaucoup" in result

    def test_normaliser_complet(self):
        texte = "wach kayn internet? ma tkhdemch 😡 @user #probleme"
        result = normaliser(texte)
        assert "@user" not in result
        assert "#probleme" not in result
        assert "😡" not in result

    def test_detecter_langue_fr(self):
        assert detecter_langue("bonjour comment allez-vous") == "fr"

    def test_detecter_langue_darija(self):
        assert detecter_langue("wach rak 3ndi problème") == "darija"

    def test_detecter_langue_ar(self):
        assert detecter_langue("السلام عليكم كيف حالك") == "ar"

    def test_normaliser_vide(self):
        assert normaliser("") == ""

    def test_normaliser_urls_uniquement(self):
        assert normaliser("https://google.com").strip() == ""


# ============================================================
# TESTS DONNÉES
# ============================================================
class TestDonnees:

    def test_intentions_json_existe(self):
        assert os.path.exists("data/intentions.json")

    def test_intentions_json_valide(self):
        with open("data/intentions.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "intentions" in data
        assert len(data["intentions"]) > 0

    def test_chaque_intention_a_patterns(self):
        with open("data/intentions.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        for intent in data["intentions"]:
            assert "tag" in intent
            assert "responses" in intent
            assert len(intent["responses"]) >= 1

    def test_tags_uniques(self):
        with open("data/intentions.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        tags = [i["tag"] for i in data["intentions"]]
        assert len(tags) == len(set(tags)), "Tags dupliqués détectés"


# ============================================================
# LANCEMENT DIRECT
# ============================================================
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
