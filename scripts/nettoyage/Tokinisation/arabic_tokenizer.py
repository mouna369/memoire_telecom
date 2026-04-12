"""
arabic_tokenizer.py
=========================
Tokenisation multi-langues - 
"""

import re
import logging
from abc import ABC, abstractmethod
from typing import List, Dict

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# DÉTECTION DES BIBLIOTHÈQUES
# ============================================================================

# CamelTools pour l'arabe
try:
    from camel_tools.tokenizers.word import simple_word_tokenize
    from camel_tools.utils.dediac import dediac_ar
    CAMEL_AVAILABLE = True
    logger.info("✓ CamelTools disponible")
except ImportError:
    CAMEL_AVAILABLE = False
    logger.warning("✗ CamelTools non disponible")

# NLTK pour fallback
try:
    import nltk
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt', quiet=True)
    from nltk.tokenize import word_tokenize
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

# spaCy pour français/anglais
try:
    import spacy
    
    # Modèle français
    try:
        nlp_fr = spacy.load("fr_core_news_sm")
        SPACY_FR_AVAILABLE = True
        logger.info("✓ Modèle français chargé")
    except:
        SPACY_FR_AVAILABLE = False
        logger.warning("✗ Modèle français non disponible")
    
    # Modèle anglais
    try:
        nlp_en = spacy.load("en_core_web_sm")
        SPACY_EN_AVAILABLE = True
        logger.info("✓ Modèle anglais chargé")
    except:
        SPACY_EN_AVAILABLE = False
        logger.warning("✗ Modèle anglais non disponible")
        
except ImportError:
    SPACY_FR_AVAILABLE = False
    SPACY_EN_AVAILABLE = False
    logger.warning("✗ spaCy non disponible")

# ============================================================================
# CONSTANTES
# ============================================================================

ARABIC_SCRIPT = re.compile(r'[\u0600-\u06FF]+')

# Dictionnaire complet des contractions françaises
FRENCH_CONTRACTIONS = {
    # Mots complets
    "aujourd'hui": "AUJOURD_HUI",
    
    # Pronoms + verbes
    "j'aime": "J_AIME",
    "j'ai": "J_AI",
    "j'étais": "J_ETAIS",
    "j'aurais": "J_AURAIS",
    "j'avais": "J_AVAIS",
    "j'aurai": "J_AURAI",
    "j'avais": "J_AVAIS",
    
    "c'est": "C_EST",
    "c'était": "C_ETAIT",
    "c'étais": "C_ETAIS",
    "c'étaient": "C_ETAIENT",
    
    "m'aime": "M_AIME",
    "m'ai": "M_AI",
    "m'as": "M_AS",
    "m'a": "M_A",
    
    "t'aime": "T_AIME",
    "t'ai": "T_AI",
    "t'as": "T_AS",
    "t'a": "T_A",
    
    "s'aime": "S_AIME",
    "s'ai": "S_AI",
    "s'est": "S_EST",
    "s'était": "S_ETAIT",
    
    "n'aime": "N_AIME",
    "n'ai": "N_AI",
    "n'est": "N_EST",
    "n'était": "N_ETAIT",
    "n'aurais": "N_AURAIS",
    
    "l'aime": "L_AIME",
    "l'ai": "L_AI",
    "l'a": "L_A",
    "l'as": "L_AS",
    "l'avait": "L_AVAIT",
    
    # Prépositions + mots (TRÈS IMPORTANT)
    "d'accord": "D_ACCORD",
    "d'abord": "D_ABORD",
    "d'ailleurs": "D_AILLEURS",
    "d'habitude": "D_HABITUDE",
    "d'orange": "D_ORANGE",
    "d'ooredoo": "D_OOREDOO",
    "d'aide": "D_AIDE",
    "d'eau": "D_EAU",
    "d'un": "D_UN",
    "d'une": "D_UNE",
    "d'entre": "D_ENTRE",
    
    # Conjonctions
    "qu'il": "QU_IL",
    "qu'elle": "QU_ELLE",
    "qu'on": "QU_ON",
    "qu'ils": "QU_ILS",
    "qu'elles": "QU_ELLES",
    "qu'importe": "QU_IMPORTE",
}

# Mots-clés pour détection de langue
FRENCH_KEYWORDS = {
    'le', 'la', 'les', 'un', 'une', 'des', 'je', 'tu', 'il', 'elle',
    'nous', 'vous', 'ils', 'elles', 'et', 'ou', 'mais', 'donc',
    'est', 'sont', 'ce', 'cet', 'cette', 'dans', 'pour', 'par',
    'sur', 'avec', 'tres', 'bien', 'mal', 'trop', 'aujourd'
}

ENGLISH_KEYWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'if', 'because', 'as',
    'of', 'at', 'by', 'for', 'with', 'about', 'in', 'on',
    'very', 'good', 'bad', 'this', 'that', 'these', 'those'
}

# ============================================================================
# DÉTECTION DE LANGUE
# ============================================================================

def detect_language(text: str) -> str:
    """Détecte la langue principale d'un texte."""
    if not text:
        return 'unknown'
    
    # Détection arabe
    if ARABIC_SCRIPT.search(text):
        return 'ar'
    
    # Détection français/anglais
    words = text.lower().split()
    fr_count = sum(1 for w in words if w in FRENCH_KEYWORDS)
    en_count = sum(1 for w in words if w in ENGLISH_KEYWORDS)
    
    if fr_count > en_count:
        return 'fr'
    elif en_count > fr_count:
        return 'en'
    else:
        return 'mixed'

# ============================================================================
# TOKENIZER PRINCIPAL AVEC PRÉSERVATION DES CONTRACTIONS
# ============================================================================

class MultilingualTokenizer:
    """
    Tokenizer intelligent multi-langues avec préservation des contractions.
    """
    
    def __init__(self):
        print("\n" + "=" * 60)
        print("  Initialisation du tokenizer multilingue")
        print("=" * 60)
        
        # Initialisation des tokenizers
        self.tokenizers = {}
        
        if CAMEL_AVAILABLE:
            self.tokenizers['ar'] = self._init_camel()
            logger.info("  ✓ Tokenizer arabe prêt")
        
        if SPACY_FR_AVAILABLE:
            self.tokenizers['fr'] = self._init_spacy('fr')
            logger.info("  ✓ Tokenizer français prêt")
        
        if SPACY_EN_AVAILABLE:
            self.tokenizers['en'] = self._init_spacy('en')
            logger.info("  ✓ Tokenizer anglais prêt")
        
        # Tokenizer par défaut
        self.tokenizers['mixed'] = self._init_nltk() if NLTK_AVAILABLE else self._init_simple()
        logger.info("  ✓ Tokenizer fallback prêt")
        
        print("\n" + "=" * 60)
    
    def _init_camel(self):
        """Initialise le tokenizer CamelTools."""
        def tokenize(text):
            text = dediac_ar(text)
            return simple_word_tokenize(text)
        return tokenize
    
    def _init_spacy(self, lang):
        """Initialise le tokenizer spaCy."""
        nlp = nlp_fr if lang == 'fr' else nlp_en
        def tokenize(text):
            doc = nlp(text)
            return [token.text for token in doc]
        return tokenize
    
    def _init_nltk(self):
        """Initialise le tokenizer NLTK."""
        return word_tokenize
    
    def _init_simple(self):
        """Initialise le tokenizer simple."""
        return lambda text: text.split()
    
    def _preserve_french_contractions(self, text: str) -> str:
        """
        Remplace temporairement les contractions françaises par des marqueurs.
        """
        text_lower = text.lower()
        result = text
        
        # Trier par longueur décroissante
        contractions = sorted(FRENCH_CONTRACTIONS.keys(), key=len, reverse=True)
        
        for contraction in contractions:
            if contraction in text_lower:
                pattern = re.compile(re.escape(contraction), re.IGNORECASE)
                result = pattern.sub(FRENCH_CONTRACTIONS[contraction], result)
        
        return result
    
    def _restore_french_contractions(self, tokens: List[str]) -> List[str]:
        """
        Restaure les contractions françaises après tokenization.
        """
        restored = []
        i = 0
        while i < len(tokens):
            token = tokens[i]
            
            # Vérifier si c'est un marqueur de contraction
            if '_' in token and any(token.upper() == v for v in FRENCH_CONTRACTIONS.values()):
                # Trouver la contraction originale
                found = False
                for orig, marker in FRENCH_CONTRACTIONS.items():
                    if marker == token.upper():
                        restored.append(orig)
                        found = True
                        break
                if not found:
                    restored.append(token)
            else:
                restored.append(token)
            i += 1
        
        return restored
    
    def _merge_d_apostrophe(self, tokens: List[str]) -> List[str]:
        """
        Fusionne spécifiquement les tokens "d'" avec le mot suivant.
        """
        merged = []
        i = 0
        while i < len(tokens):
            if i < len(tokens) - 1 and tokens[i] == "d'":
                # Fusionner "d'" avec le mot suivant
                next_token = tokens[i + 1]
                merged.append(f"d'{next_token}")
                i += 2
            else:
                merged.append(tokens[i])
                i += 1
        return merged
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokenise le texte avec préservation des contractions.
        """
        if not text:
            return []
        
        # Détection de la langue
        lang = detect_language(text)
        
        # Pour le français, préserver les contractions
        if lang == 'fr':
            text = self._preserve_french_contractions(text)
        
        # Tokenization
        tokenizer = self.tokenizers.get(lang, self.tokenizers['mixed'])
        tokens = tokenizer(text)
        
        # Post-traitement pour le français
        if lang == 'fr':
            tokens = self._restore_french_contractions(tokens)
            tokens = self._merge_d_apostrophe(tokens)
        
        return tokens
    
    def tokenize_ngrams(self, text: str, n: int = 2) -> List[str]:
        """Génère des n-grammes."""
        tokens = self.tokenize(text)
        if len(tokens) < n:
            return []
        return [' '.join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]
    
    def detect_language(self, text: str) -> str:
        """Retourne la langue détectée."""
        return detect_language(text)

# ============================================================================
# FACTORY
# ============================================================================

def get_tokenizer() -> MultilingualTokenizer:
    """Retourne une instance du tokenizer multilingue."""
    return MultilingualTokenizer()

# ============================================================================
# TESTS
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("  TEST DU TOKENIZER MULTILINGUE - VERSION COMPLÈTE")
    print("=" * 60)
    
    # Initialisation
    tok = get_tokenizer()
    
    # Tests
    tests = [
        ("Arabe standard", "خدمة الاتصالات في الجزائر ضعيفة جدا"),
        ("Dialecte algérien", "موبيليس تقطع بزاف وراهي تخسر الزبائن"),
        ("Français", "Le service client d'Ooredoo est catastrophique"),
        ("Anglais", "The network coverage is very poor"),
        ("Apostrophes", "Aujourd'hui, j'aime le réseau d'Orange"),
        ("Contractions", "C'est vraiment pas bien, j'aime pas ça"),
        ("Plus contractions", "J'ai besoin d'aide, c'est important"),
        ("Code-switching", "خدمة العملاء is very bad et vraiment trop lent"),
    ]
    
    for label, text in tests:
        print(f"\n{label}")
        print(f"  Texte  : {text}")
        print(f"  Langue : {tok.detect_language(text)}")
        print(f"  Tokens : {tok.tokenize(text)}")