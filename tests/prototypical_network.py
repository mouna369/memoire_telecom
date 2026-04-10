import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

# ============================================================
# VOS EXEMPLES
# ============================================================

exemples = {
    'negatif': [
        "agence actel ben mhidi est tellement destructurée",
        "ai pas de connexion depuis plus d'un mois",
        "je n'arrive pas à m'inscrire sur l'application",
        "est du chantage",
        "راني في شهرين شريت مودم fibre optique",
        "سرعه الانترنت تحتاج الي vpn",
        "vous êtes pas sérieux ce est la pire société",
        "حشمو. راكم غي تحشيولنا",
        "سلام عليكم عندي تقريبا 20 يوم",
        "مرضتونا بالكذب تاعكم"
    ],
    'positif': [
        "سرعة 1.5 جيغا ممتاز",
        "المودم دقه تميز",
        "الرقم 1 في افريقيا",
        "100 mbps ce est la base",
        "2 go upgrade for free",
        "تحيا الجزائر واتصالات الجزائر",
        "3 مليون مشترك fibre",
        "3 مليون مشترك مبرووك",
        "500 الف سومه مقبوله",
        "500 الف مليح"
    ],
    'neutre': [
        "مقننه",
        "كي تخلاص تلاث اشهر",
        "للاسف صوره تختلف عن الوقع",
        "حي عين بن عمران برج بوعريريج",
        "01 شهر 03 ايام",
        "021211212 رقمكم",
        "سلفولنا 96 ساعه بليز",
        "030754370 xgs-pon",
        "040812260 رابح رقيه",
        "تقدمنا لوكالة اتصلات"
    ],
    'social': [
        "ربي يوفقكم",
        "بارك الله فيكم",
        "بالتوفيق ان شاءالله",
        "جاوبونا على السوال بارك الله فيكم",
        "ربي يحفظكم",
    ],
    'plainte': [
        "ani drt demond ta 3 la line fibre",
        "kolch rakeb w 3 ndi 3 mois",
        "l'équipe virifiwna el الخاص",
        "repondez au الخاص",
        "algerietelecom بعثلكم الخاص ردو"
    ]
}

# ============================================================
# CRÉER LES PROTOTYPES AVEC TF-IDF
# ============================================================

print("🔧 Création des prototypes avec TF-IDF...")

# Rassembler tous les textes d'entraînement
textes_entrainement = []
labels_entrainement = []

for classe, textes in exemples.items():
    for texte in textes:
        textes_entrainement.append(texte)
        labels_entrainement.append(classe)

# TF-IDF Vectorizer
vectorizer = TfidfVectorizer(max_features=1000)
X_train = vectorizer.fit_transform(textes_entrainement)

# 🔧 CONVERSION EN ARRAY NUMPY (au lieu de matrix)
X_train_array = X_train.toarray()

# Créer un prototype par classe
classes_uniques = list(exemples.keys())
prototypes = {}

for classe in classes_uniques:
    # Récupérer les indices des textes de cette classe
    indices = [i for i, label in enumerate(labels_entrainement) if label == classe]
    # Moyenne des vecteurs (en array numpy)
    prototype = np.mean(X_train_array[indices], axis=0)
    prototypes[classe] = prototype.reshape(1, -1)  # Reshape pour cosine_similarity

print(f"✅ Prototypes créés pour : {list(prototypes.keys())}")

# ============================================================
# FONCTION DE PRÉDICTION
# ============================================================

def predire_sentiment(commentaire):
    # Transformer le commentaire
    X_test = vectorizer.transform([commentaire])
    # Convertir en array numpy
    X_test_array = X_test.toarray()
    
    # Comparer avec chaque prototype
    scores = {}
    for classe, proto in prototypes.items():
        # Calculer la similarité cosinus
        sim = cosine_similarity(X_test_array, proto)[0][0]
        scores[classe] = sim
    
    # Logique spéciale pour les formules de politesse
    if scores.get('social', 0) > 0.2:
        return 'neutre'
    # Logique spéciale pour les plaintes déguisées
    elif scores.get('plainte', 0) > 0.2:
        return 'negatif'
    else:
        # Choisir la meilleure classe parmi negatif, positif, neutre
        classes_finales = ['negatif', 'positif', 'neutre']
        meilleure_classe = max(classes_finales, key=lambda c: scores.get(c, 0))
        return meilleure_classe

# ============================================================
# TEST
# ============================================================

print("\n" + "="*60)
print("🧪 TEST DU CLASSIFICATEUR (version corrigée)")
print("="*60)

commentaires_test = [
    "service vraiment 5ayeb",
    "service mezyana barcha",
    "mon numéro est 0555123456",
    "ربي يوفقكم",
    "repondiw prv internet coupé",
]

for com in commentaires_test:
    resultat = predire_sentiment(com)
    print(f"\n📝 Commentaire : {com}")
    print(f"   → Sentiment : {resultat}")

# ============================================================
# TEST SUR DES COMMENTAIRES RÉELS
# ============================================================

print("\n" + "="*60)
print("📊 TEST SUR DES COMMENTAIRES SUPPLÉMENTAIRES")
print("="*60)

commentaires_reels = [
    "service 5ayeb réseau machi kayen",
    "merci beaucoup pour votre aide",
    "جيد جدا",
    "السلام عليكم عندي مشكل",
]

for com in commentaires_reels:
    resultat = predire_sentiment(com)
    print(f"\n📝 {com}")
    print(f"   → {resultat}")