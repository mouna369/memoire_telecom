# pages/4_Chatbot.py
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import os
import unicodedata
import re

# ============================================
# CONNEXION MONGODB (locale ou distante)
# ============================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018/")
DB_NAME = "telecom_algerie"
COLLECTION_NAME = "dataset_unifie"  # à adapter si besoin

@st.cache_resource
def init_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception as e:
        st.error(f"❌ Erreur de connexion MongoDB: {e}")
        return None

client = init_connection()
if client is None:
    st.stop()

db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ============================================
# FONCTIONS D'ANALYSE (identiques à avant)
# ============================================
def parse_date(date_str):
    if not date_str or not isinstance(date_str, str):
        return None
    premiere = date_str.split(',')[0].strip()
    for fmt in ['%d/%m/%Y %H:%M', '%d/%m/%Y']:
        try:
            return datetime.strptime(premiere, fmt)
        except:
            continue
    return None

def get_stats_globales():
    total = collection.count_documents({})
    neg = collection.count_documents({"label_final": "negatif"})
    pos = collection.count_documents({"label_final": "positif"})
    neu = collection.count_documents({"label_final": "neutre"})
    return {
        'total': total,
        'negatif': round(neg/total*100, 1) if total else 0,
        'positif': round(pos/total*100, 1) if total else 0,
        'neutre': round(neu/total*100, 1) if total else 0
    }

def get_top_mots_negatifs(limit=10):
    mots_negatifs = [
        "5ayeb", "خايب", "problème", "مشكل", "nul", "connexion", "réseau",
        "service", "lent", "بطيء", "mauvais", "cher", "غالي", "facture",
        "fibre", "فيبر", "installation", "تركيب", "retard", "تأخير",
        "client", "support", "réponse", "رد", "coupé", "مقطوع"
    ]
    negatifs = list(collection.find({"label_final": "negatif"}))
    compteur = {}
    for doc in negatifs:
        texte = doc.get('Commentaire_Client_Original', '').lower()
        for mot in mots_negatifs:
            if mot.lower() in texte:
                compteur[mot] = compteur.get(mot, 0) + 1
    top = sorted(compteur.items(), key=lambda x: x[1], reverse=True)[:limit]
    return top

def get_stats_par_mois():
    tous = list(collection.find({}, {"dates": 1, "label_final": 1}))
    mois_stats = {}
    for doc in tous:
        date_str = doc.get('dates')
        dt = parse_date(date_str)
        if dt is None:
            continue
        mois_key = dt.strftime("%Y-%m")
        label = doc.get('label_final')
        if mois_key not in mois_stats:
            mois_stats[mois_key] = {'total': 0, 'negatif': 0, 'positif': 0, 'neutre': 0}
        mois_stats[mois_key]['total'] += 1
        if label == 'negatif':
            mois_stats[mois_key]['negatif'] += 1
        elif label == 'positif':
            mois_stats[mois_key]['positif'] += 1
        elif label == 'neutre':
            mois_stats[mois_key]['neutre'] += 1
    resultats = []
    for mois, stats in sorted(mois_stats.items()):
        total = stats['total']
        if total == 0:
            continue
        resultats.append({
            'mois': mois,
            'nom': datetime.strptime(mois, "%Y-%m").strftime("%B %Y"),
            'total': total,
            'negatif': round(stats['negatif']/total*100, 1),
            'positif': round(stats['positif']/total*100, 1),
            'neutre': round(stats['neutre']/total*100, 1)
        })
    return resultats

def get_tendance():
    mois_stats = get_stats_par_mois()
    if len(mois_stats) >= 2:
        dernier = mois_stats[-1]
        avant = mois_stats[-2]
        evolution = dernier['negatif'] - avant['negatif']
        return {
            'evolution': 'augmentation' if evolution > 0 else 'diminution',
            'hausse': abs(evolution),
            'dernier_mois': dernier['negatif'],
            'mois_precedent': avant['negatif'],
            'nom_dernier': dernier['nom'],
            'nom_precedent': avant['nom']
        }
    return None

def get_pic_negativite():
    tous = list(collection.find({}, {"dates": 1, "label_final": 1}))
    semaines = {}
    for doc in tous:
        date_str = doc.get('dates')
        dt = parse_date(date_str)
        if dt is None:
            continue
        annee_semaine = dt.strftime("%Y-%W")
        label = doc.get('label_final')
        if annee_semaine not in semaines:
            semaines[annee_semaine] = {'total': 0, 'negatif': 0, 'semaine_start': dt - timedelta(days=dt.weekday())}
        semaines[annee_semaine]['total'] += 1
        if label == 'negatif':
            semaines[annee_semaine]['negatif'] += 1
    meilleur = None
    meilleur_pct = -1
    for semaine, data in semaines.items():
        if data['total'] < 10:
            continue
        pct = data['negatif'] / data['total'] * 100
        if pct > meilleur_pct:
            meilleur_pct = pct
            meilleur = (semaine, data['semaine_start'], data['negatif'], data['total'])
    if meilleur:
        return {
            'semaine': meilleur[1],
            'negatifs': meilleur[2],
            'total': meilleur[3],
            'pct': round(meilleur_pct, 1)
        }
    return None

@st.cache_data(ttl=3600)
def load_chatbot_data():
    stats = get_stats_globales()
    top_mots = get_top_mots_negatifs(10)
    mois_stats = get_stats_par_mois()
    tendance = get_tendance()
    pic = get_pic_negativite()
    return stats, top_mots, mois_stats, tendance, pic

stats, top_mots, mois_stats, tendance, pic = load_chatbot_data()

# ============================================
# DÉTECTION DE LANGUE ET RÉPONSES MULTILINGUES
# ============================================
def detect_langue(texte):
    if re.search('[\u0600-\u06FF]', texte):
        return "ar"
    return "fr"

# Dictionnaire des réponses
reponses = {
    "fr": {
        "stats": lambda s: f"{s['negatif']}% négatifs, {s['positif']}% positifs, {s['neutre']}% neutres. Total {s['total']} commentaires.",
        "causes": lambda m: "🔍 Principales causes : " + ", ".join([f"'{mot}' ({count})" for mot,count in m[:5]]),
        "mots_frequents": lambda m: "📊 Top mots dans les commentaires négatifs :\n" + "\n".join([f"- {mot} : {count}" for mot,count in m[:10]]),
        "tendance": lambda t: f"📈 {t['nom_dernier']} : {t['dernier_mois']}% négatifs, {t['nom_precedent']} : {t['mois_precedent']}% négatifs. {t['evolution'].capitalize()} de {abs(t['hausse'])} points." if t else "Pas assez de données.",
        "pire_mois": lambda p: f"📉 Le mois le plus négatif est **{p['nom']}** avec {p['negatif']}% de négatifs (sur {p['total']} commentaires)." if p else "Données insuffisantes.",
        "pic": lambda p: f"⚠️ Pic de négativité la semaine du {p['semaine'].strftime('%d/%m/%Y')} avec {p['pct']}% de négatifs ({p['negatifs']} sur {p['total']} commentaires)." if p else "Aucun pic détecté.",
        "resume": lambda s,t,p,m: f"{s['total']} commentaires. {s['negatif']}% négatifs, {s['positif']}% positifs. Principaux sujets : {', '.join([x for x,_ in m[:3]])}." + (f" Tendance : {t['evolution']} de {abs(t['hausse'])} points." if t else "") + (f" Pic : semaine du {p['semaine'].strftime('%d/%m/%Y')} ({p['pct']}% négatifs)." if p else ""),
        "defaut": "❓ Je n'ai pas compris. Questions possibles :\n- Statistiques globales\n- Pourquoi les commentaires sont négatifs ?\n- Quels sont les mots les plus fréquents ?\n- Quelle est la tendance ?\n- Quel est le mois le plus négatif ?\n- Quand y a-t-il eu un pic ?\n- Résumé"
    },
    "ar": {
        "stats": lambda s: f"{s['negatif']}% سلبي، {s['positif']}% إيجابي، {s['neutre']}% محايد. إجمالي {s['total']} تعليق.",
        "causes": lambda m: "🔍 الأسباب الأكثر شيوعًا: " + ", ".join([f"'{mot}' ({count})" for mot,count in m[:5]]),
        "mots_frequents": lambda m: "📊 الكلمات الأكثر تكرارًا في التعليقات السلبية:\n" + "\n".join([f"- {mot} : {count}" for mot,count in m[:10]]),
        "tendance": lambda t: f"📈 {t['nom_dernier']} : {t['dernier_mois']}% سلبي، {t['nom_precedent']} : {t['mois_precedent']}% سلبي. {'زيادة' if t['evolution']=='augmentation' else 'انخفاض'} بمقدار {abs(t['hausse'])} نقطة." if t else "بيانات غير كافية.",
        "pire_mois": lambda p: f"📉 أسوأ شهر هو **{p['nom']}** بنسبة {p['negatif']}% سلبي (من {p['total']} تعليق)." if p else "بيانات غير كافية.",
        "pic": lambda p: f"⚠️ ذروة السلبية: أسبوع {p['semaine'].strftime('%d/%m/%Y')} بنسبة {p['pct']}% سلبي ({p['negatifs']} من {p['total']})." if p else "لم يتم اكتشاف ذروة.",
        "resume": lambda s,t,p,m: f"{s['total']} تعليق. {s['negatif']}% سلبي، {s['positif']}% إيجابي. المواضيع الرئيسية: {', '.join([x for x,_ in m[:3]])}." + (f" الاتجاه: {'ارتفاع' if t['evolution']=='augmentation' else 'انخفاض'} {abs(t['hausse'])} نقطة." if t else "") + (f" الذروة: أسبوع {p['semaine'].strftime('%d/%m/%Y')} ({p['pct']}% سلبي)." if p else ""),
        "defaut": "❓ لم أفهم. الأسئلة الممكنة:\n- إحصائيات عامة\n- لماذا التعليقات سلبية؟\n- ما هي الكلمات الأكثر تكرارًا؟\n- ما هو الاتجاه؟\n- ما هو أكثر شهر سلبية؟\n- متى حدثت الذروة؟\n- ملخص"
    }
}

# Optionnel : appel à Gemini (si clé API fournie)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_API_KEY)
    def gemini_response(question, lang):
        try:
            model = genai.GenerativeModel('gemini-pro')
            prompt = f"Réponds en {'français' if lang=='fr' else 'arabe'} à la question suivante sur les avis des clients télécom : {question}"
            response = model.generate_content(prompt)
            return response.text
        except:
            return None
else:
    def gemini_response(question, lang):
        return None

# ============================================
# FONCTION DE TRAITEMENT DE LA QUESTION
# ============================================
def traiter_question(question, stats, top_mots, mois_stats, tendance, pic):
    q = question.lower()
    # Normalisation des accents pour le français
    q = unicodedata.normalize('NFD', q).encode('ascii', 'ignore').decode('utf-8')
    lang = detect_langue(question)
    r = reponses[lang]
    
    if "statistiques" in q or "global" in q or "indicateurs" in q:
        return r["stats"](stats)
    elif ("pourquoi" in q or "cause" in q) and ("negatif" in q or "negatifs" in q or "سلبي" in q):
        return r["causes"](top_mots)
    elif "mot" in q or "fréquent" in q or "frequents" in q:
        return r["mots_frequents"](top_mots)
    elif "tendance" in q or "evolution" in q:
        return r["tendance"](tendance)
    elif "mois le plus négatif" in q or "pire mois" in q or "أسوأ شهر" in q:
        pire = max(mois_stats, key=lambda x: x['negatif']) if mois_stats else None
        return r["pire_mois"](pire)
    elif "pic" in q or "ذروة" in q:
        return r["pic"](pic)
    elif "resume" in q or "résumé" in q or "rapport" in q or "ملخص" in q:
        return r["resume"](stats, tendance, pic, top_mots)
    else:
        # Essayer Gemini si disponible
        gem = gemini_response(question, lang)
        if gem:
            return gem
        return r["defaut"]

# ============================================
# INTERFACE STREAMLIT
# ============================================
st.set_page_config(page_title="Assistant Télécom", page_icon="🤖", layout="wide")

st.markdown("""
<style>
    .main { font-family: 'Inter', sans-serif; }
    .user-message { background-color: #e6f7ff; border-radius: 20px; padding: 10px; margin: 5px 0; text-align: right; }
    .bot-message { background-color: #f6f8fa; border-radius: 20px; padding: 10px; margin: 5px 0; }
</style>
""", unsafe_allow_html=True)

st.title("🤖 Assistant d'analyse des sentiments")
st.caption("Posez une question en français ou en arabe, ou déposez un fichier à analyser")

# Onglets pour séparer chat et upload
tab1, tab2 = st.tabs(["💬 Chat", "📂 Analyser un fichier"])

# ============================================
# TAB 1 : CHAT
# ============================================
with tab1:
    # Historique de la conversation
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Affichage de l'historique
    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            st.markdown(f'<div class="user-message">👤 {msg["content"]}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="bot-message">🤖 {msg["content"]}</div>', unsafe_allow_html=True)

    # Suggestions rapides
    col1, col2, col3, col4 = st.columns(4)
    suggestions = [
        "Statistiques globales",
        "Pourquoi les commentaires sont négatifs ?",
        "Quelle est la tendance ?",
        "Quel est le mois le plus négatif ?"
    ]
    for i, sugg in enumerate(suggestions):
        with [col1, col2, col3, col4][i]:
            if st.button(sugg, key=f"sugg_{i}"):
                st.session_state.chat_history.append({"role": "user", "content": sugg})
                rep = traiter_question(sugg, stats, top_mots, mois_stats, tendance, pic)
                st.session_state.chat_history.append({"role": "bot", "content": rep})
                st.rerun()

    # Zone de saisie utilisateur
    user_input = st.chat_input("Posez votre question ici...")
    if user_input:
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        rep = traiter_question(user_input, stats, top_mots, mois_stats, tendance, pic)
        st.session_state.chat_history.append({"role": "bot", "content": rep})
        st.rerun()

    # Bouton pour effacer l'historique
    if st.button("🗑️ Effacer l'historique"):
        st.session_state.chat_history = []
        st.rerun()

# ============================================
# TAB 2 : UPLOAD DE FICHIER
# ============================================
with tab2:
    st.markdown("### Analyser des commentaires depuis un fichier")
    uploaded_file = st.file_uploader("Choisissez un fichier CSV ou TXT", type=["csv", "txt"])
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                df_ext = pd.read_csv(uploaded_file)
                st.write(f"Fichier chargé : {len(df_ext)} lignes")
                # Détection automatique de la colonne de texte
                text_col = st.selectbox("Colonne contenant les commentaires", df_ext.columns)
                if st.button("Lancer l'analyse"):
                    # Compter les mots négatifs (simplifié) - vous pouvez adapter
                    commentaires = df_ext[text_col].astype(str).tolist()
                    mots_negatifs_trouves = {}
                    for mot in top_mots:
                        count = sum(1 for c in commentaires if mot in c.lower())
                        if count > 0:
                            mots_negatifs_trouves[mot] = count
                    st.success(f"Analyse terminée. {len(mots_negatifs_trouves)} mots négatifs détectés.")
                    st.json(mots_negatifs_trouves)
            else:
                content = uploaded_file.read().decode("utf-8")
                st.text_area("Aperçu du fichier texte", content[:500], height=200)
                if st.button("Analyser"):
                    lignes = content.splitlines()
                    st.info(f"{len(lignes)} lignes détectées. Pour une analyse plus fine, convertissez en CSV.")
        except Exception as e:
            st.error(f"Erreur lors de la lecture : {e}")

# ============================================
# SIDEBAR AVEC INDICATEURS CLÉS
# ============================================
with st.sidebar:
    st.header("📊 En un coup d'œil")
    st.metric("🔴 Négatifs", f"{stats['negatif']}%")
    st.metric("🟢 Positifs", f"{stats['positif']}%")
    st.metric("Total commentaires", f"{stats['total']:,}")
    st.markdown("---")
    if tendance:
        st.subheader("📈 Tendance")
        st.write(f"**{tendance['nom_dernier']}** : {tendance['dernier_mois']}%")
        st.write(f"**{tendance['nom_precedent']}** : {tendance['mois_precedent']}%")
        delta = f"+{tendance['hausse']}" if tendance['evolution'] == 'augmentation' else f"-{tendance['hausse']}"
        st.metric("Variation", delta)
    if pic:
        st.subheader("⚠️ Pic")
        st.write(f"Semaine du {pic['semaine'].strftime('%d/%m/%Y')}")
        st.write(f"{pic['pct']}% négatifs ({pic['negatifs']}/{pic['total']})")