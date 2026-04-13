from collections import Counter
import re

def extraire_mots_cles(limit=10):
    """Extrait les mots-clés les plus fréquents"""
    col = get_collection()
    
    mots_cles = Counter()
    mots_a_ignorer = {"le", "la", "les", "de", "des", "et", "est", "pas", "très", "trop"}
    
    for doc in col.find({"label_final": "negatif"}):
        texte = doc.get("Commentaire_Client_Original", "").lower()
        mots = re.findall(r'\b[a-zàâçéèêëîïôûùüÿ]{4,}\b', texte)
        for mot in mots:
            if mot not in mots_a_ignorer:
                mots_cles[mot] += 1
    
    return mots_cles.most_common(limit)

def generer_nuage_mots():
    """Génère un nuage de mots (nécessite wordcloud)"""
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt
    from io import BytesIO
    import base64
    
    mots = extraire_mots_cles(50)
    if not mots:
        return None
    
    texte = " ".join([mot for mot, count in mots for _ in range(count)])
    
    wordcloud = WordCloud(width=800, height=400, background_color='black', colormap='viridis').generate(texte)
    
    img = BytesIO()
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.tight_layout(pad=0)
    plt.savefig(img, format='png')
    plt.close()
    
    img.seek(0)
    b64 = base64.b64encode(img.getvalue()).decode()
    return f'<img src="data:image/png;base64,{b64}" style="width:100%; border-radius:12px;">'