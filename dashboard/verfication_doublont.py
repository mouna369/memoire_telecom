# diagnostic.py
import streamlit as st
import os

st.set_page_config(page_title="Diagnostic", layout="wide")

st.title("🔍 Diagnostic - Recherche du doublon")

# 1. Vérifier tous les fichiers Python
st.subheader("📁 Fichiers trouvés")

pages_dir = "pages"
if os.path.exists(pages_dir):
    files = os.listdir(pages_dir)
    st.write(f"Fichiers dans `pages/`: {files}")
    
    # Lire chaque fichier
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(pages_dir, file)
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Chercher des mots-clés suspects
            if 'st.sidebar' in content:
                st.warning(f"⚠️ {file} contient 'st.sidebar' - C'est probablement la cause !")
            if 'st.radio' in content and 'sidebar' in content:
                st.error(f"❌ {file} contient un menu de navigation !")
            if 'page' in content and ('radio' in content or 'selectbox' in content):
                st.info(f"📌 {file} pourrait avoir une navigation")

# 2. Vérifier app.py
st.subheader("📄 Vérification de app.py")

if os.path.exists('app.py'):
    with open('app.py', 'r', encoding='utf-8') as f:
        app_content = f.read()
    
    if 'st.sidebar' in app_content:
        st.success("✅ app.py a une sidebar (normal)")
    if 'st.radio' in app_content:
        st.success("✅ app.py a une navigation radio")

