# pages/4_Statistiques.py
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
from scipy import stats

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
client = MongoClient(MONGO_URI)
db = client["telecom_algerie"]
collection = db["dataset_unifie_sans_doublons"]

st.set_page_config(page_title="Statistiques", layout="wide")
st.title("📊 Statistiques avancées")

# Chargement
data = list(collection.find().limit(23000))
df = pd.DataFrame(data) if data else pd.DataFrame()

if df.empty:
    st.warning("Aucune donnée trouvée")
    st.stop()

st.subheader("📈 Tests statistiques")

# Test du khi-deux
if 'sources' in df.columns and 'label_final' in df.columns:
    st.write("### 🔬 Test du khi-deux (χ²)")
    st.write("**Relation entre la source et le sentiment**")
    
    contingency = pd.crosstab(df['sources'], df['label_final'])
    st.dataframe(contingency, use_container_width=True)
    
    chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
    
    col1, col2 = st.columns(2)
    col1.metric("Khi-deux (χ²)", f"{chi2:.2f}")
    col2.metric("p-value", f"{p_value:.4f}")
    
    if p_value < 0.05:
        st.success("✅ Résultat SIGNIFICATIF : Il y a une relation entre la source et le sentiment")
    else:
        st.info("❌ Résultat NON SIGNIFICATIF : Pas de relation entre la source et le sentiment")

# Statistiques descriptives
st.subheader("📊 Statistiques descriptives")

col1, col2 = st.columns(2)

with col1:
    st.write("**Sentiment positif**")
    pos_df = df[df['label_final'] == 'positif']
    st.write(f"- Nombre: {len(pos_df)}")
    if 'confidence' in pos_df.columns:
        st.write(f"- Confiance moyenne: {pos_df['confidence'].mean():.2f}")
        st.write(f"- Confiance médiane: {pos_df['confidence'].median():.2f}")

with col2:
    st.write("**Sentiment négatif**")
    neg_df = df[df['label_final'] == 'negatif']
    st.write(f"- Nombre: {len(neg_df)}")
    if 'confidence' in neg_df.columns:
        st.write(f"- Confiance moyenne: {neg_df['confidence'].mean():.2f}")
        st.write(f"- Confiance médiane: {neg_df['confidence'].median():.2f}")

# Test de Student
if 'confidence' in df.columns:
    st.write("### 📊 Test de Student (t-test)")
    st.write("**Comparaison des scores de confiance entre positif et négatif**")
    
    pos_conf = df[df['label_final'] == 'positif']['confidence'].dropna()
    neg_conf = df[df['label_final'] == 'negatif']['confidence'].dropna()
    
    if len(pos_conf) > 0 and len(neg_conf) > 0:
        t_stat, p_value = stats.ttest_ind(pos_conf, neg_conf)
        
        col1, col2 = st.columns(2)
        col1.metric("t-statistic", f"{t_stat:.2f}")
        col2.metric("p-value", f"{p_value:.4f}")
        
        if p_value < 0.05:
            st.success("✅ Différence significative entre les scores de confiance")
        else:
            st.info("❌ Pas de différence significative")