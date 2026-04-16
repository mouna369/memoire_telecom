# config.py — Configuration partagée pour tous les fichiers du projet
import os
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

# ── URI Atlas (format long — pas de SRV, compatible DNS Docker) ──────
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://yousrahadjabderrahmane_db_user:C8wjIvWqOBUjK66u@"
    "ac-1ksfahb-shard-00-00.gejzu4a.mongodb.net:27017,"
    "ac-1ksfahb-shard-00-01.gejzu4a.mongodb.net:27017,"
    "ac-1ksfahb-shard-00-02.gejzu4a.mongodb.net:27017/"
    "?ssl=true&replicaSet=atlas-mdnqx7-shard-0&authSource=admin"
    "&retryWrites=true&w=majority&appName=Cluster0"
)

# ── Noms unifiés ─────────────────────────────────────────────────────
DB_NAME         = "telecom_algerie_new"
COLLECTION_NAME = "dataset_unifie"

# ── Ollama ───────────────────────────────────────────────────────────
OLLAMA_HOST  = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL = "qwen2.5:7b"

# ── Connexion ────────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=15000,
            connectTimeoutMS=15000,
            socketTimeoutMS=15000,
            tls=True,
            tlsAllowInvalidCertificates=True,
            directConnection=False,
        )
        client.admin.command("ping")
        return client
    except ServerSelectionTimeoutError as e:
        st.error(
            "❌ **Atlas injoignable** — vérifie :\n"
            "1. Ton IP est autorisée sur Atlas (Network Access → `0.0.0.0/0`)\n"
            "2. Tu as accès à Internet depuis le conteneur\n\n"
            f"Détail : `{e}`"
        )
        st.stop()
    except Exception as e:
        st.error(f"❌ Erreur MongoDB : {e}")
        st.stop()

def get_collection():
    client = get_client()
    return client[DB_NAME][COLLECTION_NAME]