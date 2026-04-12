#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py — API FastAPI pour le ChatBot Algérien (DziriBERT)
Endpoints :
  POST /chat       → envoyer un message, recevoir une réponse
  GET  /stats      → statistiques globales
  GET  /history/{session_id} → historique d'une session
  GET  /health     → santé de l'API
"""

import json
import random
from pathlib import Path
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient

import sys
sys.path.append(str(Path(__file__).parent.parent))
from model.predict import PredicteurIntention
from api.schemas import ChatRequest, ChatResponse, StatsResponse

# ─────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="ChatBot Algérien — DziriBERT",
    description="Chatbot bilingue français/darija algérienne basé sur DziriBERT",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# INITIALISATION
# ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent

# MongoDB
client_mongo = MongoClient("mongodb://localhost:27017/")
db = client_mongo["chatbot_algerie"]

# Modèle
predictor = PredicteurIntention()

# Réponses par intention
with open(BASE_DIR / "data" / "intentions.json", encoding="utf-8") as f:
    _data = json.load(f)
INTENTS_RESPONSES = {i["tag"]: i["responses"] for i in _data["intentions"]}


# ─────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "model": "DziriBERT", "version": "1.0.0"}


@app.post("/chat", response_model=ChatResponse)
def chat(payload: ChatRequest):
    """Endpoint principal : reçoit un message et retourne une réponse."""
    result = predictor.predire(payload.message)

    intention = result["intention"]
    confiance = result["confiance"]

    # Sélection de la réponse
    reponses_possibles = INTENTS_RESPONSES.get(intention, INTENTS_RESPONSES["incompris"])
    reponse = random.choice(reponses_possibles)

    # Log MongoDB
    log = {
        "session_id":       payload.session_id,
        "user_name":        payload.user_name,
        "message":          payload.message,
        "texte_normalise":  result["texte_normalise"],
        "langue_detectee":  result["langue_detectee"],
        "intention":        intention,
        "confiance":        confiance,
        "top3":             result["top3"],
        "reponse":          reponse,
        "timestamp":        datetime.now(),
    }
    db.conversations.insert_one(log)

    return ChatResponse(
        reponse=reponse,
        intention=intention,
        confiance=confiance,
        langue_detectee=result["langue_detectee"],
        texte_normalise=result["texte_normalise"],
        top3=result["top3"],
        session_id=payload.session_id,
    )


@app.get("/stats")
def stats():
    """Statistiques globales du chatbot."""
    total = db.conversations.count_documents({})
    if total == 0:
        return {"total_messages": 0, "message": "Aucune donnée"}

    sessions = db.conversations.distinct("session_id")
    incompris = db.conversations.count_documents({"intention": "incompris"})

    # Intentions
    pipeline_intent = [
        {"$group": {"_id": "$intention", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    intentions = list(db.conversations.aggregate(pipeline_intent))

    # Confiance moyenne
    pipeline_conf = [{"$group": {"_id": None, "moy": {"$avg": "$confiance"}}}]
    conf_result = list(db.conversations.aggregate(pipeline_conf))
    conf_moy = round(conf_result[0]["moy"], 4) if conf_result else 0.0

    # Messages par heure
    pipeline_heure = [
        {"$group": {
            "_id": {"$hour": "$timestamp"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}},
    ]
    par_heure = [{"heure": r["_id"], "count": r["count"]}
                 for r in db.conversations.aggregate(pipeline_heure)]

    # Distribution langues
    pipeline_lang = [
        {"$group": {"_id": "$langue_detectee", "count": {"$sum": 1}}}
    ]
    langues = list(db.conversations.aggregate(pipeline_lang))

    return {
        "total_messages":    total,
        "sessions_uniques":  len(sessions),
        "confiance_moyenne": conf_moy,
        "taux_incompris":    round(incompris / total, 4) if total else 0,
        "intentions":        intentions,
        "langues":           langues,
        "messages_par_heure": par_heure,
    }


@app.get("/history/{session_id}")
def history(session_id: str, limit: int = 20):
    """Historique des conversations d'une session."""
    docs = list(
        db.conversations.find(
            {"session_id": session_id},
            {"_id": 0, "message": 1, "reponse": 1, "intention": 1,
             "confiance": 1, "langue_detectee": 1, "timestamp": 1}
        ).sort("timestamp", -1).limit(limit)
    )
    return {"session_id": session_id, "messages": docs[::-1]}


@app.delete("/history/{session_id}")
def delete_history(session_id: str):
    """Supprime l'historique d'une session."""
    result = db.conversations.delete_many({"session_id": session_id})
    return {"deleted": result.deleted_count}


# ─────────────────────────────────────────────────────────────
# LANCEMENT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
