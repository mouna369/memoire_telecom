#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
schemas.py — Modèles Pydantic pour l'API FastAPI
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=500)
    session_id: Optional[str] = "anonymous"
    user_name: Optional[str] = None


class ChatResponse(BaseModel):
    reponse: str
    intention: str
    confiance: float
    langue_detectee: str
    texte_normalise: str
    top3: list
    session_id: str


class StatsResponse(BaseModel):
    total_messages: int
    sessions_uniques: int
    confiance_moyenne: float
    taux_incompris: float
    intentions: list
    messages_par_heure: list
