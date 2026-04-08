#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# dashboard_server.py
# Lancer AVANT votre script Spark : python3 dashboard_server.py
# Dashboard accessible sur : http://localhost:5055

from flask import Flask, jsonify, send_from_directory
import json, os, time

app = Flask(__name__)

STATS_FILE = "/tmp/spark_stats.json"   # fichier partagé avec le script Spark

def read_stats():
    if not os.path.exists(STATS_FILE):
        return {"phase": "waiting", "message": "En attente du démarrage Spark..."}
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"phase": "waiting", "message": "Lecture en cours..."}

@app.route("/")
def index():
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "dashboard_realtime.html")

@app.route("/api/stats")
def stats():
    return jsonify(read_stats())

if __name__ == "__main__":
    print("=" * 55)
    print("  Dashboard Spark temps réel")
    print("  http://localhost:5055")
    print("  Lancez ensuite votre script Spark")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5055, debug=False)
