#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# spark_reporter.py
# Copier ce fichier dans le même dossier que votre script Spark
# Puis ajouter : from spark_reporter import SparkReporter
# et appeler reporter.update(...) aux étapes clés

import json, os, time

STATS_FILE = "/tmp/spark_stats.json"

class SparkReporter:
    def __init__(self, nb_workers=3, total_docs=0):
        self.nb_workers   = nb_workers
        self.total_docs   = total_docs
        self.start_time   = time.time()
        self._write({
            "phase"       : "starting",
            "message"     : "Démarrage du pipeline...",
            "nb_workers"  : nb_workers,
            "total_docs"  : total_docs,
            "start_time"  : self.start_time,
            "elapsed"     : 0,
            "workers"     : [],
            "stats_avant" : {},
            "stats_apres" : {},
            "benchmark"   : {},
            "timeline"    : {},
            "succes"      : False,
        })

    # ── appel principal ────────────────────────────────────────
    def update(self, phase, message="", extra=None):
        data = self._read()
        data["phase"]   = phase
        data["message"] = message
        data["elapsed"] = round(time.time() - self.start_time, 2)
        if extra:
            data.update(extra)
        self._write(data)

    # ── étapes spécifiques ──────────────────────────────────────
    def spark_connected(self, elapsed_spark):
        self.update("spark_connected", "Spark connecté — lecture distribuée en cours...",
                    {"timeline": {"connexion_spark": elapsed_spark}})

    def loading_done(self, total_lignes, elapsed_load):
        data = self._read()
        tl   = data.get("timeline", {})
        tl["chargement"] = elapsed_load
        self.update("loading_done",
                    f"{total_lignes} documents chargés",
                    {"total_lignes": total_lignes, "timeline": tl})

    def analyse_done(self, stats_avant: dict, elapsed_analyse):
        data = self._read()
        tl   = data.get("timeline", {})
        tl["analyse"] = elapsed_analyse
        self.update("analyse_done", "Analyse terminée — nettoyage en cours...",
                    {"stats_avant": stats_avant, "timeline": tl})

    def worker_report(self, hostname, docs_traites, docs_vides):
        """Appelé depuis nettoyer_et_ecrire_partition via yield ou log."""
        data = self._read()
        workers = data.get("workers", [])
        # mise à jour ou ajout
        found = False
        for w in workers:
            if w["hostname"] == hostname:
                w["docs_traites"] += docs_traites
                w["docs_vides"]   += docs_vides
                found = True
                break
        if not found:
            workers.append({"hostname": hostname,
                             "docs_traites": docs_traites,
                             "docs_vides"  : docs_vides})
        data["workers"] = workers
        data["elapsed"] = round(time.time() - self.start_time, 2)
        self._write(data)

    def writing_done(self, total_inseres, total_vides, elapsed_write):
        data = self._read()
        tl   = data.get("timeline", {})
        tl["ecriture_mongodb"] = elapsed_write
        self.update("writing_done",
                    f"{total_inseres} documents écrits en {elapsed_write:.2f}s",
                    {"total_inseres": total_inseres,
                     "total_vides"  : total_vides,
                     "timeline"     : tl})

    def benchmark_done(self, temps_1, count_1, temps_3, count_3):
        speedup = round(temps_1 / temps_3, 2) if temps_3 > 0 else 0
        self.update("benchmark_done", f"Speedup mesuré : {speedup}×",
                    {"benchmark": {
                        "temps_1_worker" : round(temps_1, 2),
                        "docs_1_worker"  : count_1,
                        "temps_3_workers": round(temps_3, 2),
                        "docs_3_workers" : count_3,
                        "speedup"        : speedup,
                    }})

    def final(self, stats_apres, total_docs, total_inseres, total_vides,
              vitesse, succes, timeline):
        self.update("done", "✅ Pipeline terminé avec succès !",
                    {"stats_apres"  : stats_apres,
                     "total_inseres": total_inseres,
                     "total_vides"  : total_vides,
                     "vitesse"      : vitesse,
                     "succes"       : succes,
                     "timeline"     : timeline})

    # ── helpers internes ────────────────────────────────────────
    def _write(self, data):
        try:
            with open(STATS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[reporter] erreur écriture : {e}")

    def _read(self):
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
