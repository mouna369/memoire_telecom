#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tokenizer_no_spark.py
======================
Tokenisation SANS Spark — pure Python + pymongo.
Bien plus léger pour WSL avec 26K documents.
"""

import os
import time
import logging
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION_IN  = "dataset_unifie"
COLLECTION_OUT = "dataset_tokenizes"

READ_BATCH     = 1000   # documents lus par lot
WRITE_BATCH    = 200    # documents écrits par bulk_write
PROCESS_BATCH  = 500    # documents tokenisés en mémoire à la fois

COLUMNS = [
    "Commentaire_Client",
    "normalized_arabert",
    "normalized_full",
    "commentaire_moderateur",
]

# ============================================================================
# CONNEXION MONGO
# ============================================================================
def get_client():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10_000,
        socketTimeoutMS=60_000,
        connectTimeoutMS=10_000,
        readPreference="secondaryPreferred",
    )

# ============================================================================
# LECTURE PAR LOTS (évite de tout charger en RAM)
# ============================================================================
def iter_documents(collection, batch_size=READ_BATCH):
    """Générateur : lit les docs par lots pour économiser la RAM."""
    cursor = collection.find({}, batch_size=batch_size)
    batch  = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
    cursor.close()

# ============================================================================
# ÉCRITURE BULK
# ============================================================================
def write_batch(collection_out, docs):
    if not docs:
        return 0
    ops = [UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True) for d in docs]
    try:
        result = collection_out.bulk_write(ops, ordered=False)
        return result.upserted_count + result.modified_count
    except BulkWriteError as e:
        logger.warning(f"BulkWriteError : {e.details.get('nInserted', 0)} insérés")
        return e.details.get("nInserted", 0)

# ============================================================================
# PROCESSUS PRINCIPAL — STREAMING (pas tout en RAM)
# ============================================================================
def process():
    t0 = time.time()

    # Init tokenizer UNE SEULE FOIS
    print("🔧 Initialisation du tokenizer...")
    from arabic_tokenizer import get_tokenizer
    tok = get_tokenizer()
    print("✅ Tokenizer prêt.")

    # Connexion MongoDB
    client         = get_client()
    db             = client[DB_NAME]
    col_in         = db[COLLECTION_IN]
    col_out        = db[COLLECTION_OUT]

    total_est = col_in.estimated_document_count()
    print(f"📊 ~{total_est} documents à traiter")
    print(f"📋 Traitement par lots de {READ_BATCH} — écriture par {WRITE_BATCH}\n")

    processed = 0
    written   = 0
    write_buf = []   # buffer d'écriture

    for batch in iter_documents(col_in, batch_size=READ_BATCH):
        # Tokeniser chaque document du lot
        for doc in batch:
            for col_name in COLUMNS:
                text      = doc.get(col_name)
                token_col = f"tokens_{col_name.lower()}"
                if text:
                    tokens          = tok.tokenize(str(text))
                    lang            = tok.detect_language(str(text))
                    doc[token_col]              = tokens
                    doc[f"{token_col}_count"]   = len(tokens)
                    doc[f"{token_col}_lang"]    = lang
                else:
                    doc[token_col]              = []
                    doc[f"{token_col}_count"]   = 0
                    doc[f"{token_col}_lang"]    = "unknown"

            doc["tokenization_date"] = time.strftime("%Y-%m-%dT%H:%M:%S")
            write_buf.append(doc)

            # Écrire dès que le buffer est plein
            if len(write_buf) >= WRITE_BATCH:
                written += write_batch(col_out, write_buf)
                write_buf.clear()

        processed += len(batch)
        elapsed    = time.time() - t0
        pct        = processed / total_est * 100 if total_est else 0
        rate       = processed / elapsed if elapsed > 0 else 0
        eta        = (total_est - processed) / rate if rate > 0 else 0
        print(f"  ↳ {processed}/{total_est} ({pct:.0f}%) | "
              f"{rate:.0f} docs/s | ETA: {eta:.0f}s")

    # Vider le buffer restant
    if write_buf:
        written += write_batch(col_out, write_buf)

    client.close()

    total_time = time.time() - t0
    print(f"\n🎉 Terminé en {total_time:.1f}s")
    print(f"   Documents traités : {processed}")
    print(f"   Documents écrits  : {written}")
    print(f"   Vitesse moyenne   : {processed/total_time:.0f} docs/s")

if __name__ == "__main__":
    process()