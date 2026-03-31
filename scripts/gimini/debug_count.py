#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
debug_count.py – Comprendre la différence de compte
"""

from pymongo import MongoClient
import config

print("─" * 70)
print("  🔍 DIAGNOSTIC DU COMPTE DE DOCUMENTS")
print("─" * 70)

client = MongoClient(config.MONGO_URI)
db = client[config.DB_NAME]
coll = db[config.INPUT_COLL]

# 1. Total absolu
total_absolu = coll.count_documents({})
print(f"\n  📊 Total absolu (tous documents) : {total_absolu}")

# 2. Avec le champ normalized_arabert (existe)
with_field = coll.count_documents({
    config.TEXT_COL: {"$exists": True}
})
print(f"  📝 Avec champ '{config.TEXT_COL}' (existe) : {with_field}")

# 3. Avec le champ normalized_arabert (existe ET non vide)
with_field_not_empty = coll.count_documents({
    config.TEXT_COL: {"$exists": True, "$ne": ""}
})
print(f"  ✅ Avec champ '{config.TEXT_COL}' (non vide) : {with_field_not_empty}")

# 4. Sans le champ normalized_arabert
without_field = coll.count_documents({
    config.TEXT_COL: {"$exists": False}
})
print(f"  ❌ Sans champ '{config.TEXT_COL}' : {without_field}")

# 5. Avec le champ mais vide
with_field_empty = coll.count_documents({
    config.TEXT_COL: {"$exists": True, "$eq": ""}
})
print(f"  ⚠️  Avec champ '{config.TEXT_COL}' mais vide : {with_field_empty}")

# 6. Montrer un exemple de document sans le champ
print(f"\n  📄 Exemple de document SANS '{config.TEXT_COL}' :")
sample_no_field = coll.find_one({config.TEXT_COL: {"$exists": False}})
if sample_no_field:
    print(f"     Keys: {list(sample_no_field.keys())}")
else:
    print(f"     Aucun document trouvé sans ce champ")

# 7. Montrer un exemple de document avec champ vide
print(f"\n  📄 Exemple de document AVEC '{config.TEXT_COL}' vide :")
sample_empty = coll.find_one({config.TEXT_COL: {"$exists": True, "$eq": ""}})
if sample_empty:
    print(f"     Document trouvé avec champ vide")
else:
    print(f"     Aucun document avec champ vide")

client.close()

print("\n" + "─" * 70)
print("  📋 CONCLUSION")
print("─" * 70)
print(f"  • Atlas montre TOUS les documents         : {total_absolu}")
print(f"  • Ton script montre les utilisables       : {with_field_not_empty}")
print(f"  • Différence (non annotables)             : {total_absolu - with_field_not_empty}")
print("─" * 70)
print("\n  ✅ C'est NORMAL ! Tu travailles sur les documents valides.")