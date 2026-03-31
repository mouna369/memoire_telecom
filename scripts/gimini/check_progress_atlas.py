# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# check_progress_atlas.py – Voir la progression sur Atlas
# """

# from pymongo import MongoClient
# import config

# try:
#     client = MongoClient(config.MONGO_URI)
#     db = client[config.DB_NAME]
#     coll = db[config.INPUT_COLL]
    
#     total = coll.count_documents({config.TEXT_COL: {"$exists": True, "$ne": ""}})
#     labeled = coll.count_documents({config.FLAG_COL: True})
#     remaining = total - labeled
    
#     print("─" * 60)
#     print("  📊 PROGRESSION D'ANNOTATION (MongoDB Atlas)")
#     print("─" * 60)
#     print(f"  Base de données : {config.DB_NAME}")
#     print(f"  Collection      : {config.INPUT_COLL}")
#     print(f"  Total commentaires    : {total}")
#     print(f"  ✅ Annotés            : {labeled} ({labeled/total*100:.1f}%)")
#     print(f"  ⏳ Restants           : {remaining} ({remaining/total*100:.1f}%)")
#     print("─" * 60)
    
#     if total > 0:
#         bar_len = 50
#         filled = int(bar_len * labeled / total)
#         bar = "█" * filled + "░" * (bar_len - filled)
#         print(f"  [{bar}] {labeled/total*100:.1f}%")
    
#     client.close()
    
# except Exception as e:
#     print(f"❌ Erreur de connexion : {e}")
#     print("→ Vérifie config.py")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_progress_atlas.py – Voir la progression sur Atlas
"""

from pymongo import MongoClient
import config

try:
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command('ping')
    db   = client[config.DB_NAME]
    coll = db[config.INPUT_COLL]
    output_coll = db[config.OUTPUT_COLL]

    total     = coll.count_documents({config.TEXT_COL: {"$exists": True, "$ne": ""}})
    labeled   = coll.count_documents({config.FLAG_COL: True})
    pending   = coll.count_documents({config.FLAG_COL: "pending"})
    remaining = coll.count_documents({
        config.TEXT_COL: {"$exists": True, "$ne": ""},
        config.FLAG_COL: {"$nin": [True, "pending"]}
    })

    print("─" * 60)
    print("  📊 PROGRESSION D'ANNOTATION (MongoDB Atlas)")
    print("─" * 60)
    print(f"  Base de données : {config.DB_NAME}")
    print(f"  Collection      : {config.INPUT_COLL}")
    print(f"  Total commentaires    : {total}")
    print(f"  ✅ Annotés            : {labeled} ({labeled/total*100:.1f}%)")
    print(f"  🕐 En cours (pending) : {pending} ({pending/total*100:.1f}%)")
    print(f"  ⏳ Restants           : {remaining} ({remaining/total*100:.1f}%)")
    print("─" * 60)

    if total > 0:
        bar_len = 50
        filled  = int(bar_len * labeled / total)
        bar     = "█" * filled + "░" * (bar_len - filled)
        print(f"  [{bar}] {labeled/total*100:.1f}%")

    # Stats OUTPUT
    output_total = output_coll.count_documents({})
    if output_total > 0:
        print(f"\n  📦 OUTPUT collection  : {output_total} documents annotés")
        print(f"  🎯 Objectif           : 6000")
        progress = (output_total / 6000) * 100
        filled2  = min(50, int(50 * output_total / 6000))
        bar2     = "█" * filled2 + "░" * (50 - filled2)
        print(f"  [{bar2}] {progress:.1f}% vers 6000")

        print(f"\n  📊 Distribution des labels :")
        pipeline = [{"$group": {"_id": "$label", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
        for lbl in output_coll.aggregate(pipeline):
            icon = {"positif": "✅", "neutre": "😐", "negatif": "❌"}.get(str(lbl["_id"]), "📄")
            pct  = lbl["count"] / output_total * 100
            print(f"     {icon} {str(lbl['_id']):10s} : {lbl['count']:5d} ({pct:.1f}%)")

    client.close()

except Exception as e:
    print(f"❌ Erreur de connexion : {e}")
    print("→ Vérifie config.py")