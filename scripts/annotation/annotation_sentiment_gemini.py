# # # # #!/usr/bin/env python3
# # # # # -*- coding: utf-8 -*-
# # # # # scripts/nlp/comparer_4_modeles.py

# # # # from transformers import AutoTokenizer, AutoModelForSequenceClassification
# # # # import torch
# # # # torch.set_num_threads(2)
# # # # from pymongo import MongoClient
# # # # import time
# # # # import csv
# # # # import json
# # # # from datetime import datetime

# # # # # ============================================================
# # # # # CONFIGURATION
# # # # # ============================================================
# # # # MONGO_URI = "mongodb://localhost:27018/"
# # # # DB_NAME = "telecom_algerie"
# # # # COL_SOURCE = "commentaires_normalises"
# # # # NOMBRE_TESTS = 50  # Nombre de commentaires à tester

# # # # # ✅ Les 4 modèles (avec gestion d'échec)
# # # # MODELES = {
# # # #     "DziriBERT": "alger-ia/dziribert_sentiment",           # 🥇 Darja
# # # #     "AraBERT": "aubmindlab/bert-base-arabertv02",          # 🥈 MSA
# # # #     "CAMeLBERT": "CAMeL-Lab/camelbert-mix",                # 🥉 Mixte
# # # #     "DistilBERT_FR": "distilbert-base-uncased-finetuned-sst-2-english",  # 🇬🇧 Français
# # # # }

# # # # # Labels pour chaque modèle
# # # # LABELS_MODELES = {
# # # #     "DziriBERT": {0: "Négatif", 1: "Neutre", 2: "Positif"},
# # # #     "AraBERT": {0: "LABEL_0", 1: "LABEL_1"},
# # # #     "CAMeLBERT": {0: "LABEL_0", 1: "LABEL_1"},
# # # #     "DistilBERT_FR": {0: "Négatif", 1: "Positif"},
# # # # }

# # # # # Export fichiers
# # # # TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
# # # # FICHIER_CSV = f"resultats_comparison_4modeles_{TIMESTAMP}.csv"
# # # # FICHIER_JSON = f"resultats_comparison_4modeles_{TIMESTAMP}.json"
# # # # RAPPORT_TXT = f"rapport_comparison_4modeles_{TIMESTAMP}.txt"

# # # # # ============================================================
# # # # # CHARGEMENT DES MODÈLES (avec gestion d'erreur)
# # # # # ============================================================
# # # # def charger_modeles():
# # # #     modeles_charges = {}
# # # #     for nom, path in MODELES.items():
# # # #         try:
# # # #             print(f"🤖 Chargement de {nom}...")
# # # #             tokenizer = AutoTokenizer.from_pretrained(path)
# # # #             model = AutoModelForSequenceClassification.from_pretrained(path)
# # # #             model.eval()
            
# # # #             labels = LABELS_MODELES.get(nom, {})
# # # #             print(f"   🏷️ Labels : {labels}")
# # # #             print(f"   ✅ {nom} prêt\n")
            
# # # #             modeles_charges[nom] = {
# # # #                 "tokenizer": tokenizer,
# # # #                 "model": model,
# # # #                 "labels": labels
# # # #             }
# # # #         except Exception as e:
# # # #             print(f"   ❌ Échec {nom} : {str(e)[:100]}")
# # # #             print(f"   → On continue avec les autres modèles\n")
# # # #     return modeles_charges

# # # # # ============================================================
# # # # # PRÉDICTION
# # # # # ============================================================
# # # # def predire(modeles_charges, texte):
# # # #     resultats = {}
# # # #     for nom, components in modeles_charges.items():
# # # #         tokenizer = components["tokenizer"]
# # # #         model = components["model"]
# # # #         labels = components["labels"]
        
# # # #         inputs = tokenizer(texte, return_tensors="pt", truncation=True, max_length=128)
# # # #         with torch.no_grad():
# # # #             outputs = model(**inputs)
# # # #             probs = torch.softmax(outputs.logits, dim=-1)[0]
# # # #             classe = torch.argmax(probs).item()
# # # #             confiance = probs[classe].item()
# # # #             label_name = labels.get(classe, f"Class_{classe}")
        
# # # #         resultats[nom] = {
# # # #             "classe": classe,
# # # #             "label": label_name,
# # # #             "confiance": round(confiance, 4),
# # # #             "scores": {f"score_{i}": round(probs[i].item(), 4) for i in range(len(probs))}
# # # #         }
# # # #     return resultats

# # # # # ============================================================
# # # # # EXPORT CSV
# # # # # ============================================================
# # # # def initialiser_csv(nom_fichier, modeles):
# # # #     with open(nom_fichier, "w", newline="", encoding="utf-8-sig") as f:
# # # #         writer = csv.writer(f)
# # # #         header = ["id", "texte", "longueur"]
# # # #         for nom in modeles:
# # # #             header.extend([f"{nom}_sentiment", f"{nom}_confiance"])
# # # #         writer.writerow(header)

# # # # def sauvegarder_csv(nom_fichier, modeles, doc, resultats, idx):
# # # #     with open(nom_fichier, "a", newline="", encoding="utf-8-sig") as f:
# # # #         writer = csv.writer(f)
# # # #         row = [idx, doc.get("Commentaire_Client", "")[:300], len(doc.get("Commentaire_Client", ""))]
# # # #         for nom in modeles:
# # # #             if nom in resultats:
# # # #                 row.extend([resultats[nom]["label"], resultats[nom]["confiance"]])
# # # #             else:
# # # #                 row.extend(["N/A", "N/A"])
# # # #         writer.writerow(row)

# # # # # ============================================================
# # # # # EXPORT JSON
# # # # # ============================================================
# # # # def sauvegarder_json(nom_fichier, resultats_complets):
# # # #     with open(nom_fichier, "w", encoding="utf-8") as f:
# # # #         json.dump(resultats_complets, f, ensure_ascii=False, indent=2)

# # # # # ============================================================
# # # # # RAPPORT TEXTE
# # # # # ============================================================
# # # # def generer_rapport(nom_fichier, resultats_complets, modeles_charges):
# # # #     with open(nom_fichier, "w", encoding="utf-8") as f:
# # # #         f.write("=" * 80 + "\n")
# # # #         f.write("🔬 COMPARAISON DE 4 MODÈLES - ANALYSE DE SENTIMENT\n")
# # # #         f.write(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
# # # #         f.write("=" * 80 + "\n\n")
        
# # # #         f.write("📋 MODÈLES TESTÉS :\n")
# # # #         for nom in MODELES.keys():
# # # #             statut = "✅" if nom in modeles_charges else "❌"
# # # #             f.write(f"   {statut} {nom:20} : {MODELES.get(nom, 'N/A')}\n")
# # # #         f.write("\n")
        
# # # #         f.write("📊 STATISTIQUES PAR MODÈLE :\n")
# # # #         f.write("-" * 80 + "\n")
        
# # # #         for nom in modeles_charges.keys():
# # # #             confiances = [r["predictions"].get(nom, {}).get("confiance", 0) 
# # # #                          for r in resultats_complets if nom in r["predictions"]]
            
# # # #             if confiances:
# # # #                 avg_conf = sum(confiances) / len(confiances)
# # # #                 f.write(f"\n🤖 {nom}:\n")
# # # #                 f.write(f"   • Confiance moyenne : {avg_conf:.2%}\n")
# # # #                 f.write(f"   • Min : {min(confiances):.2%} | Max : {max(confiances):.2%}\n")
                
# # # #                 # Compter les labels
# # # #                 labels_count = {}
# # # #                 for r in resultats_complets:
# # # #                     if nom in r["predictions"]:
# # # #                         label = r["predictions"][nom]["label"]
# # # #                         labels_count[label] = labels_count.get(label, 0) + 1
                
# # # #                 f.write(f"   • Répartition : ")
# # # #                 f.write(" | ".join([f"{k}: {v}" for k, v in labels_count.items()]))
# # # #                 f.write("\n")
        
# # # #         f.write("\n" + "=" * 80 + "\n")
# # # #         f.write("✅ Fichiers générés :\n")
# # # #         f.write(f"   • CSV  : {FICHIER_CSV}\n")
# # # #         f.write(f"   • JSON : {FICHIER_JSON}\n")
# # # #         f.write(f"   • Rapport : {nom_fichier}\n")
# # # #         f.write("=" * 80 + "\n")

# # # # # ============================================================
# # # # # MAIN
# # # # # ============================================================
# # # # if __name__ == "__main__":
# # # #     print("🔬 COMPARAISON DE 4 MODÈLES - SENTIMENT ANALYSIS")
# # # #     print("=" * 80)
    
# # # #     # 1. Charger les modèles
# # # #     modeles_charges = charger_modeles()
    
# # # #     if not modeles_charges:
# # # #         print("❌ Aucun modèle chargé. Arrêt du script.")
# # # #         exit(1)
    
# # # #     # 2. Connexion MongoDB
# # # #     client = MongoClient(MONGO_URI)
# # # #     db = client[DB_NAME]
# # # #     coll = db[COL_SOURCE]
    
# # # #     # 3. Récupérer N commentaires aléatoires
# # # #     echantillon = list(coll.aggregate([{"$sample": {"size": NOMBRE_TESTS}}]))
# # # #     print(f"\n📥 {len(echantillon)} commentaires chargés pour le test\n")
    
# # # #     # 4. Initialiser les fichiers d'export
# # # #     initialiser_csv(FICHIER_CSV, modeles_charges.keys())
# # # #     resultats_complets = []
    
# # # #     # 5. Tester chaque commentaire
# # # #     start_global = time.time()
    
# # # #     for i, doc in enumerate(echantillon, 1):
# # # #         texte = doc.get("Commentaire_Client", "")[:300]
# # # #         print(f"[{i}/{NOMBRE_TESTS}] {texte[:60]}...")
        
# # # #         start = time.time()
# # # #         predictions = predire(modeles_charges, texte)
# # # #         duration = time.time() - start
        
# # # #         # Affichage immédiat
# # # #         for nom, res in predictions.items():
# # # #             print(f"   🤖 {nom:15} → {res['label']:10} ({res['confiance']:.2%})")
# # # #         print()
        
# # # #         # Sauvegarder
# # # #         resultats_complets.append({
# # # #             "id": i,
# # # #             "texte": texte,
# # # #             "predictions": predictions,
# # # #             "temps": duration
# # # #         })
        
# # # #         sauvegarder_csv(FICHIER_CSV, modeles_charges.keys(), doc, predictions, i)
    
# # # #     temps_total = time.time() - start_global
    
# # # #     # 6. Export JSON
# # # #     sauvegarder_json(FICHIER_JSON, resultats_complets)
    
# # # #     # 7. Générer rapport
# # # #     generer_rapport(RAPPORT_TXT, resultats_complets, modeles_charges)
    
# # # #     # 8. Stats finales
# # # #     print("\n" + "=" * 80)
# # # #     print("📊 STATISTIQUES FINALES")
# # # #     print("=" * 80)
    
# # # #     for nom in modeles_charges.keys():
# # # #         confiances = [r["predictions"].get(nom, {}).get("confiance", 0) 
# # # #                      for r in resultats_complets if nom in r["predictions"]]
# # # #         if confiances:
# # # #             print(f"\n📈 {nom}:")
# # # #             print(f"   Confiance moyenne : {sum(confiances)/len(confiances):.2%}")
    
# # # #     print(f"\n⏱️  Temps total : {temps_total:.2f}s")
# # # #     print(f"🚀 Vitesse : {NOMBRE_TESTS/temps_total:.1f} docs/sec")
    
# # # #     client.close()
    
# # # #     print(f"\n✅ TERMINÉ !")
# # # #     print(f"   📁 CSV : {FICHIER_CSV}")
# # # #     print(f"   📁 JSON : {FICHIER_JSON}")
# # # #     print(f"   📄 Rapport : {RAPPORT_TXT}")
# # # #     print("=" * 80)

# # # #!/usr/bin/env python3
# # # # -*- coding: utf-8 -*-
# # # # scripts/nlp/comparer_4_modeles.py

# # # from transformers import AutoTokenizer, AutoModelForSequenceClassification
# # # import torch
# # # torch.set_num_threads(2)
# # # from pymongo import MongoClient
# # # import time
# # # import csv
# # # import json
# # # from datetime import datetime

# # # # ============================================================
# # # # CONFIGURATION
# # # # ============================================================
# # # MONGO_URI = "mongodb://localhost:27018/"
# # # DB_NAME = "telecom_algerie"
# # # COL_SOURCE = "commentaires_sans_doublons"  # Collection après nettoyage
# # # NOMBRE_TESTS = 20  # Nombre de commentaires à tester

# # # # ✅ Les 4 modèles
# # # MODELES = {
# # #     "DziriBERT": "alger-ia/dziribert_sentiment",           # 🥇 Spécialisé dialecte algérien
# # #     "AraBERT": "aubmindlab/bert-base-arabertv02",          # 🥈 Arabe standard
# # #     "CAMeLBERT": "CAMeL-Lab/camelbert-mix",                # 🥉 Mixte (arabe + dialectes)
# # #     "DistilBERT_FR": "distilbert-base-uncased-finetuned-sst-2-english",  # ⚠️ C'est anglais !
# # # }

# # # # ✅ Labels pour chaque modèle
# # # LABELS_MODELES = {
# # #     "DziriBERT": {0: "NEGATIF", 1: "NEUTRE", 2: "POSITIF"},
# # #     "AraBERT": {0: "NEGATIF", 1: "POSITIF", 2: "NEUTRE"},
# # #     "CAMeLBERT": {0: "NEGATIF", 1: "POSITIF", 2: "NEUTRE"},
# # #     "DistilBERT_FR": {0: "NEGATIF", 1: "POSITIF"},
# # # }

# # # # Export fichiers
# # # TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
# # # FICHIER_CSV = f"resultats_comparison_4modeles_{TIMESTAMP}.csv"
# # # FICHIER_JSON = f"resultats_comparison_4modeles_{TIMESTAMP}.json"
# # # RAPPORT_TXT = f"rapport_comparison_4modeles_{TIMESTAMP}.txt"

# # # # ============================================================
# # # # FONCTION POUR DÉTECTER LE NOMBRE DE LABELS
# # # # ============================================================
# # # def get_num_labels(model):
# # #     try:
# # #         if hasattr(model.config, 'num_labels'):
# # #             return model.config.num_labels
# # #         elif hasattr(model.config, 'num_classes'):
# # #             return model.config.num_classes
# # #     except:
# # #         pass
# # #     return None

# # # # ============================================================
# # # # CHARGEMENT DES MODÈLES
# # # # ============================================================
# # # def charger_modeles():
# # #     modeles_charges = {}
# # #     for nom, path in MODELES.items():
# # #         try:
# # #             print(f"🤖 Chargement de {nom}...")
# # #             tokenizer = AutoTokenizer.from_pretrained(path)
# # #             model = AutoModelForSequenceClassification.from_pretrained(path)
# # #             model.eval()
            
# # #             num_labels = get_num_labels(model)
# # #             labels = LABELS_MODELES.get(nom, {})
# # #             print(f"   ✅ {nom} chargé ({num_labels} labels)\n")
            
# # #             modeles_charges[nom] = {
# # #                 "tokenizer": tokenizer,
# # #                 "model": model,
# # #                 "labels": labels,
# # #                 "num_labels": num_labels
# # #             }
# # #         except Exception as e:
# # #             print(f"   ❌ Échec {nom} : {str(e)[:100]}\n")
# # #     return modeles_charges

# # # # ============================================================
# # # # PRÉDICTION
# # # # ============================================================
# # # def predire(modeles_charges, texte):
# # #     resultats = {}
# # #     for nom, components in modeles_charges.items():
# # #         tokenizer = components["tokenizer"]
# # #         model = components["model"]
# # #         labels = components["labels"]
        
# # #         try:
# # #             inputs = tokenizer(texte, return_tensors="pt", truncation=True, max_length=128)
# # #             with torch.no_grad():
# # #                 outputs = model(**inputs)
# # #                 probs = torch.softmax(outputs.logits, dim=-1)[0]
# # #                 classe = torch.argmax(probs).item()
# # #                 confiance = probs[classe].item()
# # #                 label_name = labels.get(classe, f"Class_{classe}")
            
# # #             resultats[nom] = {
# # #                 "classe": classe,
# # #                 "label": label_name,
# # #                 "confiance": round(confiance, 4),
# # #             }
# # #         except Exception as e:
# # #             resultats[nom] = {
# # #                 "classe": -1,
# # #                 "label": f"ERREUR",
# # #                 "confiance": 0,
# # #             }
# # #     return resultats

# # # # ============================================================
# # # # EXPORT CSV
# # # # ============================================================
# # # def initialiser_csv(nom_fichier, modeles):
# # #     with open(nom_fichier, "w", newline="", encoding="utf-8-sig") as f:
# # #         writer = csv.writer(f)
# # #         header = ["id", "commentaire", "longueur"]
# # #         for nom in modeles:
# # #             header.extend([f"{nom}_sentiment", f"{nom}_confiance"])
# # #         writer.writerow(header)

# # # def sauvegarder_csv(nom_fichier, modeles, doc, resultats, idx):
# # #     with open(nom_fichier, "a", newline="", encoding="utf-8-sig") as f:
# # #         writer = csv.writer(f)
# # #         row = [idx, doc.get("Commentaire_Client", "")[:300], len(doc.get("Commentaire_Client", ""))]
# # #         for nom in modeles:
# # #             if nom in resultats:
# # #                 row.extend([resultats[nom]["label"], resultats[nom]["confiance"]])
# # #             else:
# # #                 row.extend(["N/A", "N/A"])
# # #         writer.writerow(row)

# # # # ============================================================
# # # # EXPORT JSON
# # # # ============================================================
# # # def sauvegarder_json(nom_fichier, resultats_complets):
# # #     with open(nom_fichier, "w", encoding="utf-8") as f:
# # #         json.dump(resultats_complets, f, ensure_ascii=False, indent=2)

# # # # ============================================================
# # # # RAPPORT TEXTE
# # # # ============================================================
# # # def generer_rapport(nom_fichier, resultats_complets, modeles_charges):
# # #     with open(nom_fichier, "w", encoding="utf-8") as f:
# # #         f.write("=" * 80 + "\n")
# # #         f.write("🔬 COMPARAISON DE 4 MODÈLES - ANALYSE DE SENTIMENT\n")
# # #         f.write(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
# # #         f.write("=" * 80 + "\n\n")
        
# # #         f.write("📋 MODÈLES TESTÉS :\n")
# # #         for nom in MODELES.keys():
# # #             statut = "✅" if nom in modeles_charges else "❌"
# # #             f.write(f"   {statut} {nom:20} : {MODELES.get(nom, 'N/A')}\n")
# # #         f.write("\n")
        
# # #         # Taux d'accord
# # #         f.write("📊 TAUX D'ACCORD ENTRE MODÈLES :\n")
# # #         f.write("-" * 80 + "\n")
        
# # #         modeles_list = list(modeles_charges.keys())
# # #         for i, m1 in enumerate(modeles_list):
# # #             for m2 in modeles_list[i+1:]:
# # #                 accords = 0
# # #                 total = 0
# # #                 for r in resultats_complets:
# # #                     if m1 in r["predictions"] and m2 in r["predictions"]:
# # #                         total += 1
# # #                         if r["predictions"][m1]["label"] == r["predictions"][m2]["label"]:
# # #                             accords += 1
# # #                 if total > 0:
# # #                     f.write(f"   {m1:15} vs {m2:15} : {accords}/{total} ({accords/total*100:.1f}%)\n")
        
# # #         f.write("\n📊 STATISTIQUES PAR MODÈLE :\n")
# # #         f.write("-" * 80 + "\n")
        
# # #         for nom in modeles_charges.keys():
# # #             confiances = [r["predictions"].get(nom, {}).get("confiance", 0) 
# # #                          for r in resultats_complets if nom in r["predictions"]]
            
# # #             if confiances:
# # #                 avg_conf = sum(confiances) / len(confiances)
# # #                 f.write(f"\n🤖 {nom}:\n")
# # #                 f.write(f"   • Confiance moyenne : {avg_conf:.2%}\n")
                
# # #                 # Compter les labels
# # #                 labels_count = {}
# # #                 for r in resultats_complets:
# # #                     if nom in r["predictions"]:
# # #                         label = r["predictions"][nom]["label"]
# # #                         labels_count[label] = labels_count.get(label, 0) + 1
                
# # #                 f.write(f"   • Répartition : ")
# # #                 f.write(" | ".join([f"{k}: {v}" for k, v in labels_count.items()]))
# # #                 f.write("\n")
        
# # #         f.write("\n" + "=" * 80 + "\n")
# # #         f.write("✅ Fichiers générés :\n")
# # #         f.write(f"   • CSV  : {FICHIER_CSV}\n")
# # #         f.write(f"   • JSON : {FICHIER_JSON}\n")
# # #         f.write(f"   • Rapport : {nom_fichier}\n")
# # #         f.write("=" * 80 + "\n")

# # # # ============================================================
# # # # MAIN
# # # # ============================================================
# # # if __name__ == "__main__":
# # #     print("🔬 COMPARAISON DE 4 MODÈLES - SENTIMENT ANALYSIS")
# # #     print("=" * 80)
    
# # #     # 1. Charger les modèles
# # #     modeles_charges = charger_modeles()
    
# # #     if not modeles_charges:
# # #         print("❌ Aucun modèle chargé. Arrêt du script.")
# # #         exit(1)
    
# # #     # 2. Connexion MongoDB
# # #     print("\n📂 Connexion MongoDB...")
# # #     client = MongoClient(MONGO_URI)
# # #     db = client[DB_NAME]
# # #     coll = db[COL_SOURCE]
# # #     total = coll.count_documents({})
# # #     print(f"✅ {total} commentaires disponibles\n")
    
# # #     # 3. Récupérer les PREMIERS commentaires (pas aléatoires)
# # #     print(f"📥 Récupération des {NOMBRE_TESTS} premiers commentaires...")
# # #     # ✅ Utiliser find() sans sample, et limit()
# # #     echantillon = list(coll.find({}, {"Commentaire_Client": 1}).limit(NOMBRE_TESTS))
# # #     print(f"✅ {len(echantillon)} commentaires chargés (les premiers)\n")
    
# # #     # 4. Afficher les commentaires
# # #     print("=" * 80)
# # #     print("📝 COMMENTAIRES À ANALYSER :")
# # #     print("=" * 80)
# # #     for i, doc in enumerate(echantillon, 1):
# # #         texte = doc.get("Commentaire_Client", "")[:100]
# # #         print(f"{i}. {texte}...")
# # #     print()
    
# # #     # 5. Initialiser les fichiers d'export
# # #     initialiser_csv(FICHIER_CSV, modeles_charges.keys())
# # #     resultats_complets = []
    
# # #     # 6. Tester chaque commentaire
# # #     start_global = time.time()
    
# # #     for i, doc in enumerate(echantillon, 1):
# # #         texte = doc.get("Commentaire_Client", "")
# # #         print(f"\n{'='*70}")
# # #         print(f"[{i}/{NOMBRE_TESTS}] COMMENTAIRE")
# # #         print(f"   {texte[:200]}..." if len(texte) > 200 else f"   {texte}")
        
# # #         start = time.time()
# # #         predictions = predire(modeles_charges, texte)
# # #         duration = time.time() - start
        
# # #         # Affichage des résultats
# # #         print(f"\n   📊 RÉSULTATS :")
# # #         for nom, res in predictions.items():
# # #             if "POSITIF" in res['label']:
# # #                 emoji = "😊"
# # #             elif "NEGATIF" in res['label']:
# # #                 emoji = "😠"
# # #             else:
# # #                 emoji = "😐"
# # #             print(f"      🤖 {nom:15} → {emoji} {res['label']:10} ({res['confiance']:.2%})")
        
# # #         # Sauvegarder
# # #         resultats_complets.append({
# # #             "id": i,
# # #             "texte": texte[:500],
# # #             "predictions": predictions,
# # #             "temps": duration
# # #         })
        
# # #         sauvegarder_csv(FICHIER_CSV, modeles_charges.keys(), doc, predictions, i)
    
# # #     temps_total = time.time() - start_global
    
# # #     # 7. Export JSON
# # #     sauvegarder_json(FICHIER_JSON, resultats_complets)
    
# # #     # 8. Générer rapport
# # #     generer_rapport(RAPPORT_TXT, resultats_complets, modeles_charges)
    
# # #     # 9. Stats finales
# # #     print("\n" + "=" * 80)
# # #     print("📊 STATISTIQUES FINALES")
# # #     print("=" * 80)
    
# # #     for nom in modeles_charges.keys():
# # #         confiances = [r["predictions"].get(nom, {}).get("confiance", 0) 
# # #                      for r in resultats_complets if nom in r["predictions"]]
# # #         if confiances:
# # #             print(f"\n📈 {nom}:")
# # #             print(f"   Confiance moyenne : {sum(confiances)/len(confiances):.2%}")
    
# # #     print(f"\n⏱️  Temps total : {temps_total:.2f}s")
# # #     print(f"🚀 Vitesse : {NOMBRE_TESTS/temps_total:.1f} docs/sec")
    
# # #     client.close()
    
# # #     print(f"\n✅ TERMINÉ !")
# # #     print(f"   📁 CSV : {FICHIER_CSV}")
# # #     print(f"   📁 JSON : {FICHIER_JSON}")
# # #     print(f"   📄 Rapport : {RAPPORT_TXT}")
# # #     print("=" * 80)
# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # # comparer_modeles_avec_camel.py
# # # Compare DziriBERT, AraBERT, CAMeL Tools, VADER (lexique), DistilBERT_FR

# # from transformers import AutoTokenizer, AutoModelForSequenceClassification
# # import torch
# # from pymongo import MongoClient
# # from datetime import datetime
# # import numpy as np
# # import re
# # import time

# # print("=" * 80)
# # print("🔬 COMPARAISON DES 5 MODÈLES D'ANALYSE DE SENTIMENT")
# # print("   DziriBERT | AraBERT | CAMeL Tools | VADER | DistilBERT_FR")
# # print("=" * 80)

# # # ============================================================
# # # 1. CHARGER LES MODÈLES
# # # ============================================================

# # # --- DziriBERT ---
# # print("\n🤖 Chargement de DziriBERT...")
# # try:
# #     tokenizer_dziri = AutoTokenizer.from_pretrained("alger-ia/dziribert_sentiment")
# #     model_dziri = AutoModelForSequenceClassification.from_pretrained("alger-ia/dziribert_sentiment")
# #     model_dziri.eval()
# #     print("✅ DziriBERT chargé")
# #     DZIRI_OK = True
# # except Exception as e:
# #     print(f"⚠️ DziriBERT non disponible: {e}")
# #     DZIRI_OK = False

# # # --- AraBERT (fine-tuné pour sentiment) ---
# # print("\n🤖 Chargement de AraBERT...")
# # try:
# #     tokenizer_arabert = AutoTokenizer.from_pretrained("HadyNLP/arabert-sentiment")
# #     model_arabert = AutoModelForSequenceClassification.from_pretrained("HadyNLP/arabert-sentiment")
# #     model_arabert.eval()
# #     print("✅ AraBERT chargé")
# #     ARABERT_OK = True
# # except:
# #     try:
# #         tokenizer_arabert = AutoTokenizer.from_pretrained("aubmindlab/bert-base-arabertv02")
# #         model_arabert = AutoModelForSequenceClassification.from_pretrained("aubmindlab/bert-base-arabertv02")
# #         model_arabert.eval()
# #         print("✅ AraBERT (standard) chargé")
# #         ARABERT_OK = True
# #     except:
# #         ARABERT_OK = False

# # # --- CAMeL Tools (avec chargement correct) ---
# # print("\n🐫 Chargement de CAMeL Tools...")
# # CAMEL_OK = False
# # camel_analyzer = None

# # try:
# #     from camel_tools.sentiment import SentimentAnalyzer
# #     from camel_tools.utils.normalize import normalize_alef, normalize_teh
# #     from camel_tools.models import download
    
# #     # Télécharger le modèle de sentiment si nécessaire
# #     try:
# #         download.download_sentiment_model()
# #     except:
# #         pass
    
# #     # Initialiser l'analyseur
# #     camel_analyzer = SentimentAnalyzer()
    
# #     # Vérifier que ça fonctionne
# #     test_result = camel_analyzer.analyze("شكرا")
# #     if test_result.get('sentiment') != 'neutral':
# #         print("✅ CAMeL Tools chargé (modèle de sentiment actif)")
# #         CAMEL_OK = True
# #     else:
# #         print("⚠️ CAMeL Tools chargé mais modèle de sentiment non actif")
# #         CAMEL_OK = True  # On garde quand même pour tester
        
# # except ImportError as e:
# #     print(f"⚠️ CAMeL Tools non installé: {e}")
# #     print("   Installation: pip install camel-tools")
# #     print("   Puis: camel_data -i sentiment-analysis-arabert")
# # except Exception as e:
# #     print(f"⚠️ Erreur CAMeL Tools: {e}")
# #     print("   Le modèle sera désactivé")

# # # --- VADER avec lexique personnalisé ---
# # print("\n📚 Chargement de VADER avec lexique personnalisé...")

# # # Lexique pour le dialecte algérien
# # LEXIQUE_POSITIF = {
# #     'شكرا': 2.0, 'ممتاز': 2.5, 'جيد': 1.5, 'مزيان': 1.8, 'رائع': 2.0,
# #     'يوفقكم': 1.8, 'باهي': 1.2, 'مليح': 1.2, 'الحمدلله': 1.5, 'تبارك': 1.8,
# #     'بالتوفيق': 1.5, 'نعم': 1.0, 'صح': 1.0, 'ممتازة': 2.5, 'رائعة': 2.0,
# #     'مرسي': 1.5, 'بارك': 1.5, 'ربي': 1.0, 'واصل': 1.0,
# #     'merci': 1.5, 'bravo': 2.0, 'excellent': 2.5, 'super': 2.0, 'good': 1.5,
# # }

# # LEXIQUE_NEGATIF = {
# #     'مشكل': -1.8, 'فاشلة': -2.0, 'عطب': -1.8, 'ضعيف': -1.5, 'بطيء': -1.5,
# #     'مقطوع': -1.8, 'غالي': -1.2, 'خايب': -1.5, 'قهر': -2.0, 'زعاف': -1.5,
# #     'نكد': -1.5, 'لا': -1.0, 'ماشي': -1.0, 'والو': -1.0, 'مازال': -1.0,
# #     'بدون': -1.0, 'خاسر': -1.5, 'ناقص': -1.2, 'صعاب': -1.2,
# #     'problème': -1.5, 'erreur': -1.5, 'lent': -1.2, 'bad': -1.5, 'problem': -1.5,
# # }

# # NEGATIONS = {'لا', 'مش', 'ماشي', 'ماكاش', 'بدون', 'pas', 'non', 'not'}

# # def vader_lexique_analyse(texte):
# #     if not texte or len(texte) < 3:
# #         return "NEUTRE", 0.5
    
# #     mots = re.findall(r'[\w\u0600-\u06FF]+', texte.lower())
# #     score = 0
# #     negatif_actif = False
    
# #     for mot in mots:
# #         if mot in NEGATIONS:
# #             negatif_actif = True
# #             continue
        
# #         val = 0
# #         if mot in LEXIQUE_POSITIF:
# #             val = LEXIQUE_POSITIF[mot]
# #         elif mot in LEXIQUE_NEGATIF:
# #             val = LEXIQUE_NEGATIF[mot]
        
# #         if val != 0:
# #             if negatif_actif:
# #                 val = -val
# #             score += val
# #             negatif_actif = False
    
# #     if mots:
# #         score = score / max(1, len(mots) / 5)
# #     score = max(-1, min(1, score))
    
# #     if score >= 0.2:
# #         return "POSITIF", min(0.9, 0.5 + score)
# #     elif score <= -0.2:
# #         return "NEGATIF", min(0.9, 0.5 + abs(score))
# #     else:
# #         return "NEUTRE", 0.5

# # print("✅ VADER avec lexique chargé")

# # # --- DistilBERT_FR ---
# # print("\n🤖 Chargement de DistilBERT_FR...")
# # try:
# #     tokenizer_distil = AutoTokenizer.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")
# #     model_distil = AutoModelForSequenceClassification.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")
# #     model_distil.eval()
# #     print("✅ DistilBERT_FR chargé")
# #     DISTIL_OK = True
# # except:
# #     DISTIL_OK = False

# # # ============================================================
# # # 2. FONCTIONS DE PRÉDICTION
# # # ============================================================

# # def predire_dziri(texte):
# #     if not DZIRI_OK:
# #         return "NEUTRE", 0.5
# #     inputs = tokenizer_dziri(texte, return_tensors="pt", truncation=True, max_length=128)
# #     with torch.no_grad():
# #         outputs = model_dziri(**inputs)
# #         probs = torch.softmax(outputs.logits, dim=-1)[0]
# #         classe = torch.argmax(probs).item()
# #         conf = probs[classe].item()
# #     labels = {0: "NEGATIF", 1: "NEUTRE", 2: "POSITIF"}
# #     return labels[classe], conf

# # def predire_arabert(texte):
# #     if not ARABERT_OK:
# #         return "NEUTRE", 0.5
# #     inputs = tokenizer_arabert(texte, return_tensors="pt", truncation=True, max_length=128)
# #     with torch.no_grad():
# #         outputs = model_arabert(**inputs)
# #         probs = torch.softmax(outputs.logits, dim=-1)[0]
# #         classe = torch.argmax(probs).item()
# #         conf = probs[classe].item()
# #     labels = {0: "NEGATIF", 1: "POSITIF", 2: "NEUTRE"}
# #     return labels.get(classe, "NEUTRE"), conf

# # def predire_camel(texte):
# #     """Utilise CAMeL Tools pour analyser le sentiment"""
# #     if not CAMEL_OK or camel_analyzer is None:
# #         return "NEUTRE", 0.5
    
# #     try:
# #         # Normalisation du texte (recommandée pour CAMeL)
# #         texte_norm = normalize_alef(texte)
# #         texte_norm = normalize_teh(texte_norm)
        
# #         # Analyser
# #         resultat = camel_analyzer.analyze(texte_norm)
        
# #         sentiment = resultat.get('sentiment', 'neutral').upper()
# #         score = resultat.get('score', 0.5)
        
# #         if sentiment == "POSITIVE":
# #             sentiment = "POSITIF"
# #         elif sentiment == "NEGATIVE":
# #             sentiment = "NEGATIF"
# #         else:
# #             sentiment = "NEUTRE"
        
# #         return sentiment, score
# #     except Exception as e:
# #         return "NEUTRE", 0.5

# # def predire_distil(texte):
# #     if not DISTIL_OK:
# #         return "NEUTRE", 0.5
# #     inputs = tokenizer_distil(texte, return_tensors="pt", truncation=True, max_length=128)
# #     with torch.no_grad():
# #         outputs = model_distil(**inputs)
# #         probs = torch.softmax(outputs.logits, dim=-1)[0]
# #         classe = torch.argmax(probs).item()
# #         conf = probs[classe].item()
# #     labels = {0: "NEGATIF", 1: "POSITIF"}
# #     return labels.get(classe, "NEUTRE"), conf

# # # ============================================================
# # # 3. RÉCUPÉRER LES COMMENTAIRES
# # # ============================================================
# # print("\n📂 Connexion MongoDB...")
# # client = MongoClient("mongodb://localhost:27018/")
# # db = client["telecom_algerie"]
# # coll = db["commentaires_sans_doublons"]

# # NOMBRE_TESTS = 20
# # print(f"📥 Récupération des {NOMBRE_TESTS} premiers commentaires...")
# # echantillon = list(coll.find({}, {"Commentaire_Client": 1}).limit(NOMBRE_TESTS))
# # print(f"✅ {len(echantillon)} commentaires chargés\n")

# # # ============================================================
# # # 4. ANALYSE
# # # ============================================================
# # print("=" * 80)
# # print("🔍 ANALYSE DES COMMENTAIRES")
# # print("=" * 80)

# # stats = {"dziri": [], "arabert": [], "camel": [], "vader": [], "distil": []}
# # comparaisons = {
# #     "dziri_vs_camel": 0, "dziri_vs_arabert": 0, "dziri_vs_vader": 0,
# #     "arabert_vs_camel": 0, "vader_vs_camel": 0
# # }

# # for i, doc in enumerate(echantillon, 1):
# #     texte = doc.get("Commentaire_Client", "")
# #     print(f"\n{'='*70}")
# #     print(f"[{i}/{NOMBRE_TESTS}] {texte[:80]}...")
    
# #     # Prédictions
# #     s1, c1 = predire_dziri(texte)
# #     s2, c2 = predire_arabert(texte)
# #     s3, c3 = predire_camel(texte)
# #     s4, c4 = vader_lexique_analyse(texte)
# #     s5, c5 = predire_distil(texte)
    
# #     stats["dziri"].append(c1)
# #     stats["arabert"].append(c2)
# #     stats["camel"].append(c3)
# #     stats["vader"].append(c4)
# #     stats["distil"].append(c5)
    
# #     # Comparaisons
# #     if s1 == s2: comparaisons["dziri_vs_arabert"] += 1
# #     if s1 == s3: comparaisons["dziri_vs_camel"] += 1
# #     if s1 == s4: comparaisons["dziri_vs_vader"] += 1
# #     if s2 == s3: comparaisons["arabert_vs_camel"] += 1
# #     if s4 == s3: comparaisons["vader_vs_camel"] += 1
    
# #     # Affichage
# #     emoji1 = "😊" if s1 == "POSITIF" else "😠" if s1 == "NEGATIF" else "😐"
# #     emoji2 = "😊" if s2 == "POSITIF" else "😠" if s2 == "NEGATIF" else "😐"
# #     emoji3 = "😊" if s3 == "POSITIF" else "😠" if s3 == "NEGATIF" else "😐"
# #     emoji4 = "😊" if s4 == "POSITIF" else "😠" if s4 == "NEGATIF" else "😐"
# #     emoji5 = "😊" if s5 == "POSITIF" else "😠" if s5 == "NEGATIF" else "😐"
    
# #     print(f"\n   📊 RÉSULTATS :")
# #     print(f"      🤖 DziriBERT     → {emoji1} {s1:8} ({c1:.2%})")
# #     print(f"      🤖 AraBERT       → {emoji2} {s2:8} ({c2:.2%})")
# #     print(f"      🐫 CAMeL Tools   → {emoji3} {s3:8} ({c3:.2%})")
# #     print(f"      📚 VADER (lex)   → {emoji4} {s4:8} ({c4:.2%})")
# #     print(f"      🇬🇧 DistilBERT   → {emoji5} {s5:8} ({c5:.2%})")

# # # ============================================================
# # # 5. STATISTIQUES
# # # ============================================================
# # print("\n" + "=" * 80)
# # print("📊 STATISTIQUES FINALES")
# # print("=" * 80)

# # print(f"\n📈 CONFIANCES MOYENNES :")
# # print(f"   🤖 DziriBERT     : {sum(stats['dziri'])/len(stats['dziri']):.2%}")
# # print(f"   🤖 AraBERT       : {sum(stats['arabert'])/len(stats['arabert']):.2%}")
# # print(f"   🐫 CAMeL Tools   : {sum(stats['camel'])/len(stats['camel']):.2%}")
# # print(f"   📚 VADER (lex)   : {sum(stats['vader'])/len(stats['vader']):.2%}")
# # print(f"   🇬🇧 DistilBERT   : {sum(stats['distil'])/len(stats['distil']):.2%}")

# # print(f"\n📊 TAUX D'ACCORD ENTRE MODÈLES :")
# # print(f"   DziriBERT vs CAMeL   : {comparaisons['dziri_vs_camel']}/{NOMBRE_TESTS} ({comparaisons['dziri_vs_camel']/NOMBRE_TESTS*100:.0f}%)")
# # print(f"   DziriBERT vs AraBERT : {comparaisons['dziri_vs_arabert']}/{NOMBRE_TESTS} ({comparaisons['dziri_vs_arabert']/NOMBRE_TESTS*100:.0f}%)")
# # print(f"   DziriBERT vs VADER   : {comparaisons['dziri_vs_vader']}/{NOMBRE_TESTS} ({comparaisons['dziri_vs_vader']/NOMBRE_TESTS*100:.0f}%)")
# # print(f"   AraBERT vs CAMeL     : {comparaisons['arabert_vs_camel']}/{NOMBRE_TESTS} ({comparaisons['arabert_vs_camel']/NOMBRE_TESTS*100:.0f}%)")
# # print(f"   VADER vs CAMeL       : {comparaisons['vader_vs_camel']}/{NOMBRE_TESTS} ({comparaisons['vader_vs_camel']/NOMBRE_TESTS*100:.0f}%)")

# # print(f"\n🏆 RECOMMANDATION :")
# # print(f"   → DZIRIBERT est le plus adapté pour le dialecte algérien")
# # print(f"   → CAMeL Tools peut servir de référence")
# # print(f"   → VADER avec lexique personnalisé (approche de ton ami)")
# # print(f"   → DistilBERT_FR est un modèle anglais, à exclure")

# # client.close()
# # print("\n🔌 Terminé!")

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# # phase2_bert_encodeurs.py
# # Test des modèles BERT spécialisés pour l'arabe/dialecte algérien
# # Modèles: DziriBERT | AraBERT | MarBERT

# import torch
# import pandas as pd
# import time
# import os
# from datetime import datetime
# from pymongo import MongoClient
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# import numpy as np

# print("=" * 80)
# print("🔬 PHASE 2 : TEST DES MODÈLES BERT ENCODEURS")
# print("   Modèles: DziriBERT | AraBERT | MarBERT")
# print("=" * 80)

# # ============================================================
# # CONFIGURATION
# # ============================================================
# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "telecom_algerie"
# COLLECTION = "commentaires_normalises"
# NB_TESTS = 10

# # Liste des modèles BERT à tester
# MODELES = {
#     "DziriBERT (Algérien)": {
#         "path": "alger-ia/dziribert_sentiment",
#         "labels": {0: "NEGATIF", 1: "NEUTRE", 2: "POSITIF"},
#         "specialite": "Dialecte algérien"
#     },
#     "AraBERT (Arabe standard)": {
#         "path": "HadyNLP/arabert-sentiment",
#         "labels": {0: "NEGATIF", 1: "POSITIF", 2: "NEUTRE"},
#         "specialite": "Arabe standard"
#     },
#     "MarBERT (Maghrébin)": {
#         "path": "UBC-NLP/marbert",
#         "labels": {0: "NEGATIF", 1: "POSITIF", 2: "NEUTRE"},
#         "specialite": "Dialectes maghrébins"
#     }
# }

# # ============================================================
# # 1. CHARGEMENT DES MODÈLES
# # ============================================================
# print("\n📥 CHARGEMENT DES MODÈLES BERT...")
# print("-" * 80)

# modeles_charges = {}

# for nom, config in MODELES.items():
#     print(f"\n🤖 {nom}")
#     print(f"   Spécialité: {config['specialite']}")
#     try:
#         start = time.time()
#         tokenizer = AutoTokenizer.from_pretrained(config["path"])
#         model = AutoModelForSequenceClassification.from_pretrained(config["path"])
#         model.eval()
#         duree = time.time() - start
        
#         modeles_charges[nom] = {
#             "tokenizer": tokenizer,
#             "model": model,
#             "labels": config["labels"]
#         }
#         print(f"   ✅ Chargé en {duree:.2f}s")
#     except Exception as e:
#         print(f"   ❌ Erreur: {e}")
#         print(f"   ⚠️  {nom} sera ignoré")

# if not modeles_charges:
#     print("\n❌ Aucun modèle chargé. Vérifie les installations.")
#     exit(1)

# # ============================================================
# # 2. CONNEXION MONGODB
# # ============================================================
# print("\n📂 CONNEXION MONGODB...")
# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]
# collection = db[COLLECTION]

# total = collection.count_documents({})
# print(f"✅ {total} commentaires disponibles")

# # ============================================================
# # 3. RÉCUPÉRATION DES COMMENTAIRES
# # ============================================================
# print(f"\n📥 Récupération des {NB_TESTS} premiers commentaires...")
# echantillon = list(collection.find({}, {"Commentaire_Client": 1}).limit(NB_TESTS))
# print(f"✅ {len(echantillon)} commentaires chargés\n")

# # ============================================================
# # 4. FONCTION DE PRÉDICTION
# # ============================================================
# def predire_bert(texte, tokenizer, model, labels, nom_modele):
#     """Prédit le sentiment avec un modèle BERT"""
#     try:
#         inputs = tokenizer(texte, return_tensors="pt", truncation=True, max_length=128)
#         with torch.no_grad():
#             outputs = model(**inputs)
#             probs = torch.softmax(outputs.logits, dim=-1)[0]
#             classe = torch.argmax(probs).item()
#             confiance = probs[classe].item()
#             sentiment = labels.get(classe, "NEUTRE")
#         return sentiment, confiance
#     except Exception as e:
#         print(f"   ⚠️ Erreur {nom_modele}: {e}")
#         return "NEUTRE", 0.0

# # ============================================================
# # 5. ANALYSE DES COMMENTAIRES
# # ============================================================
# print("=" * 80)
# print("🔍 ANALYSE DES COMMENTAIRES")
# print("=" * 80)

# resultats = []
# stats = {nom: {"temps": [], "confiances": [], "compteur": {"POSITIF": 0, "NEGATIF": 0, "NEUTRE": 0}} 
#          for nom in modeles_charges.keys()}

# for i, doc in enumerate(echantillon, 1):
#     texte = doc.get("Commentaire_Client", "")
#     print(f"\n{'='*70}")
#     print(f"[{i}/{NB_TESTS}] {texte[:80]}...")
    
#     row = {"id": i, "commentaire": texte[:200]}
    
#     for nom_modele, config in modeles_charges.items():
#         start = time.time()
#         sentiment, confiance = predire_bert(
#             texte, 
#             config["tokenizer"], 
#             config["model"], 
#             config["labels"],
#             nom_modele
#         )
#         duree = time.time() - start
        
#         # Stocker les stats
#         stats[nom_modele]["temps"].append(duree)
#         stats[nom_modele]["confiances"].append(confiance)
#         stats[nom_modele]["compteur"][sentiment] += 1
        
#         # Stocker les résultats
#         row[f"{nom_modele}_sentiment"] = sentiment
#         row[f"{nom_modele}_confiance"] = round(confiance, 4)
#         row[f"{nom_modele}_temps"] = round(duree, 3)
    
#     resultats.append(row)
    
#     # Affichage des résultats
#     print(f"\n   📊 RÉSULTATS :")
#     for nom_modele in modeles_charges.keys():
#         sentiment = row[f"{nom_modele}_sentiment"]
#         confiance = row[f"{nom_modele}_confiance"]
#         duree = row[f"{nom_modele}_temps"]
#         emoji = "😊" if sentiment == "POSITIF" else "😠" if sentiment == "NEGATIF" else "😐"
#         print(f"      {emoji} {nom_modele}: {sentiment} (conf: {confiance:.2%}) - {duree*1000:.0f}ms")

# # ============================================================
# # 6. STATISTIQUES COMPARATIVES
# # ============================================================
# print("\n" + "=" * 80)
# print("📊 STATISTIQUES COMPARATIVES")
# print("=" * 80)

# print(f"\n📈 PERFORMANCES PAR MODÈLE :")
# print("-" * 80)
# print(f"{'Modèle':<35} {'Temps moyen':<15} {'Confiance moyenne':<20} {'Précision':<15}")
# print("-" * 80)

# for nom_modele in modeles_charges.keys():
#     temps_moyen = sum(stats[nom_modele]["temps"]) / len(stats[nom_modele]["temps"])
#     conf_moyenne = sum(stats[nom_modele]["confiances"]) / len(stats[nom_modele]["confiances"])
#     print(f"{nom_modele:<35} {temps_moyen*1000:.0f}ms{'':<8} {conf_moyenne:.2%}{'':<12} ?")

# print("\n📈 RÉPARTITION DES SENTIMENTS :")
# print("-" * 80)
# for nom_modele in modeles_charges.keys():
#     compteur = stats[nom_modele]["compteur"]
#     total = sum(compteur.values())
#     print(f"\n{nom_modele}:")
#     print(f"   😊 POSITIF : {compteur['POSITIF']}/{total} ({compteur['POSITIF']/total*100:.1f}%)")
#     print(f"   😠 NEGATIF : {compteur['NEGATIF']}/{total} ({compteur['NEGATIF']/total*100:.1f}%)")
#     print(f"   😐 NEUTRE  : {compteur['NEUTRE']}/{total} ({compteur['NEUTRE']/total*100:.1f}%)")

# # ============================================================
# # 7. TAUX D'ACCORD ENTRE MODÈLES
# # ============================================================
# print("\n📊 TAUX D'ACCORD ENTRE MODÈLES :")
# print("-" * 80)

# modeles_list = list(modeles_charges.keys())
# for i, m1 in enumerate(modeles_list):
#     for m2 in modeles_list[i+1:]:
#         accords = 0
#         for r in resultats:
#             if r[f"{m1}_sentiment"] == r[f"{m2}_sentiment"]:
#                 accords += 1
#         taux = accords / len(resultats) * 100
#         print(f"   {m1[:20]} vs {m2[:20]}: {accords}/{len(resultats)} ({taux:.1f}%)")

# # ============================================================
# # 8. SAUVEGARDE DES RÉSULTATS
# # ============================================================
# print("\n💾 SAUVEGARDE DES RÉSULTATS...")

# output_dir = "/home/mouna/projet_telecom/scripts/annotation/Rapports"
# os.makedirs(output_dir, exist_ok=True)

# timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# csv_path = f"{output_dir}/phase2_bert_encodeurs_{timestamp}.csv"
# df = pd.DataFrame(resultats)
# df.to_csv(csv_path, index=False, encoding='utf-8-sig')
# print(f"✅ CSV: {csv_path}")

# # ============================================================
# # 9. RAPPORT TXT
# # ============================================================
# rapport_path = f"{output_dir}/phase2_rapport_{timestamp}.txt"
# with open(rapport_path, "w", encoding="utf-8") as f:
#     f.write("=" * 80 + "\n")
#     f.write("RAPPORT PHASE 2 - TEST DES MODÈLES BERT ENCODEURS\n")
#     f.write("=" * 80 + "\n")
#     f.write(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
#     f.write(f"Nombre de commentaires testés : {NB_TESTS}\n\n")
    
#     f.write("MODÈLES TESTÉS :\n")
#     for nom, config in MODELES.items():
#         statut = "✅" if nom in modeles_charges else "❌"
#         f.write(f"   {statut} {nom}: {config['specialite']}\n")
#     f.write("\n")
    
#     f.write("PERFORMANCES PAR MODÈLE :\n")
#     for nom_modele in modeles_charges.keys():
#         temps_moyen = sum(stats[nom_modele]["temps"]) / len(stats[nom_modele]["temps"])
#         conf_moyenne = sum(stats[nom_modele]["confiances"]) / len(stats[nom_modele]["confiances"])
#         f.write(f"\n   {nom_modele}:\n")
#         f.write(f"      Temps moyen : {temps_moyen*1000:.0f} ms\n")
#         f.write(f"      Confiance moyenne : {conf_moyenne:.2%}\n")
        
#         compteur = stats[nom_modele]["compteur"]
#         total = sum(compteur.values())
#         f.write(f"      POSITIF : {compteur['POSITIF']}/{total} ({compteur['POSITIF']/total*100:.1f}%)\n")
#         f.write(f"      NEGATIF : {compteur['NEGATIF']}/{total} ({compteur['NEGATIF']/total*100:.1f}%)\n")
#         f.write(f"      NEUTRE  : {compteur['NEUTRE']}/{total} ({compteur['NEUTRE']/total*100:.1f}%)\n")
    
#     f.write("\nTAUX D'ACCORD :\n")
#     for i, m1 in enumerate(modeles_list):
#         for m2 in modeles_list[i+1:]:
#             accords = 0
#             for r in resultats:
#                 if r[f"{m1}_sentiment"] == r[f"{m2}_sentiment"]:
#                     accords += 1
#             taux = accords / len(resultats) * 100
#             f.write(f"   {m1[:20]} vs {m2[:20]}: {taux:.1f}%\n")

# print(f"✅ Rapport: {rapport_path}")

# # ============================================================
# # 10. RECOMMANDATION
# # ============================================================
# print("\n" + "=" * 80)
# print("🎯 RECOMMANDATION FINALE")
# print("=" * 80)

# if "DziriBERT (Algérien)" in modeles_charges:
#     print("\n🏆 DZIRIBERT est le modèle recommandé pour ton projet !")
#     print("   → Spécialisé pour le dialecte algérien")
#     print("   → Entraîné sur des commentaires similaires")
#     print("   → À utiliser pour l'analyse finale des 26k commentaires")
# else:
#     print("\n⚠️  Aucun modèle spécialisé n'est disponible.")
#     print("   Vérifie les installations ou essaie les modèles alternatifs.")

# print("\n📊 COMPARAISON AVEC LA PHASE 1 (LLM) :")
# print("   → Les modèles BERT (Phase 2) sont plus rapides (ms vs secondes)")
# print("   → Les modèles BERT sont spécialisés pour l'arabe/dialecte")
# print("   → Les LLM (Phase 1) sont plus généraux et comprennent mieux le contexte")
# print("   → Pour l'analyse finale, utilise DziriBERT (meilleur ratio vitesse/précision)")

# client.close()
# print("\n🔌 Terminé!")
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# scripts/nlp/phase2_comparaison_robuste.py
# ✅ PHASE 2 : Comparaison avec gestion d'erreurs

import torch
import pandas as pd
from pymongo import MongoClient
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import os
import time

# ============================================================
# 1. CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27018/"
DB_NAME = "telecom_algerie"
COLLECTION = "commentaires_sans_doublons"
NB_TESTS = 10  # Commence avec 10 pour tester vite

print("=" * 80)
print("🔬 PHASE 2 : COMPARAISON ROBUSTE DES MODÈLES")
print("=" * 80)

# ============================================================
# 2. CHARGEMENT DES MODÈLES (AVEC TRY/EXCEPT)
# ============================================================

modeles_charges = {}

# --- DZIRIBERT ---
print("\n🤖 1. Chargement de DziriBERT...")
try:
    tokenizer_dziri = AutoTokenizer.from_pretrained("alger-ia/dziribert_sentiment")
    model_dziri = AutoModelForSequenceClassification.from_pretrained("alger-ia/dziribert_sentiment")
    model_dziri.eval()
    modeles_charges["DziriBERT"] = {"tokenizer": tokenizer_dziri, "model": model_dziri, "labels": {0: "NEGATIF", 1: "NEUTRE", 2: "POSITIF"}}
    print("   ✅ DziriBERT prêt")
except Exception as e:
    print(f"   ❌ DziriBERT échec : {str(e)[:100]}")

# --- ARABERT (Version base publique) ---
print("\n🤖 2. Chargement de AraBERT (version base publique)...")
try:
    # On utilise la version BASE (pas fine-tunée sentiment, mais publique)
    ara_name = "aubmindlab/bert-base-arabertv02"
    tokenizer_ara = AutoTokenizer.from_pretrained(ara_name)
    model_ara = AutoModelForSequenceClassification.from_pretrained(ara_name)
    model_ara.eval()
    # AraBERT base n'a pas de labels sentiment → on utilise des labels génériques
    modeles_charges["AraBERT_base"] = {"tokenizer": tokenizer_ara, "model": model_ara, "labels": {0: "LABEL_0", 1: "LABEL_1"}}
    print("   ✅ AraBERT_base prêt (⚠️ pas fine-tuné sentiment)")
except Exception as e:
    print(f"   ❌ AraBERT échec : {str(e)[:100]}")

# --- MARBERT (Optionnel) ---
print("\n🤖 3. Chargement de MARBERT...")
try:
    marbert_name = "UBC-NLP/MARBERT"
    tokenizer_mar = AutoTokenizer.from_pretrained(marbert_name)
    model_mar = AutoModelForSequenceClassification.from_pretrained(marbert_name)
    model_mar.eval()
    modeles_charges["MARBERT"] = {"tokenizer": tokenizer_mar, "model": model_mar, "labels": {0: "NEGATIF", 1: "NEUTRE", 2: "POSITIF"}}
    print("   ✅ MARBERT prêt")
except Exception as e:
    print(f"   ❌ MARBERT échec : {str(e)[:100]}")

# --- CAMEL TOOLS ---
print("\n🐫 4. Chargement de CAMeL Tools...")
CAMEL_OK = False
camel_analyzer = None
try:
    from camel_tools.sentiment import SentimentAnalyzer
    camel_analyzer = SentimentAnalyzer()
    # Test rapide
    test = camel_analyzer.analyze("شكرا")
    CAMEL_OK = True
    print("   ✅ CAMeL Tools prêt")
except Exception as e:
    print(f"   ⚠️ CAMeL Tools non disponible : {str(e)[:80]}")

# Vérification finale
if not modeles_charges and not CAMEL_OK:
    print("\n❌ Aucun modèle disponible ! Arrêt du script.")
    exit(1)

print(f"\n✅ {len(modeles_charges)} modèles BERT chargés + CAMeL: {'✅' if CAMEL_OK else '❌'}")

# ============================================================
# 3. FONCTIONS DE PRÉDICTION
# ============================================================

def predire_bert(nom, config, texte):
    """Prédiction avec un modèle BERT"""
    tokenizer = config["tokenizer"]
    model = config["model"]
    labels = config["labels"]
    
    inputs = tokenizer(texte, return_tensors="pt", truncation=True, max_length=128)
    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.softmax(outputs.logits, dim=-1)[0]
        idx = torch.argmax(probs).item()
        confiance = probs[idx].item()
    
    return labels.get(idx, f"LABEL_{idx}"), round(confiance, 4)

def predire_camel(texte):
    """Prédiction avec CAMeL Tools"""
    if not CAMEL_OK or camel_analyzer is None:
        return "NEUTRE", 0.5
    try:
        result = camel_analyzer.analyze(texte)
        sentiment = result.get('sentiment', 'neutral').upper()
        if sentiment == 'POSITIVE':
            return "POSITIF", 0.7
        elif sentiment == 'NEGATIVE':
            return "NEGATIF", 0.7
        else:
            return "NEUTRE", 0.5
    except:
        return "ERREUR", 0.0

# ============================================================
# 4. ANALYSE DES COMMENTAIRES
# ============================================================

print("\n" + "=" * 80)
print("📥 CHARGEMENT DES COMMENTAIRES")
print("=" * 80)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
docs = list(db[COLLECTION].find({}, {"Commentaire_Client": 1}).limit(NB_TESTS))
print(f"✅ {len(docs)} commentaires chargés\n")

results = []

for i, doc in enumerate(docs):
    txt = doc.get("Commentaire_Client", "")
    if not txt or len(txt.strip()) < 3:
        continue
        
    print(f"🔄 [{len(results)+1}/{NB_TESTS}] {txt[:60]}...")
    
    resultats_doc = {"Commentaire": txt[:70] + "..."}
    
    # Tester chaque modèle BERT chargé
    for nom, config in modeles_charges.items():
        try:
            sentiment, confiance = predire_bert(nom, config, txt)
            resultats_doc[f"{nom}_sentiment"] = sentiment
            resultats_doc[f"{nom}_confiance"] = confiance
            print(f"   🤖 {nom:15} → {sentiment:10} ({confiance:.2%})")
        except Exception as e:
            resultats_doc[f"{nom}_sentiment"] = "ERREUR"
            resultats_doc[f"{nom}_confiance"] = 0.0
            print(f"   ❌ {nom} erreur : {str(e)[:50]}")
    
    # CAMeL Tools
    if CAMEL_OK:
        try:
            sentiment, confiance = predire_camel(txt)
            resultats_doc["CAMeL_sentiment"] = sentiment
            resultats_doc["CAMeL_confiance"] = confiance
            print(f"   🐫 {'CAMeL':15} → {sentiment:10} ({confiance:.2%})")
        except:
            resultats_doc["CAMeL_sentiment"] = "ERREUR"
            resultats_doc["CAMeL_confiance"] = 0.0
    
    results.append(resultats_doc)
    print()

# ============================================================
# 5. RÉSULTATS & SAUVEGARDE
# ============================================================

print("=" * 80)
print("📊 RÉSULTATS FINAUX")
print("=" * 80)

df = pd.DataFrame(results)
print(df.to_string(index=False))

# Sauvegarde
os.makedirs("Rapports", exist_ok=True)
timestamp = time.strftime("%Y%m%d_%H%M%S")
fichier_csv = f"Rapports/comparaison_phase2_{timestamp}.csv"
df.to_csv(fichier_csv, index=False, encoding='utf-8-sig')

# Stats rapides
print(f"\n📈 STATISTIQUES RAPIDES :")
for col in df.columns:
    if "_sentiment" in col and col != "Commentaire":
        valeurs = df[col].value_counts()
        print(f"   • {col}: {dict(valeurs.head(3))}")

print(f"\n✅ Terminé !")
print(f"📁 Fichier généré : {fichier_csv}")

client.close()