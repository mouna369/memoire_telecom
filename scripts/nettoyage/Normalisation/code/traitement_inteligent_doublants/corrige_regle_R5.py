# Ce script corrige les problèmes R5 détectés

import pandas as pd

# Charger les résultats de la vérification
df_complet = pd.read_excel("verification_R5_complete.xlsx")

# Identifier les groupes à problème
groupes_a_corriger = {}

for _, row in df_complet.iterrows():
    groupe_id = row['groupe_id']
    if groupe_id not in groupes_a_corriger:
        groupes_a_corriger[groupe_id] = {}
    
    if row['type'] == 'VERSION_COMPLETE':
        groupes_a_corriger[groupe_id]['complet'] = {
            'commentaire': row['commentaire'],
            'label': row['label']
        }
    else:
        if 'tronques' not in groupes_a_corriger[groupe_id]:
            groupes_a_corriger[groupe_id]['tronques'] = []
        groupes_a_corriger[groupe_id]['tronques'].append({
            'commentaire': row['commentaire'],
            'label': row['label']
        })

print("="*60)
print("CORRECTION DES TRONCATURES")
print("="*60)

for groupe_id, data in groupes_a_corriger.items():
    if 'complet' in data:
        label_correct = data['complet']['label']
        print(f"\n📝 Groupe : {groupe_id}")
        print(f"   ✅ Garder : {data['complet']['commentaire'][:50]}... (label: {label_correct})")
        
        for t in data.get('tronques', []):
            print(f"   ❌ Supprimer : {t['commentaire'][:50]}... (label: {t['label']} → à corriger en {label_correct})")

print("\n" + "="*60)
print("RECOMMANDATION FINALE")
print("="*60)
print("""
1. Supprimez les 143 versions tronquées de votre base MongoDB
2. Pour les 20 incohérences, mettez à jour les labels avec ceux de la version complète
3. Re-exécutez le script de vérification pour confirmer
""")