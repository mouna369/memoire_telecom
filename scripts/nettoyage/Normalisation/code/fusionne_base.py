#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

base = '/home/mouna/projet_telecom/donnees/transformees/'

with open(base + 'telecom_algerie.commentaires_sans_emojis.json', encoding='utf-8') as f:
    originaux = json.load(f)

with open(base + 'telecom_algerie.commentaires_normalises.json', encoding='utf-8') as f:
    normalises = json.load(f)

print(f'✅ Originaux  : {len(originaux)} docs')
print(f'✅ Normalisés : {len(normalises)} docs')

# Vérification
if len(originaux) != len(normalises):
    print(f'⚠️  Tailles différentes ! Fusion par index impossible.')
    exit(1)

# Fusion par position (même ordre garanti)
corpus = []
for orig, norm in zip(originaux, normalises):
    corpus.append({
        'client_comment'    : orig['Commentaire_Client'],
        'normalized_comment': norm['Commentaire_Client']
    })

# Vérification rapide
print(f'\n🔍 Vérification (3 premiers) :')
for i in range(3):
    print(f'   [{i}] original  : {corpus[i]["client_comment"][:60]}')
    print(f'   [{i}] normalisé : {corpus[i]["normalized_comment"][:60]}')
    print()

# Sauvegarder
out = base + 'corpus_validation.json'
with open(out, 'w', encoding='utf-8') as f:
    json.dump(corpus, f, ensure_ascii=False, indent=2)

print(f'✅ {len(corpus)} docs fusionnés → corpus_validation.json')