# ChatBot Algérien — DziriBERT

Chatbot intelligent bilingue **français / darija algérienne** basé sur **DziriBERT**
(`alger-ia/dziribert`), un modèle BERT pré-entraîné spécifiquement sur des corpus algériens.

## Modèles utilisés

| Modèle | HuggingFace ID | Usage |
|--------|---------------|-------|
| **DziriBERT** | `alger-ia/dziribert` | Modèle principal (recommandé) |
| DarijaBERT | `alger-ia/darijabert` | Alternative |
| DarijaBERT (Moussa) | `moussaKam/DarijaBERT` | Alternative |

## Installation

```bash
python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate         # Windows

pip install -r requirements.txt
cp .env.example .env
```

## Étapes

### 1. Entraîner le modèle
```bash
python model/train.py
```
Télécharge DziriBERT depuis HuggingFace et le fine-tune sur vos intentions.
Durée : ~5 min (CPU) / ~1 min (GPU)

### 2. Lancer l'API
```bash
uvicorn api.main:app --reload --port 8000
```
API disponible sur : http://localhost:8000
Documentation auto : http://localhost:8000/docs

### 3. Lancer l'interface chat
```bash
streamlit run ui/app.py
```
Interface disponible sur : http://localhost:8501

### 4. Lancer le dashboard d'analyse
```bash
streamlit run analyse/dashboard.py --server.port 8502
```
Dashboard disponible sur : http://localhost:8502

### 5. Évaluer le modèle
```bash
python analyse/metrics.py
```
Génère dans `analyse/rapports/` :
- `rapport.json` — métriques complètes
- `confusion_matrix.png` — matrice de confusion
- `f1_par_intention.png` — F1-score par intention
- `erreurs.json` — détail des erreurs

## Structure
```
chatbot_algerie/
├── data/
│   ├── intentions.json        ← corpus bilingue fr/darija
│   ├── synonymes_darija.json  ← dictionnaire darija→français
│   └── test_set.json          ← jeu d'évaluation
├── model/
│   ├── train.py               ← fine-tuning DziriBERT
│   ├── predict.py             ← prédiction + normalisation
│   └── dziribert_finetuned/   ← modèle sauvegardé (après train)
├── preprocessing/
│   └── darija_mapper.py       ← normalisation + traduction darija
├── api/
│   ├── main.py                ← FastAPI (chat, stats, history)
│   └── schemas.py             ← modèles Pydantic
├── ui/
│   └── app.py                 ← interface Streamlit
├── analyse/
│   ├── dashboard.py           ← tableau de bord Plotly
│   └── metrics.py             ← évaluation scikit-learn
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Tester l'API

```bash
# Envoyer un message
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "ma kaynach connexion 4G", "session_id": "test1"}'

# Statistiques
curl http://localhost:8000/stats

# Historique session
curl http://localhost:8000/history/test1
```

## Ajouter des intentions

Éditez `data/intentions.json` puis relancez `python model/train.py`.

## Docker

```bash
docker-compose up -d
```
