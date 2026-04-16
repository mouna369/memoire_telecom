# 💠 Drive Dashboard — Streamlit

Dashboard de gestion de fichiers inspiré de Google Drive, construit avec Streamlit.

## 📁 Structure des fichiers

```
drive_dashboard/
├── app.py                          # Point d'entrée principal
├── requirements.txt
├── styles/
│   ├── __init__.py
│   └── theme.py                    # CSS Dark/Light mode
├── components/
│   ├── __init__.py
│   ├── sidebar.py                  # Navigation sidebar
│   ├── cards.py                    # Composants réutilisables (cartes, fichiers, dossiers)
│   ├── calendar_widget.py          # Calendrier + tâches
│   └── storage_chart.py            # Graphiques Plotly
└── pages/
    ├── __init__.py
    ├── dashboard.py                # 🏠 Page principale
    ├── my_drive.py                 # ☁️ Mon Drive
    ├── shared_files.py             # 📤 Fichiers partagés
    ├── file_requests.py            # 📋 Demandes de fichiers
    ├── starred.py                  # ⭐ Favoris
    ├── trash.py                    # 🗑️ Corbeille
    ├── statistics.py               # 📊 Statistiques
    └── task.py                     # ✅ Tableau Kanban
```

## 🚀 Installation & Lancement

```bash
# 1. Installer les dépendances
pip install -r requirements.txt

# 2. Lancer l'application
streamlit run app.py
```

## ✨ Fonctionnalités

- **8 pages** : Dashboard, My Drive, Shared Files, File Requests, Starred, Trash, Statistics, Task
- **Dark / Light mode** toggle dans la sidebar
- **Calendrier interactif** avec vue mensuelle
- **Graphiques Plotly** : barres groupées, donut, courbe de tendance
- **Kanban Board** pour les tâches (To Do / In Progress / Completed)
- **Design fidèle** à la maquette avec couleurs, typographie et mise en page identiques

## 🎨 Palette de couleurs

| Couleur | Hex |
|---------|-----|
| Accent Blue | `#4F6EF7` |
| Red | `#F05454` |
| Yellow | `#F5A623` |
| Green | `#2ECC71` |
| Purple | `#9B59B6` |
| Dark BG | `#0F1117` |
| Dark Card | `#1A1D2E` |
