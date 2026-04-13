import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.graphiques import *

print("Test de graphique_evolution_sentiments()")
fig = graphique_evolution_sentiments()
if fig:
    print("✅ Graphique généré avec succès")
    fig.write_html("test_evolution.html")
    print("📁 Fichier sauvegardé : test_evolution.html")
else:
    print("❌ Échec de génération")