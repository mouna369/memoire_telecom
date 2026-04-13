#!/usr/bin/env python3
# monitor.py - Affiche les événements Kafka en temps réel

from kafka import KafkaConsumer
import json
from datetime import datetime

# Couleurs pour mieux voir (optionnel)
class Couleurs:
    ROUGE = '\033[91m'
    VERT = '\033[92m'
    JAUNE = '\033[93m'
    BLEU = '\033[94m'
    VIOLET = '\033[95m'
    RESET = '\033[0m'

# Connexion à Kafka
consumer = KafkaConsumer(
    'pipeline_events',           # Le topic que vous utilisez
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',  # Commence à partir des nouveaux messages
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("=" * 60)
print("📡 SURVEILLANCE DU PIPELINE EN TEMPS RÉEL")
print("=" * 60)
print("En attente des événements...")
print("(Appuyez sur Ctrl+C pour arrêter)")
print("=" * 60)

try:
    for message in consumer:
        event = message.value
        timestamp = event.get('timestamp', datetime.now().isoformat())
        event_type = event.get('event_type', 'inconnu')
        data = event.get('data', {})
        
        # Formatage selon le type d'événement
        if 'debut' in event_type:
            print(f"\n{Couleurs.BLEU}🚀 {event_type} - {timestamp}{Couleurs.RESET}")
            if 'total' in data:
                print(f"   📊 {data['total']} éléments à traiter")
                
        elif 'fin' in event_type:
            print(f"\n{Couleurs.VERT}✅ {event_type} - {timestamp}{Couleurs.RESET}")
            if 'duree' in data:
                print(f"   ⏱️  Durée: {data['duree']:.1f} secondes")
            if 'avant' in data:
                print(f"   📥 Avant: {data['avant']} | 📤 Après: {data['apres']}")
                
        elif 'erreur' in event_type:
            print(f"\n{Couleurs.ROUGE}❌ {event_type} - {timestamp}{Couleurs.RESET}")
            if 'error' in data:
                print(f"   ⚠️  Erreur: {data['error'][:200]}")
                
        elif 'pipeline_debut' in event_type:
            print(f"\n{Couleurs.VIOLET}🏁 PIPELINE DÉMARRÉ - {timestamp}{Couleurs.RESET}")
            
        elif 'pipeline_fin' in event_type:
            print(f"\n{Couleurs.VIOLET}🎉 PIPELINE TERMINÉ - {timestamp}{Couleurs.RESET}")
            
        else:
            print(f"\n{Couleurs.JAUNE}ℹ️ {event_type} - {timestamp}{Couleurs.RESET}")
            print(f"   📦 Données: {data}")
            
except KeyboardInterrupt:
    print(f"\n\n{Couleurs.JAUNE}👋 Arrêt de la surveillance...{Couleurs.RESET}")
finally:
    consumer.close()