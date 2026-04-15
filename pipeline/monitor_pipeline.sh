#!/bin/bash
# monitor_pipeline.sh - Surveille l'état du pipeline

echo "============================================================"
echo "📊 MONITORING DU PIPELINE"
echo "============================================================"

# Vérifier les processus
echo ""
echo "🔍 État des processus :"

if pgrep -f "kafka_producer.py" > /dev/null; then
    echo "   ✅ Producer: EN MARCHE"
else
    echo "   ❌ Producer: ARRÊTÉ"
fi

if pgrep -f "kafka_consumer.py" > /dev/null; then
    echo "   ✅ Consumer: EN MARCHE"
else
    echo "   ❌ Consumer: ARRÊTÉ"
fi

if pgrep -f "streamlit run dashboard_final.py" > /dev/null; then
    echo "   ✅ Dashboard: EN MARCHE"
else
    echo "   ❌ Dashboard: ARRÊTÉ"
fi

# Vérifier Kafka
echo ""
echo "📡 État de Kafka :"
if docker ps | grep -q "kafka_pfe"; then
    echo "   ✅ Kafka: EN MARCHE"
else
    echo "   ❌ Kafka: ARRÊTÉ"
fi

# Vérifier MongoDB
echo ""
echo "📂 État de MongoDB :"
if docker ps | grep -q "mongodb_pfe"; then
    echo "   ✅ MongoDB: EN MARCHE"
else
    echo "   ❌ MongoDB: ARRÊTÉ"
fi

# Afficher les derniers logs
echo ""
echo "📋 Derniers logs (Producer) :"
tail -3 /tmp/kafka_producer.log 2>/dev/null || echo "   Aucun log"

echo ""
echo "📋 Derniers logs (Consumer) :"
tail -3 /tmp/kafka_consumer.log 2>/dev/null || echo "   Aucun log"

echo ""
echo "📋 Derniers logs (Dashboard) :"
tail -3 /tmp/dashboard.log 2>/dev/null || echo "   Aucun log"