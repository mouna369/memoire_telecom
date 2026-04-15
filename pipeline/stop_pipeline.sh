#!/bin/bash
# stop_pipeline.sh - Arrête tout le pipeline

echo "============================================================"
echo "🛑 ARRÊT DU PIPELINE COMPLET"
echo "============================================================"

# Lire les PIDs sauvegardés
if [ -f /tmp/pipeline_pids.txt ]; then
    while read PID; do
        if kill -0 $PID 2>/dev/null; then
            kill $PID
            echo "   ✅ Arrêt du processus $PID"
        fi
    done < /tmp/pipeline_pids.txt
    rm /tmp/pipeline_pids.txt
fi

# Tuer les processus restants par nom
echo ""
echo "🧹 Nettoyage des processus restants..."

pkill -f "kafka_producer.py" 2>/dev/null && echo "   ✅ Producer arrêté"
pkill -f "kafka_consumer.py" 2>/dev/null && echo "   ✅ Consumer arrêté"
pkill -f "streamlit run dashboard_final.py" 2>/dev/null && echo "   ✅ Dashboard arrêté"

echo ""
echo "============================================================"
echo "✅ PIPELINE COMPLET ARRÊTÉ"
echo "============================================================"