#!/bin/bash
# start_pipeline.sh - Lance tout le pipeline automatiquement

echo "============================================================"
echo "🚀 DÉMARRAGE DU PIPELINE COMPLET"
echo "============================================================"

# ============================================================
# 1. Vérifier que Kafka tourne
# ============================================================
echo ""
echo "📡 [1/6] Vérification de Kafka..."

if docker ps | grep -q "kafka_pfe"; then
    echo "   ✅ Kafka est déjà démarré"
else
    echo "   ⚠️ Kafka n'est pas démarré. Démarrage..."
    cd ~/projet_telecom/kafka_server
    bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
    sleep 3
    bin/kafka-server-start.sh -daemon config/server.properties
    sleep 5
    echo "   ✅ Kafka démarré"
fi

# ============================================================
# 2. Vérifier MongoDB
# ============================================================
echo ""
echo "📂 [2/6] Vérification de MongoDB..."

if docker ps | grep -q "mongodb_pfe"; then
    echo "   ✅ MongoDB est déjà démarré"
else
    echo "   ⚠️ MongoDB n'est pas démarré. Démarrage..."
    docker start mongodb_pfe
    sleep 3
    echo "   ✅ MongoDB démarré"
fi

# ============================================================
# 3. Nettoyer les anciens logs
# ============================================================
echo ""
echo "🧹 [3/6] Nettoyage des anciens logs..."
rm -f /tmp/kafka_producer.log
rm -f /tmp/kafka_consumer.log
rm -f /tmp/dashboard.log
echo "   ✅ Logs nettoyés"

# ============================================================
# 4. Lancer le Producer Kafka (en arrière-plan)
# ============================================================
echo ""
echo "📤 [4/6] Lancement du Kafka Producer..."
cd ~/projet_telecom
nohup python3 /home/mouna/projet_telecom/Kafka/kafka_producer.py > /tmp/kafka_producer.log 2>&1 &
PRODUCER_PID=$!
echo "   ✅ Producer démarré (PID: $PRODUCER_PID)"
sleep 2

# ============================================================
# 5. Lancer le Consumer Kafka (en arrière-plan)
# ============================================================
echo ""
echo "📥 [5/6] Lancement du Kafka Consumer..."
cd ~/projet_telecom
nohup python3 /home/mouna/projet_telecom/Kafka/kafka_consumer.py > /tmp/kafka_consumer.log 2>&1 &
CONSUMER_PID=$!
echo "   ✅ Consumer démarré (PID: $CONSUMER_PID)"
sleep 2

# ============================================================
# 6. Lancer le Dashboard (en arrière-plan)
# ============================================================
echo ""
echo "📊 [6/6] Lancement du Dashboard Streamlit..."
cd ~/projet_telecom
nohup streamlit run /home/mouna/projet_telecom/Kafka/dasbord_streamlit.py --server.port 8503 > /tmp/dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo "   ✅ Dashboard démarré (PID: $DASHBOARD_PID)"
sleep 3

# ============================================================
# RÉSUMÉ
# ============================================================
echo ""
echo "============================================================"
echo "🎉 PIPELINE COMPLET DÉMARRÉ !"
echo "============================================================"
echo ""
echo "   📍 URLs d'accès :"
echo "      Dashboard: http://localhost:8502"
echo "      Kafka UI: http://localhost:8088"
echo ""
echo "   📍 PIDs des processus :"
echo "      Producer: $PRODUCER_PID"
echo "      Consumer: $CONSUMER_PID"
echo "      Dashboard: $DASHBOARD_PID"
echo ""
echo "   📍 Logs :"
echo "      Producer: tail -f /tmp/kafka_producer.log"
echo "      Consumer: tail -f /tmp/kafka_consumer.log"
echo "      Dashboard: tail -f /tmp/dashboard.log"
echo ""
echo "   🛑 Pour arrêter : ./stop_pipeline.sh"
echo "============================================================"

# Sauvegarder les PIDs
echo "$PRODUCER_PID" > /tmp/pipeline_pids.txt
echo "$CONSUMER_PID" >> /tmp/pipeline_pids.txt
echo "$DASHBOARD_PID" >> /tmp/pipeline_pids.txt