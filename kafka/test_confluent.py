# test_confluent.py
from confluent_kafka import Producer, KafkaException
import json, time

def wait_for_kafka(broker, retries=15, delay=3):
    print(f"⏳ Attente de Kafka sur {broker}...")
    for i in range(retries):
        try:
            p = Producer({'bootstrap.servers': broker,
                          'socket.timeout.ms': 3000,
                          'message.timeout.ms': 3000})
            p.list_topics(timeout=3)
            print(f"✅ Kafka prêt après {i * delay}s !")
            return True
        except Exception as e:
            print(f"   tentative {i+1}/{retries} — pas encore prêt...")
            time.sleep(delay)
    return False

broker = 'localhost:9092'

if not wait_for_kafka(broker):
    print("❌ Kafka n'a pas démarré à temps.")
    exit(1)

conf = {
    'bootstrap.servers': broker,
    'client.id': 'python-producer',
    'message.timeout.ms': 10000
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'❌ Erreur: {err}')
    else:
        print(f'✅ Message envoyé ! Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}')

print("📤 Envoi du message...")
producer.produce(
    'pipeline_events',
    json.dumps({'test': 'Hello moun', 'timestamp': time.time()}).encode('utf-8'),
    callback=delivery_report
)
producer.flush()
print("✅ Terminé !")