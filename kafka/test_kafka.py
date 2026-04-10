# # test_kafka.py
# from kafka_producer import producer

# # Envoyer un message test
# producer.send_event("test", {
#     "message": "Hello Kafka !",
#     "status": "ok"
# })

# print("✅ Message envoyé !")

# test_kafka_direct.py
from kafka import KafkaProducer
import json

print("1️⃣ Création du producer...")
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("2️⃣ Envoi du message...")
future = producer.send('pipeline_events', {'test': 'Hello Python direct'})

print("3️⃣ Attente du résultat...")
result = future.get(timeout=10)
print(f"4️⃣ Message envoyé ! Partition: {result.partition}, Offset: {result.offset}")

producer.flush()
print("5️⃣ Terminé !")