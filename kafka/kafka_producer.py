# kafka_producer.py
from confluent_kafka import Producer
import json
from datetime import datetime
import socket

class KafkaPipelineProducer:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})
        self.hostname = socket.gethostname()
        print("✅ Producteur Kafka connecté")
    
    def send_event(self, event_type, data):
        message = {
            "event_type": event_type,
            "timestamp": datetime.now().isoformat(),
            "hostname": self.hostname,
            "data": data
        }
        self.producer.produce('pipeline_events', json.dumps(message).encode('utf-8'))
        self.producer.flush()
        print(f"📨 [{datetime.now().strftime('%H:%M:%S')}] {event_type} - OK")
        return True

producer = KafkaPipelineProducer()