from kafka import KafkaProducer
from config import log
import json

class KafkaManager:
    def __init__(self):
        self.kafka_producer: KafkaProducer = KafkaProducer(api_version=(0, 11))
        if(self.kafka_producer.bootstrap_connected()):
            log.debug(f"Kafka producer connected")

    def send(self, topic_name:str, data):
        if topic_name not in self.kafka_admin_client.list_topics():
            print(self.createTopic(topic_name=topic_name))
        self.kafka_producer.send(topic=topic_name, value=json.dumps(data, ensure_ascii=False).encode(encoding="utf-8"))

