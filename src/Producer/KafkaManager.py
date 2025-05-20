from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from config import log
import json

class KafkaManager:
    def __init__(self):
        self.kafka_admin_client: KafkaAdminClient = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')
        self.kafka_producer: KafkaProducer = KafkaProducer(api_version=(0, 11))
        if(self.kafka_producer.bootstrap_connected()):
            log.debug(f"Kafka producer connected")

    def createTopics(self, topic_names):
        topic_list = []
        for topic_name in topic_names:
            topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
        self.kafka_admin_client.create_topics(new_topics=topic_list, validate_only=False)

    def createTopic(self, topic_name):
        self.kafka_admin_client.create_topics(new_topics=[NewTopic(name=topic_name, num_partitions=1, replication_factor=1)], validate_only=False)

    def send(self, topic_name:str, data):
        if topic_name not in self.kafka_admin_client.list_topics():
            print(self.createTopic(topic_name=topic_name))
        self.kafka_producer.send(topic=topic_name, value=json.dumps(data, ensure_ascii=False).encode(encoding="utf-8"))

