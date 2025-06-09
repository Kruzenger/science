from kafka import KafkaConsumer
from config import log

def main():
    consumer = KafkaConsumer('public192867633')
    for msg in consumer:
        text = msg.value.decode("utf-8")
        log.debug(text)

main()