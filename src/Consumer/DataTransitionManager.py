from KafkaManager import KafkaManager

class DataTransitionManager:
    def __init__(self):
        self.kafka_manager = KafkaManager()


DATA_TRN_MNG = DataTransitionManager()