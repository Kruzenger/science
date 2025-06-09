from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
from Commands.Command import Command
from Commands.FetcherAdminCommand import FetcherAdminCommandContent
from config import log, CMD_FACTORY, CMD_ENCODER
import config


import time

class FetcherAdminManager:
    __command: Command[FetcherAdminCommandContent]

    def __init__(self):
        self.__initKafkaProducer()
        self.__initCommandTopic()
        self.__startReadingCommands()

    def __initKafkaProducer(self):
        self.kafka_producer = KafkaProducer()
        if(self.kafka_producer.bootstrap_connected()):
            log.debug(f"kafka producer bootstrap connection succeed")
        else: 
            log.error(f"kafka producer bootstrap connection failed")

    def __initCommandTopic(self):
        command = CMD_FACTORY.createKafkaCreateTopicCommand(
            topics_names=list([config.FETCHER_ADMIN_COMMAND_TOPIC_NAME]), 
            topics_parameters=dict({config.FETCHER_ADMIN_COMMAND_TOPIC_NAME: [1, 1]})
            )
        self.__sendCommand(config.KAFKA_ADMIN_COMMAND_TOPIC_NAME, command)
        self.__initCommandConsumer()

    def __initCommandConsumer(self):
        self.kafka_consumer = KafkaConsumer(config.FETCHER_ADMIN_COMMAND_TOPIC_NAME)
        if(self.kafka_consumer.bootstrap_connected()):
            log.debug(f"kafka consumer bootstrap connection succeed. Topic: {config.FETCHER_ADMIN_COMMAND_TOPIC_NAME}")
        else: 
            log.error(f"kafka consumer bootstrap connection failed. Topic: {config.FETCHER_ADMIN_COMMAND_TOPIC_NAME}")

    def __startReadingCommands(self):
        for msg in self.kafka_consumer:
            self.__command = CMD_ENCODER.decodeCommandFromJSON(msg)
            self.__sendCommandToCorrespondingManager()

    def __sendCommandToCorrespondingManager(self):
        topic = self.__getCommandSocialMediaManagerKafkaTopic()
        self.__sendCommand(topic, self.__command)

    def __getCommandSocialMediaManagerKafkaTopic(self):
        return self.__command.content.social_media_type.name + "_manager_commands"

    def __sendCommand(self, topic, command):
        self.kafka_producer.send(topic=topic, value=CMD_ENCODER.encodeCommandToJSON(command))

    
