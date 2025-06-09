from kafka.admin import KafkaAdminClient, NewTopic
from kafka.consumer import KafkaConsumer
from Commands.Command import Command
from Commands.KafkaAdminCommand import KafkaAdminCommandContent, EKafkaAdminCommandType 
from config import log, CMD_ENCODER
import config

class KafkaAdminManager:
    __command: Command[KafkaAdminCommandContent]

    def __init__(self):
        self.kafka_admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='admin')
        self.__initCommandTopic()
        self.__startReadingCommands()

    def __initCommandTopic(self):
        self.__createTopics({NewTopic(name=config.KAFKA_ADMIN_COMMAND_TOPIC_NAME, num_partitions=1, replication_factor=1)})
        self.__initCommandConsumer()

    def __initCommandConsumer(self):
        self.kafka_consumer = KafkaConsumer(config.KAFKA_ADMIN_COMMAND_TOPIC_NAME)
        if(self.kafka_consumer.bootstrap_connected()):
            log.debug(f"kafka consumer bootstrap connection succeed. Topic: {config.KAFKA_ADMIN_COMMAND_TOPIC_NAME}")
        else: 
            log.error(f"kafka consumer bootstrap connection failed. Topic: {config.KAFKA_ADMIN_COMMAND_TOPIC_NAME}")

    def __startReadingCommands(self):
        for msg in self.kafka_consumer:
            log.debug(f"Got command {msg.value}.\n Starting decoding")
            self.__command = CMD_ENCODER.decodeCommandFromJSON(msg.value)
            if(self.__command != None):
                self.__processCommand()
            else:
                log.error(f"Command {msg.value} is incorrect. Dropping")

    def __processCommand(self):
        match self.__command.content.command_type:
            case EKafkaAdminCommandType.CREATE_TOPIC.value:
                log.debug(f"Decoded CREATE_TOPICS command. Starting processing")
                topics = self.__prepareTopics()
                self.__createTopics(topics)
    
    def __prepareTopics(self):
        topics = []
        for name in self.__command.content.topics_names:
            parameters = self.__command.content.getTopicParameters(name)
            if(parameters is not None):
                topics.append(NewTopic(name=name, num_partitions=parameters.num_partitions, replication_factor=parameters.replication_factor))
            else:
                log.error(f"Can't create topic {name}: No parameters found")
        return topics
    
    def __createTopics(self, topics):
        if(len(topics) > 0):
            self.kafka_admin_client.create_topics(new_topics=topics, validate_only=False)
        else:
            log.debug("List of topics for creation is empty, skipping")