from kafka.admin import KafkaAdminClient, NewTopic
from kafka.consumer import KafkaConsumer
import KafkaAdminService.config as config
from config import log
from Commands.CommandParcer import CommandParcer, Command
from Commands.KafkaAdminCommand import KafkaAdminCommandContent, EKafkaAdminCommandType 

class KafkaAdminManager:
    def __init__(self):
        self.kafka_admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='admin')
        self.initCommandTopic()
        self.command_parcer = CommandParcer()
        self.readCommands()

    def initCommandTopic(self):
        self.kafka_admin_client.create_topics({NewTopic(name=config.COMMAND_TOPIC_NAME, num_partitions=1, replication_factor=1)})
        self.initCommandConsumer()

    def initCommandConsumer(self):
        self.kafka_consumer = KafkaConsumer(config.COMMAND_TOPIC_NAME)
        if(self.kafka_consumer.bootstrap_connected()):
            log.debug(f"kafka admin connected to topic {config.COMMAND_TOPIC_NAME}")
        else: 
            log.error(f"kafka admin failed to connect to topic {config.COMMAND_TOPIC_NAME}")

    def readCommands(self):
        for msg in self.kafka_consumer:
            command: Command[KafkaAdminCommandContent] = self.command_parcer.parceMessage(msg)
            self.processCommand(command)

    
    def processCommand(self, command):
        match command.content.command_type:
            case EKafkaAdminCommandType.CREATE_TOPIC.value:
                self.createTopics(self.prepareTopics(command.content))
                

    def createTopics(self, topics):
        self.kafka_admin_client.create_topics(new_topics=topics, validate_only=False)
    
    def prepareTopics(self, content:KafkaAdminCommandContent):
        topics = []
        for name in content.topics_names:
            parametrs = content.getTopicParametrs(name)
            if(parametrs is not None):
                topics.append(NewTopic(name=name, num_partitions=parametrs.num_partitions, replication_factor=parametrs.replication_factor))
            else:
                log.error(f"Can't create topic {name}: No parametrs found")
        return topics