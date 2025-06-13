from Utils.Singleton import Singleton
from Commands.KafkaAdminCommand import KafkaAdminCommandContent, EKafkaAdminCommandType
from Commands.FetcherAdminCommand import FetcherAdminCommandContent, EFetcherAdminCommandType, ESocialMediaType
from Commands.Command import Command, ECommandTargetTypes

class CommandCreator(metaclass=Singleton):
    __last_id: int = 0

    @property
    def LastId(self):
        self.__last_id += 1
        return self.__last_id

    @LastId.setter
    def LastId(self, value):
        self.__last_id = value

    def createKafkaCreateTopicCommand(self, topics_names: list, topics_parameters: dict[str, KafkaAdminCommandContent.TopicParameters]):
        return Command[KafkaAdminCommandContent](
            ECommandTargetTypes.KAFKA_ADMIN.value,
            KafkaAdminCommandContent(
                EKafkaAdminCommandType.CREATE_TOPIC.value,
                self.LastId,
                topics_names,
                topics_parameters
                )
            )
        
