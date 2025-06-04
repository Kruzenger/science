from dataclasses import dataclass
from enum import Enum

class EKafkaAdminCommandType(Enum):
    CREATE_TOPIC = 0

@dataclass
class KafkaAdminCommandContent:
    @dataclass
    class TopicParametrs:
        num_partitions: int
        replication_factor: int

    command_type: EKafkaAdminCommandType
    id: int
    topics_names: list
    topics_parametrs: dict[str, TopicParametrs]

    def getTopicParametrs(self, name):
        return self.topics_parametrs.get(name)
