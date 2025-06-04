import dacite
from utils import convertFromJSONToDict
from Commands.Command import Command, ECommandTargetTypes
from Commands.KafkaAdminCommand import KafkaAdminCommandContent
from Commands.FetcherAdminCommand import FetcherAdminCommandContent

class CommandParcer:
    def __init__(self):
        pass

    def parceCommand(self, raw_command):
        structured_command = convertFromJSONToDict(raw_command)
        match structured_command["target_type"]:
            case ECommandTargetTypes.KAFKA_ADMIN:
                command = dacite.from_dict(data_class=Command[KafkaAdminCommandContent], data=structured_command)
            case ECommandTargetTypes.FETCHER_ADMIN:
                command = dacite.from_dict(data_class=Command[FetcherAdminCommandContent], data=structured_command)
        return command
                