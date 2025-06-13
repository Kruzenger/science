from Utils.Singleton import Singleton
from Commands.Command import Command, ECommandTargetTypes
from Commands.KafkaAdminCommand import KafkaAdminCommandContent
from Commands.FetcherAdminCommand import FetcherAdminCommandContent
from dataclasses import asdict
from Utils.Logger import Logger
import dacite
import json

log = Logger()

class Encoder(metaclass=Singleton):
    def encodeCommandToJSON(self, command: Command):
        return self.encodeData(asdict(command))


    def decodeCommandFromJSON(self, raw_command: str):
        structured_command = self.decodeData(raw_command)
        match structured_command["target_type"]:
            case ECommandTargetTypes.KAFKA_ADMIN.value:
                command = dacite.from_dict(data_class=Command[KafkaAdminCommandContent], data=structured_command)
            case ECommandTargetTypes.FETCHER_ADMIN.value:
                command = dacite.from_dict(data_class=Command[FetcherAdminCommandContent], data=structured_command)
            case _:
                command = None
        return command
    

    def encodeData(self, data):
        return json.dumps(data, ensure_ascii=False).encode(encoding="utf-8")
    
    def decodeData(self, data):
        return json.loads(data.value.decode("utf-8"))
