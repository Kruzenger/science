from Utils.Singleton import Singleton
from Commands.Command import Command, ECommandTargetTypes
from Commands.KafkaAdminCommand import KafkaAdminCommandContent
from Commands.FetcherAdminCommand import FetcherAdminCommandContent
from dataclasses import asdict
from Logger import Logger
import dacite
import json

log = Logger()

class CommandEncoder(Singleton):
    def encodeCommandToJSON(self, command: Command):
        return json.dumps(asdict(command), ensure_ascii=False).encode(encoding="utf-8")

    def decodeCommandFromJSON(self, raw_command: str):
        structured_command = json.loads(raw_command.decode("utf-8"))
        match structured_command["target_type"]:
            case ECommandTargetTypes.KAFKA_ADMIN.value:
                command = dacite.from_dict(data_class=Command[KafkaAdminCommandContent], data=structured_command)
            case ECommandTargetTypes.FETCHER_ADMIN.value:
                command = dacite.from_dict(data_class=Command[FetcherAdminCommandContent], data=structured_command)
            case _:
                command = None
        return command
