from Commands.CommandFactory import CommandFactory
from Commands.CommandEncoder import CommandEncoder
from Logger import Logger

log = Logger()

CMD_FACTORY = CommandFactory()
CMD_ENCODER = CommandEncoder()

target_groups_ids: list = ["public192867633", 
                           "lovelycosplay", 
                           "miptdndleague", 
                           "stfpmi", 
                           "anifox", 
                           "fpmi_edu", 
                           "reddit", 
                           "twinsrpg", 
                           "marsbgame", 
                           "best_rpg_games", 
                           "starcraft_memes", 
                           "dit2225", 
                           "lore_dnd", 
                           "fpmi_students", 
                           "kleientertainment", 
                           "anime", 
                           "knigiprint", 
                           "cpp_lib", 
                           "resf"]

DEFAULT_TOPIC_PREFIX: str = ""
KAFKA_ADMIN_COMMAND_TOPIC_NAME: str = "kafka_admin_commands"
FETCHER_ADMIN_COMMAND_TOPIC_NAME: str = "fetcher_admin_commands"