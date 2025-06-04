from dataclasses import dataclass
from enum import Enum

class EFetcherAdminCommandType(Enum):
    ADD_GROUP_TO_OBSERVE = 0

@dataclass
class FetcherAdminCommandContent:
    command_type: EFetcherAdminCommandType