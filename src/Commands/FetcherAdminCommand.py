from dataclasses import dataclass
from enum import Enum

class EFetcherAdminCommandType(Enum):
    ADD_GROUP_TO_OBSERVE = 0
    ADD_API_TOKEN = 1

class ESocialMediaType(Enum):
    VK = 0
    Twitter = 1
    LinkedIn = 2

@dataclass
class FetcherAdminCommandContent:
    command_type: int
    social_media_type: int
    groups: list[str]
    APIToken: str

    def __init__(self):
        pass