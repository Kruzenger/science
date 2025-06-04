from dataclasses import dataclass
from enum import Enum
from typing import TypeVar, Generic

class ECommandTargetTypes(Enum):
    KAFKA_ADMIN = 0,
    FETCHER_ADMIN = 1

T = TypeVar('T')

@dataclass
class Command:
    target_type: ECommandTargetTypes
    content: Generic[T]