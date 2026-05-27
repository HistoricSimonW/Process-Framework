# stdlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from .composition.core import ContainerBase
from .settings import SettingsBase
from typing import Self

@dataclass
class ClientsBase[TSettings:SettingsBase](ContainerBase, ABC):
    """base class for client containers holding external service connections."""

    @abstractmethod
    @classmethod
    def initialize(cls, settings:TSettings) -> Self:
        ...

    
class EmptyClients(ClientsBase[SettingsBase]):
    """marker class for empty client containers."""
    
    @classmethod
    def initialize(cls, settings: SettingsBase) -> Self:
        return cls()