# stdlib
from abc import ABC
from dataclasses import dataclass
from .composition.core import ContainerBase

@dataclass
class ClientsBase(ContainerBase, ABC):
    """base class for client containers holding external service connections."""