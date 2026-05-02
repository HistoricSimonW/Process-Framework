# stdlib
from abc import ABC
from dataclasses import dataclass
from .composition.core import ContainerBase

@dataclass
class ReferencesBase(ContainerBase, ABC):
    """base class for reference containers holding pipeline state."""