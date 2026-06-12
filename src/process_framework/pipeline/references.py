# stdlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from .composition.core import ContainerBase

@dataclass(kw_only=True)
class ReferencesBase(ContainerBase, ABC):
    """base class for reference containers holding pipeline state."""
    ...


class ReferencesDefinitionBase[TReferences:ReferencesBase](ABC):
    """ Constructs and wires a concrete ReferencesBase instance. Implementations should return a fully initialized reference graph. """
    
    @classmethod
    @abstractmethod
    def instantiate(cls) -> TReferences:
        ...