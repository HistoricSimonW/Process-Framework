# stdlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self
from .composition.core import ContainerBase

@dataclass
class ReferenceGraphBase(ContainerBase, ABC):
    """base class for reference containers holding pipeline state."""

    @abstractmethod
    @classmethod
    def initialize(cls) -> Self:
        """ construct a wired-up instance of `cls` """
        ...