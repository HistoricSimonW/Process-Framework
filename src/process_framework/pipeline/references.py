# stdlib
from abc import ABC
from dataclasses import dataclass, fields


@dataclass
class ReferencesBase(ABC):

    def preflight(self) -> None:
        for field in fields(self):
            if getattr(self, field.name) is None:
                raise ValueError(f"Required reference {field} is not assigned")
