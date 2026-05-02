from pydantic import BaseModel
from ..settings import CliArg
from typing import Annotated
from abc import ABC
from dataclasses import dataclass, fields
from argparse import BooleanOptionalAction

class CrudArgs(BaseModel):
    """ creations, updates, deletions, with CliArgs """
    creations:Annotated[bool, CliArg('--creations', '-c', action=BooleanOptionalAction)]
    updates:Annotated[bool, CliArg('--updates', '-u', action=BooleanOptionalAction)]
    deletions:Annotated[bool, CliArg('--deletions', '-d', action=BooleanOptionalAction)]


@dataclass
class ContainerBase(ABC):
    """base class for containers requiring all fields to be assigned."""
    def preflight(self) -> None:
        for field in fields(self):
            if getattr(self, field.name) is None:
                raise ValueError(f"required field {field.name} is not assigned")