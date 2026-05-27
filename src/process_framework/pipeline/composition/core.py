from pydantic import BaseModel
from ..settings import CliArg
from typing import Annotated
from abc import ABC
from dataclasses import dataclass, fields
from argparse import BooleanOptionalAction

def mask(value: str, chars:int=4) -> str:
    if len(value) <= chars*2:
        return "*" * len(value)

    return f"{value[:chars]}***{value[-chars:]}"


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
            value = getattr(self, field.name)

            if value is None:
                raise ValueError(
                    f"required field {field.name} is not assigned"
                )

            if isinstance(value, ContainerBase):
                value.preflight()