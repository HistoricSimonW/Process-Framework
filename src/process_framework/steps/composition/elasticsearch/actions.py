
from typing import Any
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, computed_field
from enum import Enum

class OpType(str, Enum):
    INDEX = "index"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class BulkAction(BaseModel, ABC):
    index: str = Field(alias="_index")
    id: Any = Field(alias="_id")

    @computed_field(alias='_op_type')
    @property
    @abstractmethod
    def op_type(self) -> OpType:
        ...


class IndexAction(BulkAction):
    pipeline:str|None=None
    source: dict[str, Any] = Field(alias="_source")
    @property
    def op_type(self) -> OpType:
        return OpType.INDEX


class UpdateAction(BulkAction):
    doc:dict
    @property
    def op_type(self) -> OpType:
        return OpType.UPDATE


class DeleteAction(BulkAction):
    @property
    def op_type(self) -> OpType:
        return OpType.DELETE