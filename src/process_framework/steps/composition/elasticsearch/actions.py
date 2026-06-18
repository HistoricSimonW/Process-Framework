
from typing import Any, Iterable
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, computed_field
from enum import Enum
from .document import DocumentBase

class OpType(str, Enum):
    INDEX = "index"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class BulkAction(BaseModel, ABC):
    """ https://elasticsearch-py.readthedocs.io/en/v7.12.0/helpers.html#bulk-helpers """
    index: str = Field(alias="_index")
    id: Any = Field(alias="_id")

    @computed_field(alias='_op_type')
    @property
    @abstractmethod
    def op_type(self) -> OpType:
        ...


class IndexAction(BulkAction):
    """ a bulk index action\n
    https://elasticsearch-py.readthedocs.io/en/v7.12.0/helpers.html#bulk-helpers """
    pipeline:str|None=None
    source: dict[str, Any] = Field(alias="_source")
    
    @property
    def op_type(self) -> OpType:
        return OpType.INDEX

    @staticmethod
    def from_document(document:DocumentBase, index:str) -> 'IndexAction':
        source = document._dump()
        return IndexAction(
            _index=index,
            _id=source.pop('_id'),  # required by bulk action; raise if absent
            _source=source
        )
    
    
    @staticmethod
    def from_documents(documents: Iterable[DocumentBase], index:str) -> Iterable['dict']:
        for doc in documents:
            yield IndexAction.from_document(doc, index).model_dump(exclude_none=True, by_alias=True)
            
    
class UpdateAction(BulkAction):
    """ a bulk update action\n
    https://elasticsearch-py.readthedocs.io/en/v7.12.0/helpers.html#bulk-helpers """
    doc:dict
    
    @property
    def op_type(self) -> OpType:
        return OpType.UPDATE
    
    @staticmethod
    def from_document(document:DocumentBase, index:str) -> 'UpdateAction':
        source = document._dump()
        return UpdateAction(
            _index=index,
            _id=source.pop('_id'),  # required by bulk action; raise if absent
            doc=source
        )
    
    @staticmethod
    def from_documents(documents: Iterable[DocumentBase], index:str) -> Iterable['dict']:
        for doc in documents:
            yield UpdateAction.from_document(doc, index).model_dump(exclude_none=True)


class DeleteAction(BulkAction):
    """ a bulk delete action\n
    https://elasticsearch-py.readthedocs.io/en/v7.12.0/helpers.html#bulk-helpers """
    @property
    def op_type(self) -> OpType:
        return OpType.DELETE
    
    @staticmethod
    def from_document(document:DocumentBase, index:str) -> 'DeleteAction':
        return DeleteAction(
            _index=index,
            _id=document._id    # required by bulk action; raise if absent
        )
    
    @staticmethod
    def from_documents(documents: Iterable[DocumentBase], index:str) -> Iterable['dict']:
        for doc in documents:
            yield DeleteAction.from_document(doc, index).model_dump(exclude_none=True)