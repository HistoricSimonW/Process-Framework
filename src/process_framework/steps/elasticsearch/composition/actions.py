from abc import ABC, abstractmethod
from collections.abc import Iterable
from enum import Enum
from typing import Any, Self

from pydantic import BaseModel, Field, computed_field

from .document import DocumentBase


class OpType(str, Enum):
    INDEX = "index"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class BulkAction(BaseModel, ABC):
    """Base Elasticsearch bulk action."""

    index: str = Field(alias="_index")
    id: Any = Field(alias="_id")

    @computed_field(alias="_op_type")
    @property
    @abstractmethod
    def op_type(self) -> OpType:
        ...

    def _dump(self) -> dict[str, Any]:
        """Serialize the action using Elasticsearch field names."""
        return self.model_dump(exclude_none=True, by_alias=True)

    @classmethod
    @abstractmethod
    def from_document(cls, document: DocumentBase, index: str) -> Self:
        """Construct an action from a document."""
        ...

    @classmethod
    def from_documents(cls, documents: Iterable[DocumentBase], index: str, **kwargs: Any) -> Iterable[dict[str, Any]]:
        """Generate bulk actions from documents."""
        for document in documents:
            yield cls.from_document(document, index, **kwargs)._dump()


class IndexAction(BulkAction):
    """Elasticsearch bulk index action."""
    source: dict[str, Any] = Field(alias="_source")

    @property
    def op_type(self) -> OpType:
        return OpType.INDEX

    @classmethod
    def from_document(cls, document: DocumentBase, index: str) -> Self:
        """Construct an index action from a document."""
        source = document._dump()

        return cls(
            _index=index,
            _id=source.pop("_id"),
            _source=source,
        )


class UpdateAction(BulkAction):
    """Elasticsearch bulk partial-update action."""

    doc: dict[str, Any]
    doc_as_upsert: bool = False

    @property
    def op_type(self) -> OpType:
        return OpType.UPDATE

    @classmethod
    def from_document(cls, document: DocumentBase, index: str, *, doc_as_upsert:bool = False) -> Self:
        """Construct a partial-update action from a document."""
        source = document._dump()

        return cls(
            _index=index,
            _id=source.pop("_id"),
            doc=source,
            doc_as_upsert=doc_as_upsert
        )


class DeleteAction(BulkAction):
    """Elasticsearch bulk delete action."""

    @property
    def op_type(self) -> OpType:
        return OpType.DELETE

    @classmethod
    def from_document(cls, document: DocumentBase, index: str) -> Self:
        """Construct a delete action from a document."""
        return cls(
            _index=index,
            _id=document._id,
        )
        
    @classmethod
    def from_ids(cls, ids:Iterable[str], index:str) -> Iterable[dict[str, Any]]:
        for id in ids:
            yield cls(_id=id, _index=index)._dump() 