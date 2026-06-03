from pydantic import BaseModel
from typing import Any, Self, Iterable, ClassVar, Mapping
from abc import ABC
from pydantic import Field, computed_field, model_validator
from process_framework.steps.composition.elasticsearch.actions import IndexAction, UpdateAction

class DocumentBase(BaseModel, ABC):
    """Base model for documents indexed into Elasticsearch."""
    # if this is set, use its values as `_id``
    id: Any|None = Field(default=None, alias="_id")     
    # if this is set, use the field it identifies to get the value of `_id`
    _id_field: ClassVar[str | None] = None              

    @computed_field
    @property
    def _id(self) -> Any:
        """Return the explicit ID or derive it from `_id_field`."""
        if self.id is not None:
            return self.id
        
        if self._id_field is not None:
            return getattr(self, self._id_field)
        
        return None
        

    def _dump(self) -> dict:
        """Dump the document for Elasticsearch operations."""
        return self.model_dump(by_alias=True, exclude_none=True)
    

    def get_index_action(self, index:str, pipeline:str|None=None) -> IndexAction:
        """Build an index action for this document."""
        dump = self._dump()
        _id = dump.pop('_id')
        return IndexAction(
            _index=index,
            _id=_id,
            pipeline=pipeline,
            _source=dump
        )
    

    def get_update_action(self, index:str) -> UpdateAction:
        """Build a partial update action for this document."""
        dump = self._dump()
        _id = dump.pop('_id')
        return UpdateAction(
            _index=index,
            _id=_id,
            doc=dump
        )
    

    @model_validator(mode="after")
    def validate_id(self) -> "DocumentBase":
        """Require the document to resolve to a non-null ID."""
        if self._id is None:
            raise ValueError(
                "Document must define either `id`/_id "
                "or a populated field referenced by `_id_field`"
            )

        return self
    

    @classmethod
    def from_record(cls, record:Mapping[str, Any]) -> Self:
        """Create a document instance from a record mapping."""
        return cls.model_validate(dict(record))
    

    @classmethod
    def gen_documents_from_records(cls, records:Iterable[Mapping[str, Any]]) -> Iterable[Self]:
        """Yield document instances from an iterable of records."""
        for record in records:
            yield cls.from_record(record)


if __name__ == '__main__':
    # demo a class with a defined ID
    print('a doc with an explicit ID')
    class WithExplicitId(DocumentBase):
        name:str

    explicit = WithExplicitId(
        _id='explicit',
        name='test'
    )

    print(explicit)
    print(explicit.model_dump(exclude_unset=True))
    
    # demo a class with an _id_field
    print('a doc with an inplicit ID')
    class WithLabelledId(DocumentBase):
        code:str
        _id_field='code'

    implicit = WithLabelledId(code='1121')
    print(implicit)
    print(implicit.model_dump(exclude_unset=True))
