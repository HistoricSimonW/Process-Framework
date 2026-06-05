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

    
    def _get_source(self, exclude_none:bool=True, **kwargs) -> dict:
        """ get the json `source` of this doc, ready to be returned as part of an `index` or `bulk_index` action
            this pops `id`, if it's part of the doc; it should be handled separately """
        _source = self.model_dump(
            mode='json',
            exclude_none=exclude_none,
            **kwargs
        )
        _source.pop('id', None) # remove `id` if it's present
        return _source
    

    def get_bulk_index_action(self, index:str) -> dict:
        """ get a dict that can be passed to the elasticsearch.helpers.bulk api """
        return dict(
            _index=index,
            _op_type='index',
            _id=self._id,
            _source=self._get_source()
        )
    
    
    @staticmethod
    def gen_bulk_index_actions[T:'DocumentBase'](index:str, documents: Iterable[T]) -> Iterable[dict]:
        """ gennerate an Iterable of bulk index actions for an iterable of `Document`s """
        for doc in documents:
            yield doc.get_bulk_index_action(index)


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



#TODO: update 'field:type = Field(serialization_alias='field') note and example
"""
A few of these implement a pattern of `field:type = Field(serialization_alias='_field', alias='_field')`
this is becauase the elasticsearch data model for LHH escapes a few reserved words (_type, _id) with underscores
but pydantic treats underscored fields as private and doesn't serialize them
we can work around this by modelling the fields as un-underscored, but serializing them by alias
"""
    