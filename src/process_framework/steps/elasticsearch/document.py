from pydantic import BaseModel, ConfigDict
from abc import ABC
from typing import Any, Iterable

#TODO: update 'field:type = Field(serialization_alias='field') note and example
"""
A few of these implement a pattern of `field:type = Field(serialization_alias='_field', alias='_field')`
this is becauase the elasticsearch data model for LHH escapes a few reserved words (_type, _id) with underscores
but pydantic treats underscored fields as private and doesn't serialize them
we can work around this by modelling the fields as un-underscored, but serializing them by alias
"""

class Document(BaseModel, ABC):
    """ Documents must implement the `_id` abstract, computed property """
    model_config=ConfigDict(serialize_by_alias=True)

    def _get_id(self) -> Any:
        """ get an `id` for the Document:
                returns `id` if it is a field in the doc
                else raises a ValueError
            this should be overridden if `id` is not a field in the doc """
        # override this in derived classes if those classes don't implement `id` as a doc field
        try:
            return getattr(self, 'id')
        except:
            raise ValueError("Document does not contain a value for `id` and does not override `get_id`")


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
        _source = self._get_source()
        return dict(
            _index=index,
            _op_type='index',
            _id=self._get_id(),
            _source=_source
        )
    
    
    @staticmethod
    def gen_bulk_index_actions(index:str, documents: Iterable['Document']) -> Iterable[dict]:
        """ gennerate an Iterable of bulk index actions for an iterable of `Document`s """
        for doc in documents:
            yield doc.get_bulk_index_action(index)

    
    def get_index_action(self) -> dict:
        """ get an index of kwargs that can be passed to elasticsearch.index """
        _source = self._get_source()
        return dict(
            id=self._get_id(),
            document=_source
        )
    