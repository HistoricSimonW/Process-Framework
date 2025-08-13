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

    def get_id(self) -> Any:
        # overwrite this
        _id = getattr(self, 'id', None)
        assert _id is not None
        return _id


    def get_bulk_index_action(self, index:str) -> dict:
        """ get a dict that can be passed to the elasticsearch.helpers.bulk api """
        _source = self.model_dump()
        _source.pop('id', None)
        return dict(
            _index=index,
            _op_type='index',
            _id=self.get_id(),
            _source=_source
        )
    
    
    @staticmethod
    def gen_bulk_index_actions(index:str, documents: Iterable['Document']):
        for doc in documents:
            yield doc.get_bulk_index_action(index)

    
    def get_index_action(self) -> dict:
        """ get an index of kwargs that can be passed to elasticsearch.index """
        source = self.model_dump()
        source.pop('id', None)
        return dict(
            id=self.get_id(),
            document=source
        )
    