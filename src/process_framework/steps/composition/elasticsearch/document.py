from pydantic import BaseModel
from typing import Any
from abc import ABC
from pydantic import Field
from .actions import IndexAction, UpdateAction

class DocumentBase(BaseModel, ABC):
    id: Any = Field(alias="_id")

    def _dump(self) -> dict:
        return self.model_dump(by_alias=True, exclude_none=True)
    

    def get_index_action(self, index:str, pipeline:str|None=None) -> IndexAction:
        dump = self._dump()
        _id = dump.pop('_id')
        return IndexAction(
            _index=index,
            _id=_id,
            pipeline=pipeline,
            _source=dump
        )
    

    def get_update_action(self, index:str) -> UpdateAction:
        dump = self._dump()
        _id = dump.pop('_id')
        return UpdateAction(
            _index=index,
            _id=_id,
            doc=dump
        )
