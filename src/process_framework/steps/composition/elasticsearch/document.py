from pydantic import BaseModel
from typing import Any, Self, Iterable, ClassVar
from abc import ABC
from pydantic import Field, computed_field
from process_framework.steps.composition.elasticsearch.actions import IndexAction, UpdateAction

class DocumentBase(BaseModel, ABC):
    id: Any|None = Field(default=None, alias="_id")
    
    _id_field: ClassVar[str | None] = None

    @computed_field
    @property
    def _id(self) -> Any:
        if self.id is not None:
            return self.id
        
        if self._id_field is not None:
            return getattr(self, self._id_field)
        

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
    

    @classmethod
    def from_record(cls, record) -> Self:
        return cls.model_validate(dict(record))
    

    @classmethod
    def gen_documents_from_records(cls, records:Iterable) -> Iterable[Self]:
        for record in records:
            yield cls.from_record(record)


if __name__ == '__main__':

    class WithExplicitId(DocumentBase):
        name:str

    explicit = WithExplicitId(
        _id='explicit',
        name='test'
    )

    print(explicit)
    print(explicit.model_dump(exclude_unset=True))

    class WithLabelledId(DocumentBase):
        code:str
        _id_field='code'

    implicit = WithLabelledId(code='1121')
    print(implicit)
    print(implicit.model_dump(exclude_unset=True))
