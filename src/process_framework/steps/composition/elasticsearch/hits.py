from pydantic import BaseModel, Field
from typing import Self, Iterable, Any

class Hit[T:BaseModel](BaseModel):
    """typed representation of a single elasticsearch hit with passthrough access to its _source fields."""
    index: str = Field(alias="_index")
    id: str = Field(alias="_id")
    source: T = Field(alias="_source")
    score: float|None = Field(None, alias="_score")
    sort: float|list[float]|None = None

    def __getattr__(self, name):
        """delegate attribute access to the underlying source object."""
        return getattr(self.source, name)
    

class Total(BaseModel):
    """representation of the elasticsearch total hits metadata."""
    value:int
    relation:str


class Hits[T:BaseModel](BaseModel):
    """container for elasticsearch hits and total metadata with a simple display helper."""
    hits: list[Hit[T]]
    total: Total = Field(default_factory=lambda: Total(value=-1, relation="default"))
    
    def display(self, head:int|None=None) -> None:
        """print the total and optionally the first n hits."""
        print(self.total)
        vs = self.hits[:head] if head is not None else self.hits
        for i, v in enumerate(vs):
            print(i, v)
        if head and head < len(self.hits):
            print('...')

    @classmethod
    def from_hits(cls, hits:list|Iterable|Any) -> Self:
        return cls.model_validate({'hits':list(hits)})


class HasName(BaseModel):
    """simple mixin providing for a name field"""
    name:str