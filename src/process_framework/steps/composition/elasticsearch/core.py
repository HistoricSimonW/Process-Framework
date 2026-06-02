# stdlib
from abc import abstractmethod
from dataclasses import dataclass, field
from itertools import islice
from typing import Iterable

# third-party
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from pydantic import BaseModel

# first-party
from process_framework.steps.step import AssigningStep
from process_framework.steps.composition.core import HasOutput, ITransformValue, StepMixin

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

@dataclass(kw_only=True)
class HasElasticsearch(StepMixin):
    """mixin providing an elasticsearch client and connectivity check."""
    elasticsearch:Elasticsearch
    def preflight(self) -> None:
        assert self.elasticsearch.info()
        super().preflight()


class HasElasticsearchIndexBase(HasElasticsearch):
    """base mixin for index-related elasticsearch checks."""
    def check_index(self, index:str, role:str):
        self._info(f'checking {role} index: {index}')
        if not self.elasticsearch.indices.exists(index=index):
            raise ValueError(f"{role} index does not exist: {index}")


@dataclass(kw_only=True)
class HasElasticsearchIndex(HasElasticsearchIndexBase):
    """mixin requiring a generic index and validating its existence."""
    index:str
    def preflight(self) -> None:
        self.check_index(self.index, 'index')
        return super().preflight()


@dataclass(kw_only=True)
class HasElasticsearchQuery(StepMixin):
    """ mixin for a nullable query """
    query:dict|None=None


@dataclass(kw_only=True)
class HasElasticsearchTargetIndex(HasElasticsearchIndexBase):
    """mixin requiring a target index and validating its existence."""
    target_index:str
    def preflight(self) -> None:
        self.check_index(self.target_index, 'target')
        return super().preflight()
    

@dataclass(kw_only=True)
class HasElasticsearchSourceIndex(HasElasticsearchIndexBase):
    """mixin requiring a source index and validating its existence."""
    source_index:str
    def preflight(self) -> None:
        self.check_index(self.source_index, 'source')
        return super().preflight()


@dataclass(kw_only=True)
class IHasElasticsearchScan[T:BaseModel](HasElasticsearchIndex, HasElasticsearchQuery, HasOutput, StepMixin):
    """mixin for scanning typed elasticsearch hits from an index."""
    raise_on_error:bool=True
    preserve_order:bool=False
    request_timeout:float|None=False
    clear_scroll:bool=True
    size:int=1000
    scroll:str = '5m'
    scroll_kwargs:dict=field(default_factory=dict)
    scan_kwargs:dict=field(default_factory=dict)

    limit:int|None = None

    def gen_hits(self) -> Iterable[Hit[T]]:
        """yield validated hits from an elasticsearch scan."""
        scan_ = scan(
            client=self.elasticsearch,
            index=self.index,
            query=self.query,
            scroll=self.scroll,
            raise_on_error=self.raise_on_error,
            preserve_order=self.preserve_order,
            size=self.size,
            request_timeout=self.request_timeout,
            clear_scroll=self.clear_scroll,
            scroll_kwargs=self.scroll_kwargs,
            **self.scan_kwargs
        )
        
        hits = (Hit[T].model_validate(t) for t in scan_)

        if self.limit is not None:
            return islice(hits, self.limit)
        
        return hits
    
@dataclass
class AssignScan[T:BaseModel](IHasElasticsearchScan[T], AssigningStep[Iterable[Hit[T]]]):
    """assign an elasticsearch scan iterator to an output reference."""
    def generate_value(self) -> Iterable[Hit[T]] | None:
        """generate scanned hits without materializing them."""
        return self.gen_hits()


@dataclass
class AssignScanResult[THit:BaseModel, TOut](IHasElasticsearchScan[THit], ITransformValue[Iterable[Hit[THit]], TOut], AssigningStep[TOut]):
    """assign a transformed elasticsearch scan result to an output reference."""
    @abstractmethod
    def transform_value(self, input_: Iterable[Hit[THit]]) -> TOut:
        """transform scanned hits into the assigned output value."""
        ...
    
    def generate_value(self) -> TOut | None:
        """scan hits and transform them into the assigned output value."""
        hits = self.gen_hits()
        return self.transform_value(hits)
    

@dataclass
class AssignScanResultList[T: BaseModel](AssignScanResult[T, list[Hit[T]]]):
    """assign scanned elasticsearch hits as a list."""

    def transform_value(self, input_: Iterable[Hit[T]]) -> list[Hit[T]]:
        """materialize scanned hits as a list."""
        return list(input_)
    