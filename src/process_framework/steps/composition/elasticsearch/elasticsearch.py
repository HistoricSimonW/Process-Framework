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
from ...step import AssigningStep
from ..core import HasOutput, ITransformValue, StepMixin
from .hits import Hit

@dataclass
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


@dataclass
class HasElasticsearchIndex(HasElasticsearchIndexBase):
    """mixin requiring a generic index and validating its existence."""
    index:str
    def preflight(self) -> None:
        self.check_index(self.index, 'index')
        return super().preflight()


@dataclass
class HasElasticsearchQuery(StepMixin):
    """ mixin for a nullable query """
    query:dict|None=None


@dataclass
class HasElasticsearchTargetIndex(HasElasticsearchIndexBase):
    """mixin requiring a target index and validating its existence."""
    target_index:str
    def preflight(self) -> None:
        self.check_index(self.target_index, 'target')
        return super().preflight()
    

@dataclass
class HasElasticsearchSourceIndex(HasElasticsearchIndexBase):
    """mixin requiring a source index and validating its existence."""
    source_index:str
    def preflight(self) -> None:
        self.check_index(self.source_index, 'source')
        return super().preflight()


@dataclass
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
    