from dataclasses import dataclass
from elasticsearch import Elasticsearch
from .core import StepMixin

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
        (f'checking {role} index: {index}')
        assert self.elasticsearch.indices.exists(index=index)


@dataclass
class HasElasticsearchIndex(HasElasticsearchIndexBase):
    """mixin requiring a generic index and validating its existence."""
    index:str
    def preflight(self) -> None:
        self.check_index(self.index, 'index')
        return super().preflight()
    

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

